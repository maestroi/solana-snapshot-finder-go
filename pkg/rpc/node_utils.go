package rpc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

// NodeEvaluationResult represents the result of evaluating an RPC node
type NodeEvaluationResult struct {
	RPC             string  `json:"rpc"`
	Speed           float64 `json:"speed"`
	Latency         float64 `json:"latency"`
	Slot            int     `json:"slot"`
	FullSlot        int     `json:"full_slot"`
	IncrementalSlot int     `json:"incremental_slot"`
	Diff            int     `json:"diff"`
	Version         string  `json:"version"`
	Status          string  `json:"status"`
}

func MeasureSpeed(url string, measureTime int) (float64, float64, error) {
	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	startTime := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch URL: %v", err)
	}
	defer resp.Body.Close()
	latency := time.Since(startTime).Milliseconds()

	buffer := make([]byte, 81920)
	var totalLoaded int64
	var speeds []float64

	lastTime := time.Now()
	for time.Since(startTime).Seconds() < float64(measureTime) {
		n, err := resp.Body.Read(buffer)
		if n > 0 {
			totalLoaded += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, float64(latency), fmt.Errorf("error reading response body: %v", err)
		}

		elapsed := time.Since(lastTime).Seconds()
		if elapsed >= 1 {
			speed := float64(totalLoaded) / elapsed
			speeds = append(speeds, speed)
			lastTime = time.Now()
			totalLoaded = 0
		}
	}

	if len(speeds) == 0 {
		return 0, float64(latency), fmt.Errorf("no data collected during the measurement period")
	}

	medianSpeed := calculateMedian(speeds) / (1024 * 1024)
	return medianSpeed, float64(latency), nil
}

func calculateMedian(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	n := len(values)
	sort.Float64s(values)
	if n%2 == 0 {
		return (values[n/2-1] + values[n/2]) / 2
	}
	return values[n/2]
}

func checkHealth(rpc string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(rpc + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// checkSnapshotAvailability returns the extension that responded so the speed
// test hits the same URL that was just confirmed available.
func checkSnapshotAvailability(rpc string) (bool, string) {
	client := &http.Client{Timeout: 3 * time.Second}
	extensions := []string{".tar.bz2", ".tar.zst"}
	for _, ext := range extensions {
		snapshotURL := rpc + "/snapshot" + ext
		resp, err := client.Head(snapshotURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return true, ext
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return false, ""
}

// probeCandidate is a node that passed the cheap health+availability check.
type probeCandidate struct {
	node    RPCNode
	rpc     string
	ext     string
	latency float64
}

// EvaluateNodesWithVersions evaluates cluster nodes in two phases: a cheap
// health+availability+latency probe against every node, then a real
// bandwidth speed test against only the closest cfg.SpeedTestCandidates of
// those - instead of pulling real snapshot bytes from every node in the
// cluster (which can be thousands), keeping load on the network low.
func EvaluateNodesWithVersions(nodes []RPCNode, cfg config.Config, defaultSlot int) []NodeEvaluationResult {
	sem := make(chan struct{}, cfg.WorkerCount)

	var healthFailedNodes, snapshotUnavailableNodes int32
	var badResults []NodeEvaluationResult
	var badMu sync.Mutex
	addBad := func(node RPCNode, rpc string) {
		badMu.Lock()
		badResults = append(badResults, NodeEvaluationResult{RPC: rpc, Version: node.Version, Status: "bad"})
		badMu.Unlock()
	}

	// Phase A: cheap probe (health + snapshot availability + latency) on every node.
	var candMu sync.Mutex
	var candidates []probeCandidate
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(node RPCNode) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			rpc := node.Address
			if !strings.HasPrefix(rpc, "http://") && !strings.HasPrefix(rpc, "https://") {
				rpc = "http://" + rpc
			}

			start := time.Now()
			if !checkHealth(rpc) {
				atomic.AddInt32(&healthFailedNodes, 1)
				addBad(node, rpc)
				return
			}
			latency := float64(time.Since(start).Milliseconds())

			available, ext := checkSnapshotAvailability(rpc)
			if !available {
				atomic.AddInt32(&snapshotUnavailableNodes, 1)
				addBad(node, rpc)
				return
			}

			candMu.Lock()
			candidates = append(candidates, probeCandidate{node: node, rpc: rpc, ext: ext, latency: latency})
			candMu.Unlock()
		}(node)
	}
	wg.Wait()

	log.Printf("Probe complete: %d/%d nodes healthy with a snapshot available (Health failed: %d, No snapshot: %d)",
		len(candidates), len(nodes), healthFailedNodes, snapshotUnavailableNodes)

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].latency < candidates[j].latency })

	speedTestLimit := cfg.SpeedTestCandidates
	if speedTestLimit <= 0 {
		speedTestLimit = 30
	}
	if len(candidates) > speedTestLimit {
		log.Printf("Speed-testing the %d closest candidates by latency (skipping the rest to limit load on the network)", speedTestLimit)
		candidates = candidates[:speedTestLimit]
	}

	// Phase B: real bandwidth speed test, only against the shortlisted candidates.
	results := make(chan NodeEvaluationResult, len(candidates))
	var goodNodes, slowNodes, badNodes int32

	slotThreshold := cfg.FullThreshold
	if slotThreshold == 0 {
		slotThreshold = 25000 // Default fallback if not configured
	}

	for _, c := range candidates {
		wg.Add(1)
		go func(c probeCandidate) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			baseURL, err := url.Parse(c.rpc)
			if err != nil {
				results <- NodeEvaluationResult{RPC: c.rpc, Version: c.node.Version, Status: "bad"}
				atomic.AddInt32(&badNodes, 1)
				return
			}
			baseURL.Path = "/snapshot" + c.ext
			snapshotURL := baseURL.String()

			speed, latency, err := MeasureSpeed(snapshotURL, cfg.SpeedTestSeconds)
			if err != nil {
				results <- NodeEvaluationResult{RPC: c.rpc, Version: c.node.Version, Status: "slow"}
				atomic.AddInt32(&slowNodes, 1)
				return
			}

			// Try to get highest snapshot slots first, fallback to regular slot
			var slot, fullSlot, incrementalSlot int
			if slots, err := GetHighestSnapshotSlots(c.rpc); err == nil {
				fullSlot = slots.Full
				incrementalSlot = slots.Incremental
				slot = fullSlot // Use full slot as the reference
			} else {
				log.Printf("Node %s: getHighestSnapshotSlots failed, trying getSlot: %v", c.rpc, err)
				slot, err = GetReferenceSlot(c.rpc)
				if err != nil {
					results <- NodeEvaluationResult{RPC: c.rpc, Speed: speed, Latency: latency, Version: c.node.Version, Status: "slow"}
					atomic.AddInt32(&slowNodes, 1)
					return
				}
				fullSlot = slot
				incrementalSlot = slot
			}

			diff := defaultSlot - slot
			status := "slow" // Default to slow for partially functional nodes

			// For node evaluation, we always prioritize finding nodes that can provide
			// full snapshots within the full_threshold. The incremental threshold is only
			// used later when we check local files and determine what's actually needed.

			// Strict slot validation: reject nodes outside the full threshold
			// We want full snapshots as close as possible to current slot height
			if diff > slotThreshold {
				log.Printf("Node %s rejected: slot too old (diff: %d, max allowed: %d)", c.rpc, diff, slotThreshold)
				results <- NodeEvaluationResult{RPC: c.rpc, Speed: speed, Latency: latency, Slot: slot, FullSlot: fullSlot, IncrementalSlot: incrementalSlot, Diff: diff, Version: c.node.Version, Status: "bad"}
				atomic.AddInt32(&badNodes, 1)
				return
			}

			if speed >= float64(cfg.MinDownloadSpeed) && latency <= float64(cfg.MaxLatency) && diff <= slotThreshold {
				status = "good"
				atomic.AddInt32(&goodNodes, 1)
			} else if speed == 0 || latency == 0 {
				// Only mark as "bad" if completely failed (no speed or no latency response)
				status = "bad"
				atomic.AddInt32(&badNodes, 1)
			} else {
				atomic.AddInt32(&slowNodes, 1)
			}

			results <- NodeEvaluationResult{RPC: c.rpc, Speed: speed, Latency: latency, Slot: slot, FullSlot: fullSlot, IncrementalSlot: incrementalSlot, Diff: diff, Version: c.node.Version, Status: status}
		}(c)
	}

	wg.Wait()
	close(results)

	evaluatedResults := append([]NodeEvaluationResult{}, badResults...)
	for result := range results {
		evaluatedResults = append(evaluatedResults, result)
	}

	sort.Slice(evaluatedResults, func(i, j int) bool {
		// Primary sort: by speed (fastest first)
		if evaluatedResults[i].Speed != evaluatedResults[j].Speed {
			return evaluatedResults[i].Speed > evaluatedResults[j].Speed
		}
		// Secondary sort: by slot difference (closest to reference slot first)
		return evaluatedResults[i].Diff < evaluatedResults[j].Diff
	})

	log.Printf("Node evaluation complete: %d/%d nodes processed | Good: %d, Slow: %d, Bad: %d",
		len(evaluatedResults), len(nodes), goodNodes, slowNodes, int32(len(badResults))+badNodes)
	log.Printf("Filtering breakdown: Health failed: %d, No snapshot endpoint: %d",
		healthFailedNodes, snapshotUnavailableNodes)

	// Log slot threshold information
	log.Printf("Node evaluation: using full threshold %d slots for full snapshots (reference slot: %d)",
		cfg.FullThreshold, defaultSlot)
	log.Printf("Note: Incremental threshold %d slots is used later when checking local files", cfg.IncrementalThreshold)

	return evaluatedResults
}

// EvaluateNodesWithRelaxedRequirements evaluates nodes with gradually relaxed requirements on each retry
func EvaluateNodesWithRelaxedRequirements(nodes []RPCNode, cfg config.Config, defaultSlot int, attempt int) []NodeEvaluationResult {
	// Calculate relaxed requirements based on attempt number
	relaxedSpeed := float64(cfg.MinDownloadSpeed)
	relaxedLatency := float64(cfg.MaxLatency)
	// Slot threshold remains strict - we don't want to relax this requirement
	// as it could lead to downloading outdated snapshots

	// Callers only invoke this for attempt > 1 (attempt 1 uses EvaluateNodesWithVersions directly).
	if attempt > 1 {
		// Apply relaxation factor for each attempt beyond the first
		// Each attempt multiplies the previous relaxation
		for i := 1; i < attempt; i++ {
			relaxedSpeed = relaxedSpeed * cfg.SpeedRelaxationFactor
			relaxedLatency = relaxedLatency / cfg.LatencyRelaxationFactor
			// Slot threshold stays the same - no relaxation
		}

		log.Printf("Attempt %d: Relaxed requirements - Speed: %.2f MB/s (from %d), Latency: %.2f ms (from %d), Slot threshold: %d (strict, no relaxation)",
			attempt, relaxedSpeed, cfg.MinDownloadSpeed, relaxedLatency, cfg.MaxLatency, cfg.FullThreshold)
	}

	// Create a temporary config with relaxed requirements for this evaluation
	relaxedConfig := cfg
	relaxedConfig.MinDownloadSpeed = int(relaxedSpeed)
	relaxedConfig.MaxLatency = int(relaxedLatency)
	// Keep original slot threshold - no relaxation

	return EvaluateNodesWithVersions(nodes, relaxedConfig, defaultSlot)
}

func SummarizeResultsWithVersions(results []NodeEvaluationResult) {
	totalNodes := len(results)
	goodNodes := 0
	slowNodes := 0
	badNodes := 0

	for _, result := range results {
		switch result.Status {
		case "good":
			goodNodes++
		case "slow":
			slowNodes++
		case "bad":
			badNodes++
		}
	}

	log.Printf("Node evaluation complete. Total nodes: %d | Good: %d | Slow: %d | Bad: %d",
		totalNodes, goodNodes, slowNodes, badNodes)

	log.Println("List of good nodes:")
	for _, result := range results {
		if result.Status == "good" {
			log.Printf("Node: %s | Speed: %.2f MB/s | Latency: %.2f ms | Slot: %d | Full: %d | Incremental: %d | Diff: %d | Version: %s",
				result.RPC, result.Speed, result.Latency, result.Slot, result.FullSlot, result.IncrementalSlot, result.Diff, result.Version)
		}
	}
}

func DumpGoodAndSlowNodesToFile(results []NodeEvaluationResult, outputFile string) {
	var filteredNodes []struct {
		RPC             string  `json:"rpc"`
		Speed           float64 `json:"speed"`
		Latency         float64 `json:"latency"`
		Slot            int     `json:"slot"`
		FullSlot        int     `json:"full_slot"`
		IncrementalSlot int     `json:"incremental_slot"`
		Diff            int     `json:"diff"`
		Version         string  `json:"version"`
		Status          string  `json:"status"`
	}

	// Save all nodes regardless of status to help with debugging
	for _, result := range results {
		filteredNodes = append(filteredNodes, struct {
			RPC             string  `json:"rpc"`
			Speed           float64 `json:"speed"`
			Latency         float64 `json:"latency"`
			Slot            int     `json:"slot"`
			FullSlot        int     `json:"full_slot"`
			IncrementalSlot int     `json:"incremental_slot"`
			Diff            int     `json:"diff"`
			Version         string  `json:"version"`
			Status          string  `json:"status"`
		}{
			RPC:             result.RPC,
			Speed:           result.Speed,
			Latency:         result.Latency,
			Slot:            result.Slot,
			FullSlot:        result.FullSlot,
			IncrementalSlot: result.IncrementalSlot,
			Diff:            result.Diff,
			Version:         result.Version,
			Status:          result.Status,
		})
	}

	file, err := os.Create(outputFile)
	if err != nil {
		log.Printf("Error creating output file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(filteredNodes); err != nil {
		log.Printf("Error writing to JSON file: %v", err)
		return
	}

	log.Printf("All nodes saved to %s (Total: %d)", outputFile, len(filteredNodes))
}

func SelectBestRPC(results []NodeEvaluationResult) string {
	var bestGoodNode struct {
		rpc   string
		speed float64
	}

	// Only consider good nodes - let the retry loop continue with relaxed requirements
	for _, result := range results {
		if result.Status == "good" && result.Speed > bestGoodNode.speed {
			bestGoodNode = struct {
				rpc   string
				speed float64
			}{rpc: result.RPC, speed: result.Speed}
		}
	}

	// Only return good nodes - no fallback to slow nodes
	if bestGoodNode.rpc != "" {
		return bestGoodNode.rpc
	}

	// Return empty string to continue retry loop with relaxed requirements
	return ""
}

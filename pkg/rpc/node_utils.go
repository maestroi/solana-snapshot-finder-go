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

func EvaluateNodesWithVersions(nodes []RPCNode, cfg config.Config, defaultSlot int) []NodeEvaluationResult {
	var wg sync.WaitGroup
	results := make(chan NodeEvaluationResult, len(nodes))
	done := make(chan bool)

	// Create a semaphore to limit concurrent goroutines
	sem := make(chan struct{}, cfg.WorkerCount)

	var processedNodes int32
	var goodNodes int32
	var slowNodes int32
	var badNodes int32
	var healthFailedNodes int32
	var snapshotUnavailableNodes int32

	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				processed := atomic.LoadInt32(&processedNodes)
				good := atomic.LoadInt32(&goodNodes)
				slow := atomic.LoadInt32(&slowNodes)
				bad := atomic.LoadInt32(&badNodes)
				healthFailed := atomic.LoadInt32(&healthFailedNodes)
				snapshotUnavailable := atomic.LoadInt32(&snapshotUnavailableNodes)
				log.Printf("Progress: %d/%d nodes processed (%.1f%%) | Good: %d, Slow: %d, Bad: %d | Health Failed: %d, No Snapshot: %d",
					processed, len(nodes), float64(processed)/float64(len(nodes))*100, good, slow, bad, healthFailed, snapshotUnavailable)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()

	appendResult := func(node RPCNode, rpc string, speed, latency float64, slot, fullSlot, incrementalSlot, diff int, status string) {
		results <- NodeEvaluationResult{
			RPC:             rpc,
			Speed:           speed,
			Latency:         latency,
			Slot:            slot,
			FullSlot:        fullSlot,
			IncrementalSlot: incrementalSlot,
			Diff:            diff,
			Version:         node.Version,
			Status:          status,
		}

		processed := atomic.AddInt32(&processedNodes, 1)
		switch status {
		case "good":
			atomic.AddInt32(&goodNodes, 1)
		case "slow":
			atomic.AddInt32(&slowNodes, 1)
		case "bad":
			atomic.AddInt32(&badNodes, 1)
		}

		if processed%50 == 0 {
			log.Printf("Milestone reached: %d/%d nodes processed", processed, len(nodes))
		}
	}

	checkHealth := func(rpc string) bool {
		client := &http.Client{
			Timeout: 5 * time.Second,
		}
		resp, err := client.Get(rpc + "/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}

	// NEW: Check if snapshot is actually available before speed testing
	checkSnapshotAvailability := func(rpc string) bool {
		client := &http.Client{
			Timeout: 3 * time.Second, // Quick check for snapshot availability
		}

		// Try both common snapshot extensions
		extensions := []string{".tar.bz2", ".tar.zst"}
		for _, ext := range extensions {
			snapshotURL := rpc + "/snapshot" + ext
			resp, err := client.Head(snapshotURL)
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return true
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
		return false
	}

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

			if !checkHealth(rpc) {
				atomic.AddInt32(&healthFailedNodes, 1)
				appendResult(node, rpc, 0, 0, 0, 0, 0, 0, "bad")
				return
			}

			// NEW: Check if snapshot is actually available before speed testing
			if !checkSnapshotAvailability(rpc) {
				atomic.AddInt32(&snapshotUnavailableNodes, 1)
				log.Printf("Node %s rejected: no snapshot endpoint available", rpc)
				appendResult(node, rpc, 0, 0, 0, 0, 0, 0, "bad")
				return
			}

			baseURL, err := url.Parse(rpc)
			if err != nil {
				appendResult(node, rpc, 0, 0, 0, 0, 0, 0, "bad")
				return
			}

			baseURL.Path = "/snapshot.tar.bz2"
			snapshotURL := baseURL.String()

			speed, latency, err := MeasureSpeed(snapshotURL, cfg.SleepBeforeRetry/2)
			if err != nil {
				appendResult(node, rpc, speed, latency, 0, 0, 0, 0, "slow")
				return
			}

			// Try to get highest snapshot slots first, fallback to regular slot
			var slot, fullSlot, incrementalSlot int
			if slots, err := GetHighestSnapshotSlots(rpc); err == nil {
				fullSlot = slots.Full
				incrementalSlot = slots.Incremental
				slot = fullSlot // Use full slot as the reference
			} else {
				log.Printf("Node %s: getHighestSnapshotSlots failed, trying getSlot: %v", rpc, err)
				slot, err = GetReferenceSlot(rpc)
				if err != nil {
					appendResult(node, rpc, speed, latency, 0, 0, 0, 0, "slow")
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
			slotThreshold := cfg.FullThreshold
			if slotThreshold == 0 {
				slotThreshold = 25000 // Default fallback if not configured
			}

			// Strict slot validation: reject nodes outside the full threshold
			// We want full snapshots as close as possible to current slot height
			if diff > slotThreshold {
				log.Printf("Node %s rejected: slot too old (diff: %d, max allowed: %d)", rpc, diff, slotThreshold)
				appendResult(node, rpc, speed, latency, slot, fullSlot, incrementalSlot, diff, "bad")
				return
			}

			if speed >= float64(cfg.MinDownloadSpeed) && latency <= float64(cfg.MaxLatency) && diff <= slotThreshold {
				status = "good"
			} else if speed == 0 || latency == 0 {
				// Only mark as "bad" if completely failed (no speed or no latency response)
				status = "bad"
			}
			// Otherwise keep as "slow" for nodes that are functional but don't meet requirements

			appendResult(node, rpc, speed, latency, slot, fullSlot, incrementalSlot, diff, status)
		}(node)
	}

	wg.Wait()
	done <- true
	close(results)

	var evaluatedResults []NodeEvaluationResult
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
		atomic.LoadInt32(&processedNodes), len(nodes),
		atomic.LoadInt32(&goodNodes),
		atomic.LoadInt32(&slowNodes),
		atomic.LoadInt32(&badNodes))
	log.Printf("Filtering breakdown: Health failed: %d, No snapshot endpoint: %d",
		atomic.LoadInt32(&healthFailedNodes),
		atomic.LoadInt32(&snapshotUnavailableNodes))

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

	if attempt > 1 && attempt <= cfg.MaxRelaxationAttempts {
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

func summarizeResultsWithVersions(results []NodeEvaluationResult) {
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

func dumpGoodAndSlowNodesToFile(results []NodeEvaluationResult, outputFile string) {
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

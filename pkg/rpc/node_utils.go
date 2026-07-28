package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

var (
	fullSnapshotFilenameRe = regexp.MustCompile(`^snapshot-(\d+)-[a-zA-Z0-9]+\.tar\.(zst|bz2)$`)
	incrementalFilenameRe  = regexp.MustCompile(`incremental-snapshot-(\d+)-(\d+)-[a-zA-Z0-9]+\.tar\.(zst|bz2)`)
)

func parseFullSnapshotSlotFromName(name string) (int, bool) {
	match := fullSnapshotFilenameRe.FindStringSubmatch(name)
	if match == nil {
		return 0, false
	}
	slot, err := strconv.Atoi(match[1])
	return slot, err == nil && slot > 0
}

// IncrementalInfo describes a probed incremental snapshot on a node.
type IncrementalInfo struct {
	NodeRPC  string
	BaseSlot int
	EndSlot  int
	Filename string
}

// ProbeIncrementalInfo follows redirects on a node's incremental snapshot endpoint
// to learn the base/end slots from the filename. Used for matching before downloads.
func ProbeIncrementalInfo(rpcAddress string) (*IncrementalInfo, error) {
	base := normalizeRPCBase(rpcAddress)
	client := &http.Client{Timeout: 8 * time.Second}
	extensions := []string{".tar.zst", ".tar.bz2"}

	var lastErr error
	for _, ext := range extensions {
		snapshotURL := base + "/incremental-snapshot" + ext
		resp, err := client.Head(snapshotURL)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP %d for %s", resp.StatusCode, snapshotURL)
			resp.Body.Close()
			continue
		}

		fileName := filepath.Base(resp.Request.URL.String())
		if cd := resp.Header.Get("Content-Disposition"); strings.Contains(cd, "filename=") {
			fileName = strings.Trim(strings.Split(cd, "filename=")[1], `"' `)
		}
		resp.Body.Close()

		match := incrementalFilenameRe.FindStringSubmatch(fileName)
		if match == nil {
			lastErr = fmt.Errorf("invalid incremental filename: %s", fileName)
			continue
		}
		baseSlot, err1 := strconv.Atoi(match[1])
		endSlot, err2 := strconv.Atoi(match[2])
		if err1 != nil || err2 != nil {
			lastErr = fmt.Errorf("failed to parse slots from %s", fileName)
			continue
		}
		return &IncrementalInfo{
			NodeRPC:  base,
			BaseSlot: baseSlot,
			EndSlot:  endSlot,
			Filename: fileName,
		}, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no incremental snapshot at %s", rpcAddress)
}

// FindMatchingIncremental searches evaluated nodes (fastest first) for an incremental
// whose base slot matches fullSnapshotSlot.
func FindMatchingIncremental(results []NodeEvaluationResult, fullSnapshotSlot int) (*IncrementalInfo, error) {
	if fullSnapshotSlot <= 0 {
		return nil, fmt.Errorf("invalid full snapshot slot: %d", fullSnapshotSlot)
	}

	// Prefer good, then slow, already roughly sorted by speed from evaluation.
	var candidates []NodeEvaluationResult
	for _, r := range results {
		if r.Status == "good" || r.Status == "slow" {
			candidates = append(candidates, r)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Speed > candidates[j].Speed })

	log.Printf("Searching for incremental with base slot %d across %d nodes...", fullSnapshotSlot, len(candidates))
	for i, node := range candidates {
		info, err := ProbeIncrementalInfo(node.RPC)
		if err != nil {
			continue
		}
		if info.BaseSlot == fullSnapshotSlot {
			log.Printf("  Node %d/%d (%s): match found (base=%d, end=%d)",
				i+1, len(candidates), node.RPC, info.BaseSlot, info.EndSlot)
			return info, nil
		}
	}
	return nil, fmt.Errorf("no incremental with base slot %d found", fullSnapshotSlot)
}

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

func MeasureSpeed(url string, measureTime int, maxBytes int64) (speedMBs float64, latencyMs float64, err error) {
	measureDuration := time.Duration(measureTime) * time.Second
	timeout := 10 * time.Second
	if measureTime > 0 {
		timeout += measureDuration
	}
	client := &http.Client{Timeout: timeout}

	doRequest := func(useRange bool) (*http.Response, context.Context, context.CancelFunc, time.Time, error) {
		ctx, cancel := context.WithCancel(context.Background())

		requestStart := time.Now()
		req, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if requestErr != nil {
			cancel()
			return nil, nil, nil, requestStart, requestErr
		}
		if useRange {
			req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", maxBytes-1))
		}

		resp, requestErr := client.Do(req)
		if requestErr != nil {
			cancel()
			return nil, nil, nil, requestStart, requestErr
		}
		return resp, ctx, cancel, requestStart, nil
	}

	resp, ctx, cancel, requestStart, err := doRequest(maxBytes > 0)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to fetch URL: %v", err)
	}

	if maxBytes > 0 && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		cancel()
		resp, ctx, cancel, requestStart, err = doRequest(false)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to fetch URL without Range: %v", err)
		}
	}
	defer cancel()
	defer resp.Body.Close()

	latencyMs = float64(time.Since(requestStart).Milliseconds())
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return 0, latencyMs, fmt.Errorf("unexpected HTTP status: %s", resp.Status)
	}

	measureStart := time.Now()
	if measureDuration > 0 {
		timer := time.AfterFunc(measureDuration, cancel)
		defer timer.Stop()
	}

	buffer := make([]byte, 81920)
	var totalLoaded int64

	for time.Since(measureStart) < measureDuration && (maxBytes <= 0 || totalLoaded < maxBytes) {
		readBuffer := buffer
		if maxBytes > 0 && int64(len(readBuffer)) > maxBytes-totalLoaded {
			readBuffer = readBuffer[:maxBytes-totalLoaded]
		}

		n, readErr := resp.Body.Read(readBuffer)
		totalLoaded += int64(n)
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			if ctx.Err() == context.Canceled && time.Since(measureStart) >= measureDuration {
				break
			}
			return 0, latencyMs, fmt.Errorf("error reading response body: %v", readErr)
		}
	}

	elapsed := time.Since(measureStart).Seconds()
	if totalLoaded == 0 || elapsed == 0 {
		return 0, latencyMs, fmt.Errorf("no data collected during the measurement period")
	}

	speedMBs = float64(totalLoaded) / elapsed / (1024 * 1024)
	return speedMBs, latencyMs, nil
}

func normalizeRPCBase(rpc string) string {
	if !strings.HasPrefix(rpc, "http://") && !strings.HasPrefix(rpc, "https://") {
		rpc = "http://" + rpc
	}
	return strings.TrimRight(rpc, "/")
}

func checkHealth(rpc string) bool {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(normalizeRPCBase(rpc) + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// checkSnapshotAvailability returns the extension that responded so the speed
// test hits the same URL that was just confirmed available, plus the full slot
// when the response filename exposes it.
func checkSnapshotAvailability(rpc string) (bool, string, int) {
	client := &http.Client{Timeout: 3 * time.Second}
	base := normalizeRPCBase(rpc)
	extensions := []string{".tar.bz2", ".tar.zst"}
	for _, ext := range extensions {
		snapshotURL := base + "/snapshot" + ext
		resp, err := client.Head(snapshotURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			fileName := filepath.Base(resp.Request.URL.Path)
			if _, params, parseErr := mime.ParseMediaType(resp.Header.Get("Content-Disposition")); parseErr == nil && params["filename"] != "" {
				fileName = filepath.Base(params["filename"])
			}
			fullSlot, _ := parseFullSnapshotSlotFromName(fileName)
			resp.Body.Close()
			return true, ext, fullSlot
		}
		if resp != nil {
			resp.Body.Close()
		}
	}
	return false, "", 0
}

// probeCandidate is a node that passed the cheap health+availability check.
type probeCandidate struct {
	node     RPCNode
	rpc      string
	ext      string
	fullSlot int
	latency  float64
}

func speedTestWorkerCount(cfg config.Config) int {
	if cfg.SpeedTestWorkers <= 0 {
		return 5
	}
	return cfg.SpeedTestWorkers
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

			rpc := normalizeRPCBase(node.Address)

			start := time.Now()
			// Static whitelist URLs are HTTP snapshot hosts, not Solana RPCs — skip /health.
			if !node.IsStatic {
				if !checkHealth(rpc) {
					atomic.AddInt32(&healthFailedNodes, 1)
					addBad(node, rpc)
					return
				}
			}
			latency := float64(time.Since(start).Milliseconds())

			available, ext, fullSlot := checkSnapshotAvailability(rpc)
			if !available {
				atomic.AddInt32(&snapshotUnavailableNodes, 1)
				addBad(node, rpc)
				return
			}

			candMu.Lock()
			candidates = append(candidates, probeCandidate{node: node, rpc: rpc, ext: ext, fullSlot: fullSlot, latency: latency})
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
	log.Printf("Phase B: speed-testing %d candidates with %d workers", len(candidates), speedTestWorkerCount(cfg))
	speedSem := make(chan struct{}, speedTestWorkerCount(cfg))
	results := make(chan NodeEvaluationResult, len(candidates))
	var goodNodes, slowNodes, badNodes int32

	slotThreshold := cfg.FullThreshold
	if slotThreshold == 0 {
		slotThreshold = 100000 // Default fallback if not configured (Agave 3.x)
	}

	for _, c := range candidates {
		wg.Add(1)
		go func(c probeCandidate) {
			defer wg.Done()
			speedSem <- struct{}{}
			defer func() { <-speedSem }()

			snapshotURL := normalizeRPCBase(c.rpc) + "/snapshot" + c.ext
			if _, err := url.Parse(snapshotURL); err != nil {
				results <- NodeEvaluationResult{RPC: c.rpc, Version: c.node.Version, Status: "bad"}
				atomic.AddInt32(&badNodes, 1)
				return
			}

			speed, latency, err := MeasureSpeed(snapshotURL, cfg.SpeedTestSeconds, cfg.SpeedTestMaxBytes)
			if err != nil {
				results <- NodeEvaluationResult{RPC: c.rpc, Version: c.node.Version, Status: "slow"}
				atomic.AddInt32(&slowNodes, 1)
				return
			}

			// Try to get highest snapshot slots first, fallback to regular slot.
			// Static whitelist hosts are not Solana RPCs — treat them as trusted (diff=0).
			var slot, fullSlot, incrementalSlot int
			if c.node.IsStatic {
				slot = defaultSlot
				fullSlot = defaultSlot
				incrementalSlot = defaultSlot
			} else if c.fullSlot > 0 {
				fullSlot = c.fullSlot
				slot = fullSlot
				log.Printf("Node %s: full slot %d from HEAD filename (selection-time; no RPC reconcile)", c.rpc, fullSlot)
				if slots, err := GetHighestSnapshotSlots(c.rpc); err == nil {
					incrementalSlot = slots.Incremental
				}
			} else if slots, err := GetHighestSnapshotSlots(c.rpc); err == nil {
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

			if cfg.MaxSlot > 0 && !c.node.IsStatic {
				// Historical mode: accept snapshots at or before max_slot; reject newer ones.
				if fullSlot <= 0 || int64(fullSlot) > cfg.MaxSlot {
					log.Printf("Node %s rejected: full slot %d is missing or above max_slot %d", c.rpc, fullSlot, cfg.MaxSlot)
					results <- NodeEvaluationResult{RPC: c.rpc, Speed: speed, Latency: latency, Slot: slot, FullSlot: fullSlot, IncrementalSlot: incrementalSlot, Diff: diff, Version: c.node.Version, Status: "bad"}
					atomic.AddInt32(&badNodes, 1)
					return
				}
			} else if !c.node.IsStatic && diff > slotThreshold {
				// Strict slot validation: reject nodes outside the full threshold.
				// We want full snapshots as close as possible to current slot height.
				log.Printf("Node %s rejected: slot too old (diff: %d, max allowed: %d)", c.rpc, diff, slotThreshold)
				results <- NodeEvaluationResult{RPC: c.rpc, Speed: speed, Latency: latency, Slot: slot, FullSlot: fullSlot, IncrementalSlot: incrementalSlot, Diff: diff, Version: c.node.Version, Status: "bad"}
				atomic.AddInt32(&badNodes, 1)
				return
			}

			slotOK := c.node.IsStatic || cfg.MaxSlot > 0 || diff <= slotThreshold
			if speed >= float64(cfg.MinDownloadSpeed) && latency <= float64(cfg.MaxLatency) && slotOK {
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

// FilterResultsByMaxSlot keeps nodes with FullSlot <= maxSlot, retaining only those
// that share the newest such FullSlot. Useful for historical snapshot selection.
// If maxSlot <= 0, results are returned unchanged.
func FilterResultsByMaxSlot(results []NodeEvaluationResult, maxSlot int64) []NodeEvaluationResult {
	if maxSlot <= 0 {
		return results
	}

	var filtered []NodeEvaluationResult
	matching := 0

	for _, candidate := range results {
		if candidate.FullSlot <= 0 || int64(candidate.FullSlot) > maxSlot {
			continue
		}

		matching++

		if len(filtered) == 0 {
			filtered = []NodeEvaluationResult{candidate}
			continue
		}

		currentSlot := filtered[0].FullSlot
		if candidate.FullSlot == currentSlot {
			filtered = append(filtered, candidate)
		} else if candidate.FullSlot > currentSlot {
			filtered = []NodeEvaluationResult{candidate}
		}
	}

	if len(filtered) == 0 {
		log.Printf("WARNING: No nodes found with snapshots at or before slot %d", maxSlot)
		return nil
	}

	log.Printf("Found %d nodes with snapshots at or before slot %d", matching, maxSlot)
	log.Printf("Selecting newest available slot: %d (%d nodes have this snapshot)", filtered[0].FullSlot, len(filtered))
	return filtered
}

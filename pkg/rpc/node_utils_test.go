package rpc

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

func TestParseFullSnapshotSlotFromName(t *testing.T) {
	slot, ok := parseFullSnapshotSlotFromName("snapshot-12345-AbCdEf.tar.zst")
	if !ok || slot != 12345 {
		t.Fatalf("got %d %v", slot, ok)
	}
	if _, ok := parseFullSnapshotSlotFromName("incremental-snapshot-1-2-x.tar.zst"); ok {
		t.Fatal("incremental name must not parse as full")
	}
}

func TestCheckSnapshotAvailabilityReturnsSlotFromRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/snapshot.tar.bz2" {
			http.Redirect(w, r, "/snapshot-12345-AbCdEf.tar.zst", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	available, ext, fullSlot := checkSnapshotAvailability(srv.URL)
	if !available || ext != ".tar.bz2" || fullSlot != 12345 {
		t.Fatalf("got available=%v ext=%q fullSlot=%d", available, ext, fullSlot)
	}
}

func TestCheckSnapshotAvailabilityPreservesRedirectSlotWithGenericContentDisposition(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/snapshot.tar.bz2" {
			http.Redirect(w, r, "/snapshot-123-AbCdEf.tar.zst", http.StatusFound)
			return
		}
		w.Header().Set("Content-Disposition", `attachment; filename="snapshot.tar.zst"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	available, ext, fullSlot := checkSnapshotAvailability(srv.URL)
	if !available || ext != ".tar.bz2" || fullSlot != 123 {
		t.Fatalf("got available=%v ext=%q fullSlot=%d", available, ext, fullSlot)
	}
}

func TestEvaluateNodesUsesHeadFullSlotAndRPCIncrementalSlot(t *testing.T) {
	var snapshotSlotCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/health":
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/snapshot.tar.bz2":
			http.Redirect(w, r, "/snapshot-12345-AbCdEf.tar.zst", http.StatusFound)
		case r.URL.Path == "/snapshot-12345-AbCdEf.tar.zst":
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusPartialContent)
				w.Write(bytes.Repeat([]byte("x"), 1024))
			}
		case r.Method == http.MethodPost:
			snapshotSlotCalls.Add(1)
			json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"result":  map[string]int{"full": 99999, "incremental": 12399},
				"id":      1,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	results := EvaluateNodesWithVersions([]RPCNode{{Address: srv.URL}}, config.Config{
		WorkerCount:          1,
		SpeedTestWorkers:     1,
		SpeedTestCandidates:  1,
		SpeedTestSeconds:     1,
		SpeedTestMaxBytes:    1024,
		FullThreshold:        1000,
		MaxLatency:           10000,
		MinDownloadSpeed:     0,
		IncrementalThreshold: 1000,
	}, 12400)

	if len(results) != 1 {
		t.Fatalf("got %d results", len(results))
	}
	if results[0].FullSlot != 12345 || results[0].Slot != 12345 {
		t.Fatalf("HEAD full slot must win, got full=%d slot=%d", results[0].FullSlot, results[0].Slot)
	}
	if results[0].IncrementalSlot != 12399 {
		t.Fatalf("expected RPC incremental slot 12399, got %d", results[0].IncrementalSlot)
	}
	if snapshotSlotCalls.Load() != 1 {
		t.Fatalf("expected one RPC call for incremental slot, got %d", snapshotSlotCalls.Load())
	}
}

func TestSpeedTestWorkerClamp(t *testing.T) {
	if got := speedTestWorkerCount(config.Config{SpeedTestWorkers: 0}); got != 5 {
		t.Errorf("got %d want 5", got)
	}
	if got := speedTestWorkerCount(config.Config{SpeedTestWorkers: 3}); got != 3 {
		t.Errorf("got %d want 3", got)
	}
}

func TestMeasureSpeedStopsAtByteCap(t *testing.T) {
	var readBytes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Header.Get("Range") == "" {
			t.Errorf("expected Range header")
		}
		w.WriteHeader(http.StatusPartialContent)
		payload := bytes.Repeat([]byte("x"), 512*1024) // 512 KiB chunks
		for {
			n, err := w.Write(payload)
			readBytes.Add(int64(n))
			if err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if readBytes.Load() > 2*1024*1024 {
				return
			}
		}
	}))
	defer srv.Close()

	speed, _, err := MeasureSpeed(srv.URL, 30, 256*1024) // 256 KiB cap, long time
	if err != nil {
		t.Fatal(err)
	}
	if speed <= 0 {
		t.Fatalf("expected positive speed, got %v", speed)
	}
	// Server should not have been asked for multi-MiB if client stops at cap
	if readBytes.Load() > 512*1024 {
		t.Fatalf("client read too much: %d", readBytes.Load())
	}
}

func TestMeasureSpeedFallsBackWithoutRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Write(bytes.Repeat([]byte("y"), 64*1024))
	}))
	defer srv.Close()

	if _, _, err := MeasureSpeed(srv.URL, 2, 32*1024); err != nil {
		t.Fatal(err)
	}
}

func TestMeasureSpeedFallsBackOnOKWithAcceptRanges(t *testing.T) {
	var rangeRequests, plainRequests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			rangeRequests.Add(1)
			w.Header().Set("Accept-Ranges", "bytes")
		} else {
			plainRequests.Add(1)
		}
		w.Write(bytes.Repeat([]byte("z"), 64*1024))
	}))
	defer srv.Close()

	if _, _, err := MeasureSpeed(srv.URL, 2, 32*1024); err != nil {
		t.Fatal(err)
	}
	if rangeRequests.Load() != 1 || plainRequests.Load() != 1 {
		t.Fatalf("expected one Range and one plain request, got Range=%d plain=%d",
			rangeRequests.Load(), plainRequests.Load())
	}
}

func TestMeasureSpeedStopsAtMeasureTimeWhenBodyStalls(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("x"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	start := time.Now()
	speed, _, err := MeasureSpeed(srv.URL, 1, 64*1024)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if speed <= 0 {
		t.Fatalf("expected positive speed, got %v", speed)
	}
	if elapsed < 900*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("expected measurement to stop around 1s, took %v", elapsed)
	}
}

func TestMeasureSpeedStartsMeasureTimeAfterHeaders(t *testing.T) {
	const headerDelay = 300 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(headerDelay)
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("x"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	start := time.Now()
	speed, latency, err := MeasureSpeed(srv.URL, 1, 64*1024)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if speed <= 0 {
		t.Fatalf("expected positive speed, got %v", speed)
	}
	if latency < float64(headerDelay.Milliseconds()) {
		t.Fatalf("expected latency of at least %v, got %.0fms", headerDelay, latency)
	}
	if elapsed < headerDelay+900*time.Millisecond || elapsed > headerDelay+2*time.Second {
		t.Fatalf("expected full measurement window after headers, took %v", elapsed)
	}
}

func TestFilterResultsByMaxSlot(t *testing.T) {
	results := []NodeEvaluationResult{
		{RPC: "a", FullSlot: 100, Speed: 50, Status: "good"},
		{RPC: "b", FullSlot: 200, Speed: 80, Status: "good"},
		{RPC: "c", FullSlot: 200, Speed: 90, Status: "good"},
		{RPC: "d", FullSlot: 300, Speed: 100, Status: "good"},
		{RPC: "e", FullSlot: 0, Speed: 120, Status: "good"},
	}

	filtered := FilterResultsByMaxSlot(results, 250)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 nodes at newest slot <= 250, got %d", len(filtered))
	}
	for _, r := range filtered {
		if r.FullSlot != 200 {
			t.Errorf("expected FullSlot 200, got %d for %s", r.FullSlot, r.RPC)
		}
	}

	if got := FilterResultsByMaxSlot(results, 0); len(got) != len(results) {
		t.Errorf("maxSlot 0 should return results unchanged, got %d want %d", len(got), len(results))
	}

	if got := FilterResultsByMaxSlot(results, 50); got != nil {
		t.Errorf("expected nil when no nodes match, got %v", got)
	}
}

func TestReclassifyResultsPromotesSlowToGood(t *testing.T) {
	results := []NodeEvaluationResult{
		{RPC: "http://a", Speed: 60, Latency: 180, Diff: 10, Status: "slow"},
		{RPC: "http://b", Speed: 10, Latency: 500, Diff: 10, Status: "slow"},
		{RPC: "http://c", Speed: 0, Latency: 0, Diff: 10, Status: "bad"},
	}
	cfg := config.Config{MinDownloadSpeed: 50, MaxLatency: 200, FullThreshold: 100000}

	out := ReclassifyResults(results, cfg, 1_000_000)

	var good, slow, bad int
	for _, r := range out {
		switch r.Status {
		case "good":
			good++
		case "slow":
			slow++
		case "bad":
			bad++
		}
	}
	if good != 1 || slow != 1 || bad != 1 {
		t.Fatalf("good=%d slow=%d bad=%d want 1/1/1", good, slow, bad)
	}
}

func TestReclassifyResultsDoesNotPromoteBadOrRelaxSlotRules(t *testing.T) {
	results := []NodeEvaluationResult{
		{RPC: "bad", Speed: 100, Latency: 10, Slot: 999_990, FullSlot: 999_990, Diff: 10, Status: "bad"},
		{RPC: "stale", Speed: 100, Latency: 10, Slot: 800_000, FullSlot: 800_000, Diff: 200_000, Status: "slow"},
	}
	cfg := config.Config{MinDownloadSpeed: 50, MaxLatency: 200, FullThreshold: 100_000}

	out := ReclassifyResults(results, cfg, 1_000_000)

	if out[0].Status != "bad" {
		t.Fatalf("bad node promoted to %q", out[0].Status)
	}
	if out[1].Status != "slow" {
		t.Fatalf("stale node reclassified as %q", out[1].Status)
	}
}

func TestRelaxedConfigForAttemptAppliesCumulativeFactors(t *testing.T) {
	cfg := config.Config{
		MinDownloadSpeed:        100,
		MaxLatency:              200,
		SpeedRelaxationFactor:   0.8,
		LatencyRelaxationFactor: 0.8,
		FullThreshold:           1234,
	}

	got := RelaxedConfigForAttempt(cfg, 3)

	if got.MinDownloadSpeed != 64 {
		t.Errorf("MinDownloadSpeed=%d want 64", got.MinDownloadSpeed)
	}
	if got.MaxLatency != 312 {
		t.Errorf("MaxLatency=%d want 312", got.MaxLatency)
	}
	if got.FullThreshold != cfg.FullThreshold {
		t.Errorf("FullThreshold=%d want unchanged %d", got.FullThreshold, cfg.FullThreshold)
	}
	if cfg.MinDownloadSpeed != 100 || cfg.MaxLatency != 200 {
		t.Fatalf("input config mutated: speed=%d latency=%d", cfg.MinDownloadSpeed, cfg.MaxLatency)
	}
}

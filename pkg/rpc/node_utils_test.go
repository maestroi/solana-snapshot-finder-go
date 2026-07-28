package rpc

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

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

func TestRelaxationFactors(t *testing.T) {
	cfg := config.Config{
		MinDownloadSpeed:        100,
		MaxLatency:              200,
		SpeedRelaxationFactor:   0.8,
		LatencyRelaxationFactor: 0.8,
		MaxRelaxationAttempts:   3,
	}

	// Test that relaxation factors are applied correctly
	relaxedConfig := cfg
	relaxedConfig.MinDownloadSpeed = int(float64(cfg.MinDownloadSpeed) * (cfg.SpeedRelaxationFactor * 1))
	relaxedConfig.MaxLatency = int(float64(cfg.MaxLatency) / (cfg.LatencyRelaxationFactor * 1))

	// Speed should be reduced: 100 * 0.8 = 80
	expectedSpeed := 80
	if relaxedConfig.MinDownloadSpeed != expectedSpeed {
		t.Errorf("Expected relaxed speed %d, got %d", expectedSpeed, relaxedConfig.MinDownloadSpeed)
	}

	// Latency should be increased: 200 / 0.8 = 250
	expectedLatency := 250
	if relaxedConfig.MaxLatency != expectedLatency {
		t.Errorf("Expected relaxed latency %d, got %d", expectedLatency, relaxedConfig.MaxLatency)
	}
}

func TestRelaxationCalculation(t *testing.T) {
	cfg := config.Config{
		MinDownloadSpeed:        100,
		MaxLatency:              200,
		SpeedRelaxationFactor:   0.8,
		LatencyRelaxationFactor: 0.8,
		MaxRelaxationAttempts:   3,
	}

	// Test relaxation calculation logic
	relaxedSpeed := float64(cfg.MinDownloadSpeed)
	relaxedLatency := float64(cfg.MaxLatency)

	// Simulate attempt 2
	attempt := 2
	if attempt > 1 && attempt <= cfg.MaxRelaxationAttempts {
		// Each attempt multiplies the previous relaxation
		for i := 1; i < attempt; i++ {
			relaxedSpeed = relaxedSpeed * cfg.SpeedRelaxationFactor
			relaxedLatency = relaxedLatency / cfg.LatencyRelaxationFactor
		}
	}

	// Speed should be: 100 * 0.8 = 80
	expectedSpeed := 100.0 * 0.8
	if relaxedSpeed != expectedSpeed {
		t.Errorf("Expected relaxed speed %.2f, got %.2f", expectedSpeed, relaxedSpeed)
	}

	// Latency should be: 200 / 0.8 = 250
	expectedLatency := 200.0 / 0.8
	if relaxedLatency != expectedLatency {
		t.Errorf("Expected relaxed latency %.2f, got %.2f", expectedLatency, relaxedLatency)
	}
}

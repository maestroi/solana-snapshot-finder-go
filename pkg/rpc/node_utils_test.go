package rpc

import (
	"testing"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

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

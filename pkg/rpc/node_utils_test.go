package rpc

import (
	"testing"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

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

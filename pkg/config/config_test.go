package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewKnobDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc_address: \"https://example.com\"\n"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SpeedTestWorkers != 5 {
		t.Errorf("SpeedTestWorkers=%d want 5", cfg.SpeedTestWorkers)
	}
	if cfg.SpeedTestMaxBytes != 268435456 {
		t.Errorf("SpeedTestMaxBytes=%d want 268435456", cfg.SpeedTestMaxBytes)
	}
	if cfg.WarmStartMinNodes != 3 {
		t.Errorf("WarmStartMinNodes=%d want 3", cfg.WarmStartMinNodes)
	}
}

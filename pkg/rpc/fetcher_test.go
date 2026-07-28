package rpc

import (
	"testing"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

func TestIsStaticSnapshotURL(t *testing.T) {
	cases := []struct {
		url    string
		static bool
	}{
		{"https://snapshots.example.com/mainnet-beta/", true},
		{"https://host/snapshot.tar.zst", true},
		{"https://host/incremental-snapshot.tar.zst", true},
		{"http://1.2.3.4:8899", false},
		{"https://api.mainnet-beta.solana.com", false},
	}
	for _, c := range cases {
		if got := isStaticSnapshotURL(c.url); got != c.static {
			t.Errorf("isStaticSnapshotURL(%q)=%v want %v", c.url, got, c.static)
		}
	}
}

func TestParseWhitelist(t *testing.T) {
	nodes := parseWhitelist([]string{
		"https://snapshots.example.com/mainnet-beta/",
		"http://10.0.0.1:8899",
		"",
	})
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if !nodes[0].IsStatic {
		t.Errorf("first entry should be static")
	}
	if nodes[1].IsStatic {
		t.Errorf("second entry should be RPC, not static")
	}
}

func TestMergeWhitelistOnly(t *testing.T) {
	cfg := config.Config{
		Whitelist:     []string{"https://snapshots.example.com/mainnet-beta/"},
		WhitelistMode: "only",
	}
	nodes, err := mergeWhitelist(nil, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 || !nodes[0].IsStatic {
		t.Fatalf("expected single static whitelist node, got %+v", nodes)
	}
}

func TestMergeWhitelistOnlyEmpty(t *testing.T) {
	cfg := config.Config{WhitelistMode: "only"}
	if _, err := mergeWhitelist(nil, cfg); err == nil {
		t.Fatal("expected error for empty whitelist in only mode")
	}
}

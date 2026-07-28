package rpc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadWarmStartRequiresMinNodes(t *testing.T) {
	dir := t.TempDir()
	payload := `[{"rpc":"http://a","status":"good"},{"rpc":"http://b","status":"slow"}]`
	if err := os.WriteFile(filepath.Join(dir, "nodes_attempt_1.json"), []byte(payload), 0644); err != nil {
		t.Fatal(err)
	}
	if got := LoadWarmStartRPCs(dir, 3); len(got) != 0 {
		t.Fatalf("expected empty warm-start below min, got %v", got)
	}
}

func TestLoadWarmStartReadsNewestAttempt(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "nodes_attempt_1.json"), []byte(`[{"rpc":"http://old","status":"good"},{"rpc":"http://b","status":"good"},{"rpc":"http://c","status":"good"}]`), 0644)
	_ = os.WriteFile(filepath.Join(dir, "nodes_attempt_2.json"), []byte(`[{"rpc":"http://new1","status":"good"},{"rpc":"http://new2","status":"good"},{"rpc":"http://new3","status":"good"}]`), 0644)
	got := LoadWarmStartRPCs(dir, 3)
	if len(got) != 3 || got[0] != "http://new1" {
		t.Fatalf("got %v", got)
	}
}

func TestPrioritizeNodesPutsWarmFirst(t *testing.T) {
	nodes := []RPCNode{{Address: "http://x"}, {Address: "http://y"}, {Address: "http://z"}}
	out := PrioritizeNodes(nodes, []string{"http://z", "http://y"})
	if out[0].Address != "http://z" || out[1].Address != "http://y" {
		t.Fatalf("%v", out)
	}
}

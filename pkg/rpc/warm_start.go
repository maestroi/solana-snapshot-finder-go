package rpc

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type warmStartEntry struct {
	RPC    string `json:"rpc"`
	Status string `json:"status"`
}

// LoadWarmStartRPCs reads good/slow RPCs from the newest nodes_attempt_*.json cache.
// Returns nil when no cache exists or good/slow count is below minNodes.
func LoadWarmStartRPCs(snapshotPath string, minNodes int) []string {
	matches, err := filepath.Glob(filepath.Join(snapshotPath, "nodes_attempt_*.json"))
	if err != nil || len(matches) == 0 {
		return nil
	}

	newest := newestAttemptFile(matches)
	if newest == "" {
		return nil
	}

	data, err := os.ReadFile(newest)
	if err != nil {
		return nil
	}

	var entries []warmStartEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil
	}

	var rpcs []string
	for _, e := range entries {
		if e.Status == "good" || e.Status == "slow" {
			rpcs = append(rpcs, e.RPC)
		}
	}
	if len(rpcs) < minNodes {
		return nil
	}
	return rpcs
}

func newestAttemptFile(paths []string) string {
	type candidate struct {
		path    string
		attempt int
		modTime int64
	}
	var candidates []candidate
	for _, p := range paths {
		base := filepath.Base(p)
		numStr := strings.TrimPrefix(base, "nodes_attempt_")
		numStr = strings.TrimSuffix(numStr, ".json")
		attempt, err := strconv.Atoi(numStr)
		if err != nil {
			continue
		}
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{path: p, attempt: attempt, modTime: info.ModTime().Unix()})
	}
	if len(candidates) == 0 {
		return ""
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].attempt != candidates[j].attempt {
			return candidates[i].attempt > candidates[j].attempt
		}
		return candidates[i].modTime > candidates[j].modTime
	})
	return candidates[0].path
}

// PrioritizeNodes returns nodes with warm RPC addresses first (stable order), then the rest.
// Duplicate addresses are removed using normalized RPC base URLs.
func PrioritizeNodes(nodes []RPCNode, warmRPCs []string) []RPCNode {
	nodeByNorm := make(map[string]RPCNode, len(nodes))
	for _, n := range nodes {
		nodeByNorm[normalizeRPCBase(n.Address)] = n
	}

	seen := make(map[string]bool, len(nodes))
	out := make([]RPCNode, 0, len(nodes))

	for _, warm := range warmRPCs {
		norm := normalizeRPCBase(warm)
		if seen[norm] {
			continue
		}
		if node, ok := nodeByNorm[norm]; ok {
			out = append(out, node)
			seen[norm] = true
		}
	}
	for _, n := range nodes {
		norm := normalizeRPCBase(n.Address)
		if !seen[norm] {
			out = append(out, n)
			seen[norm] = true
		}
	}
	return out
}

# Probe Efficiency & Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut peer load and evaluation time by evaluating nodes once (re-score on relaxation), capping/replacing speed tests under a separate concurrency gate, and harden download selection with cooldown/Retry-After plus strict incremental mismatch failures — without changing cluster-discovery-as-default or adding resume.

**Architecture:** Keep the existing Phase A (cheap probe) / Phase B (speed shortlist) shape in `pkg/rpc/node_utils.go`. Split Phase B onto its own semaphore; replace `MeasureSpeed` entirely; add `ReclassifyResults` so relaxation never re-probes; extend `selectNextRPC` into a testable cooldown helper in `pkg/rpc`; warm-start by reading existing `nodes_attempt_*.json`; tighten incremental post-download checks in `pkg/snapshot/download_utils.go`.

**Tech Stack:** Go 1.25, stdlib `net/http` + `httptest`, `github.com/spf13/viper` config, existing packages under `pkg/rpc`, `pkg/snapshot`, `pkg/config`, `cmd/solana-snapshot-finder`.

**Spec:** `docs/superpowers/specs/2026-07-28-probe-efficiency-reliability-design.md`

## Global Constraints

- Cluster discovery stays default; whitelist remains optional
- No download resume / Range continuation across retries
- Evaluation never HEADs `/incremental-snapshot` for slots; post-selection `ProbeIncrementalInfo` / `FindMatchingIncremental` stays unchanged and separate
- Full-slot HEAD-wins is selection-time only (no RPC reconcile); post-download checks remain correctness backstop
- No age cutoff on warm-start cache (`warm_start_max_age` deferred)
- Conventional commits: `type(scope): description` with lowercase imperative; prefer scopes like `node-eval`, `snapshot`, `config`
- Run tests with: `go test ./...`
- Do not skip hooks; use `/usr/bin/git` if the local git shim blocks commits

## File map

| File | Responsibility |
|------|----------------|
| `pkg/config/config.go` | New knobs: `speed_test_workers`, `speed_test_max_bytes`, `warm_start_min_nodes` + defaults |
| `pkg/config/config.yaml.example` | Document new knobs |
| `pkg/rpc/node_utils.go` | Replace `MeasureSpeed`; Phase B semaphore; HEAD-derived full slot; `ReclassifyResults`; warm-start load helpers |
| `pkg/rpc/selection.go` (create) | `SelectNextRPC`, `HostCooldown` / Retry-After helpers (moved/extended from `process.go`) |
| `pkg/rpc/*_test.go` | Unit tests for measure/reclassify/warm-start/cooldown |
| `pkg/snapshot/download_utils.go` | Incremental base≠full → remove file + return error; optional Retry-After surfacing from download errors |
| `pkg/snapshot/download_utils_test.go` (create or extend) | Incremental hard-fail regression |
| `cmd/solana-snapshot-finder/process.go` | Evaluate once; re-score loop; warm-start order; use `rpc.SelectNextRPC` + cooldown; phase logs |
| `README.md` | Document new behavior/knobs |

---

### Task 1: Config knobs

**Files:**
- Modify: `pkg/config/config.go`
- Modify: `pkg/config/config.yaml.example`
- Test: `pkg/config/config_test.go` (create if missing; otherwise assert defaults via LoadConfig against a temp yaml)

**Interfaces:**
- Produces: `Config.SpeedTestWorkers int`, `Config.SpeedTestMaxBytes int64`, `Config.WarmStartMinNodes int` with defaults `5`, `268435456`, `3`

- [ ] **Step 1: Write failing default assertions**

Create `pkg/config/config_test.go`:

```go
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
```

- [ ] **Step 2: Run test — expect fail**

Run: `go test ./pkg/config/ -run TestNewKnobDefaults -v`  
Expected: FAIL (unknown fields / zero values)

- [ ] **Step 3: Add fields + defaults**

In `pkg/config/config.go` struct:

```go
SpeedTestWorkers   int   `mapstructure:"speed_test_workers"`
SpeedTestMaxBytes  int64 `mapstructure:"speed_test_max_bytes"`
WarmStartMinNodes  int   `mapstructure:"warm_start_min_nodes"`
```

In `LoadConfig` defaults:

```go
viper.SetDefault("speed_test_workers", 5)
viper.SetDefault("speed_test_max_bytes", int64(268435456))
viper.SetDefault("warm_start_min_nodes", 3)
```

Update `config.yaml.example` with commented entries matching the spec table.

- [ ] **Step 4: Run test — expect pass**

Run: `go test ./pkg/config/ -run TestNewKnobDefaults -v`

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/config/config.go pkg/config/config.yaml.example pkg/config/config_test.go
/usr/bin/git commit -m "$(cat <<'EOF'
feat(config): add speed-test worker, byte-cap, and warm-start knobs

EOF
)"
```

---

### Task 2: Replace `MeasureSpeed` (timed + byte cap + Range)

**Files:**
- Modify: `pkg/rpc/node_utils.go` (`MeasureSpeed` signature and body ~lines 129–174)
- Modify: `pkg/rpc/node_utils_test.go`
- Modify call sites in `EvaluateNodesWithVersions` to pass `cfg.SpeedTestMaxBytes`

**Interfaces:**
- Consumes: `cfg.SpeedTestSeconds`, `cfg.SpeedTestMaxBytes`
- Produces: `func MeasureSpeed(url string, measureTime int, maxBytes int64) (speedMBs float64, latencyMs float64, err error)`

- [ ] **Step 1: Write failing httptest tests**

```go
func TestMeasureSpeedStopsAtByteCap(t *testing.T) {
	var readBytes int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Header.Get("Range") == "" {
			t.Errorf("expected Range header")
		}
		payload := bytes.Repeat([]byte("x"), 512*1024) // 512 KiB chunks
		for {
			n, _ := w.Write(payload)
			readBytes += int64(n)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			if readBytes > 2*1024*1024 {
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
	if readBytes > 512*1024 {
		t.Fatalf("client read too much: %d", readBytes)
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
```

- [ ] **Step 2: Run tests — expect fail** (old signature / unbounded behavior)

Run: `go test ./pkg/rpc/ -run 'TestMeasureSpeed' -v`

- [ ] **Step 3: Replace `MeasureSpeed` body**

Implementation requirements (replace entire function):

1. Build request with `Range: bytes=0-(maxBytes-1)` when `maxBytes > 0`
2. If status is `416` / `200` without partial / other rejection of Range, retry once with a plain GET
3. Record latency at first byte / headers
4. Read loop: stop when `time.Since(start) >= measureTime` **or** `totalLoaded >= maxBytes` (whichever first)
5. Return median (or overall) MB/s from bytes/elapsed; keep similar return shape `(speed, latency, err)`
6. If `maxBytes <= 0`, treat as “time-only” for safety but production always passes config default

Update call site:

```go
speed, latency, err := MeasureSpeed(snapshotURL, cfg.SpeedTestSeconds, cfg.SpeedTestMaxBytes)
```

- [ ] **Step 4: Run tests — expect pass**

Run: `go test ./pkg/rpc/ -run 'TestMeasureSpeed' -v`

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/rpc/node_utils.go pkg/rpc/node_utils_test.go
/usr/bin/git commit -m "$(cat <<'EOF'
perf(node-eval): replace MeasureSpeed with timed byte-capped Range GET

EOF
)"
```

---

### Task 3: Separate Phase B `speed_test_workers` semaphore

**Files:**
- Modify: `pkg/rpc/node_utils.go` (`EvaluateNodesWithVersions` Phase B loop ~311–393)

**Interfaces:**
- Consumes: `cfg.SpeedTestWorkers` (default 5 if ≤0)
- Produces: Phase A uses `worker_count` sem; Phase B uses distinct `speedSem`

- [ ] **Step 1: Add a structural regression test**

Prefer a small exported helper or comment+test of clamp logic:

```go
func TestSpeedTestWorkerClamp(t *testing.T) {
	if got := speedTestWorkerCount(config.Config{SpeedTestWorkers: 0}); got != 5 {
		t.Errorf("got %d want 5", got)
	}
	if got := speedTestWorkerCount(config.Config{SpeedTestWorkers: 3}); got != 3 {
		t.Errorf("got %d want 3", got)
	}
}
```

Add unexported:

```go
func speedTestWorkerCount(cfg config.Config) int {
	if cfg.SpeedTestWorkers <= 0 {
		return 5
	}
	return cfg.SpeedTestWorkers
}
```

- [ ] **Step 2: Run — expect fail**

- [ ] **Step 3: Wire second semaphore in Phase B**

```go
speedSem := make(chan struct{}, speedTestWorkerCount(cfg))
// in Phase B goroutine:
speedSem <- struct{}{}
defer func() { <-speedSem }()
```

Do **not** acquire the Phase A `sem` in Phase B.

Log: `log.Printf("Phase B: speed-testing %d candidates with %d workers", len(candidates), speedTestWorkerCount(cfg))`

- [ ] **Step 4: Run `go test ./pkg/rpc/ -v`**

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/rpc/node_utils.go pkg/rpc/node_utils_test.go
/usr/bin/git commit -m "$(cat <<'EOF'
perf(node-eval): bound Phase B speed tests with speed_test_workers

EOF
)"
```

---

### Task 4: HEAD-derived full slot (opportunistic) + Phase B reuse

**Files:**
- Modify: `pkg/rpc/node_utils.go` (`probeCandidate`, `checkSnapshotAvailability`, Phase B slot logic)
- Modify: `pkg/rpc/node_utils_test.go`

**Interfaces:**
- Produces: `parseFullSnapshotSlotFromName(name string) (int, bool)`  
- Extends `probeCandidate` with `fullSlot int` (0 if unknown)  
- Phase B: if `c.fullSlot > 0`, use it and **skip** `GetHighestSnapshotSlots` for full classification; still may call RPC only when `fullSlot == 0` (and for `IncrementalSlot` via `getHighestSnapshotSlots` when needed)

- [ ] **Step 1: Unit-test filename parse**

```go
func TestParseFullSnapshotSlotFromName(t *testing.T) {
	slot, ok := parseFullSnapshotSlotFromName("snapshot-12345-AbCdEf.tar.zst")
	if !ok || slot != 12345 {
		t.Fatalf("got %d %v", slot, ok)
	}
	if _, ok := parseFullSnapshotSlotFromName("incremental-snapshot-1-2-x.tar.zst"); ok {
		t.Fatal("incremental name must not parse as full")
	}
}
```

- [ ] **Step 2: Run — expect fail**

- [ ] **Step 3: Implement parse + wire Phase A HEAD filename → `probeCandidate.fullSlot`**

During `checkSnapshotAvailability` (or immediately after successful HEAD), read `filepath.Base(resp.Request.URL.String())` and/or Content-Disposition; set `fullSlot` when parse succeeds.

Phase B:

```go
if c.fullSlot > 0 {
    fullSlot = c.fullSlot
    slot = fullSlot
    log.Printf("Node %s: full slot %d from HEAD filename (selection-time; no RPC reconcile)", c.rpc, fullSlot)
    // Still fetch IncrementalSlot via GetHighestSnapshotSlots when !IsStatic
    if slots, err := GetHighestSnapshotSlots(c.rpc); err == nil {
        incrementalSlot = slots.Incremental
    }
} else {
    // existing GetHighestSnapshotSlots / GetReferenceSlot path
}
```

Static whitelist nodes keep today’s trusted `defaultSlot` behavior.

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/rpc/node_utils.go pkg/rpc/node_utils_test.go
/usr/bin/git commit -m "$(cat <<'EOF'
perf(node-eval): reuse full snapshot slot from HEAD redirect when present

EOF
)"
```

---

### Task 5: Relaxation re-score only (no re-probe)

**Files:**
- Modify: `pkg/rpc/node_utils.go` (`EvaluateNodesWithRelaxedRequirements`)
- Modify: `cmd/solana-snapshot-finder/process.go` (evaluation loop ~160–193)
- Modify: `pkg/rpc/node_utils_test.go`

**Interfaces:**
- Produces: `func ReclassifyResults(results []NodeEvaluationResult, cfg config.Config, defaultSlot int) []NodeEvaluationResult`
- Produces: `func RelaxedConfigForAttempt(cfg config.Config, attempt int) config.Config`
- Change: `EvaluateNodesWithRelaxedRequirements` either deleted or becomes a thin wrapper that **must not** call `EvaluateNodesWithVersions`

- [ ] **Step 1: Failing test — reclassify without network**

```go
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
```

- [ ] **Step 2: Run — expect fail**

- [ ] **Step 3: Implement `ReclassifyResults` + wire process loop**

`ReclassifyResults` rules (match existing Phase B classification, no network):

- Leave `bad` as `bad` (never promote)
- For others: if `speed >= min && latency <= max && slotOK` → `good`, else if speed/latency zero → `bad`, else `slow`
- `slotOK`: static already encoded in Diff/slots; use existing Diff vs `FullThreshold` / `MaxSlot` rules already stored on the result — do **not** relax slot threshold

In `process.go`:

```go
results = rpc.EvaluateNodesWithVersions(nodes, cfg, referenceSlot)
for attempt := 1; attempt <= cfg.MaxRelaxationAttempts; attempt++ {
    attemptCfg := cfg
    if attempt > 1 {
        attemptCfg = rpc.RelaxedConfigForAttempt(cfg, attempt)
        results = rpc.ReclassifyResults(results, attemptCfg, referenceSlot)
    }
    // summarize, max_slot filter, dump, SelectBestRPC — break if bestRPC != ""
}
```

Remove the call path that invoked `EvaluateNodesWithRelaxedRequirements` → `EvaluateNodesWithVersions`.

- [ ] **Step 4: Run `go test ./pkg/rpc/ ./cmd/...` (compile process.go)**

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/rpc/node_utils.go pkg/rpc/node_utils_test.go cmd/solana-snapshot-finder/process.go
/usr/bin/git commit -m "$(cat <<'EOF'
perf(node-eval): re-score relaxation attempts without re-probing nodes

EOF
)"
```

---

### Task 6: Warm-start from `nodes_attempt_*.json`

**Files:**
- Create helpers in: `pkg/rpc/warm_start.go` (or `node_utils.go`)
- Modify: `cmd/solana-snapshot-finder/process.go` (before `EvaluateNodesWithVersions`)
- Test: `pkg/rpc/warm_start_test.go`

**Interfaces:**
- Produces: `func LoadWarmStartRPCs(snapshotPath string, minNodes int) []string`
- Produces: `func PrioritizeNodes(nodes []RPCNode, warmRPCs []string) []RPCNode` — warm addresses first (stable), then the rest; dedupe by normalized address

- [ ] **Step 1: Tests**

```go
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
```

- [ ] **Step 2: Run — expect fail**

- [ ] **Step 3: Implement load + prioritize; wire in process.go**

```go
if warm := rpc.LoadWarmStartRPCs(cfg.SnapshotPath, cfg.WarmStartMinNodes); len(warm) > 0 {
    log.Printf("Warm-start: prioritizing %d cached good/slow nodes", len(warm))
    nodes = rpc.PrioritizeNodes(nodes, warm)
} else {
    log.Printf("Warm-start: skipped (need >= %d good/slow in nodes_attempt_*.json)", cfg.WarmStartMinNodes)
}
```

No age cutoff (spec decision).

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/rpc/warm_start.go pkg/rpc/warm_start_test.go cmd/solana-snapshot-finder/process.go
/usr/bin/git commit -m "$(cat <<'EOF'
feat(node-eval): warm-start probe order from nodes_attempt cache

EOF
)"
```

---

### Task 7: Cooldown extends `selectNextRPC` + Retry-After

**Files:**
- Create: `pkg/rpc/selection.go`
- Create: `pkg/rpc/selection_test.go`
- Modify: `cmd/solana-snapshot-finder/process.go` (replace local `selectNextRPC`; feed 429s)
- Modify: `pkg/snapshot/download_utils.go` and/or download error type to surface `Retry-After` when status is 429

**Interfaces:**
- Produces:

```go
type HostCooldown struct {
    Excluded map[string]string // rpc -> reason
    RetryAfter time.Duration   // wait before next download attempt (max observed)
}

func (h *HostCooldown) Mark(rpc, reason string)
func (h *HostCooldown) MarkRetryAfter(rpc string, d time.Duration)
func (h *HostCooldown) IsExcluded(rpc string) bool
func SelectNextRPC(results []NodeEvaluationResult, cooldown *HostCooldown) string
func ParseRetryAfter(h http.Header, now time.Time) (time.Duration, bool)
```

- [ ] **Step 1: Unit tests**

```go
func TestSelectNextRPCSkipsExcluded(t *testing.T) {
	results := []NodeEvaluationResult{
		{RPC: "http://fast", Speed: 100, Status: "good"},
		{RPC: "http://other", Speed: 80, Status: "good"},
	}
	cd := &HostCooldown{Excluded: map[string]string{"http://fast": "429"}}
	if got := SelectNextRPC(results, cd); got != "http://other" {
		t.Fatalf("got %s", got)
	}
}

func TestParseRetryAfterSeconds(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "7")
	d, ok := ParseRetryAfter(h, time.Now())
	if !ok || d < 7*time.Second {
		t.Fatalf("got %v %v", d, ok)
	}
}

func TestMarkRetryAfterKeepsMax(t *testing.T) {
	cd := &HostCooldown{Excluded: map[string]string{}}
	cd.MarkRetryAfter("http://a", 2*time.Second)
	cd.MarkRetryAfter("http://b", 5*time.Second)
	if cd.RetryAfter != 5*time.Second {
		t.Fatalf("got %v", cd.RetryAfter)
	}
}
```

- [ ] **Step 2: Run — expect fail**

- [ ] **Step 3: Implement selection helpers; wire process + downloads**

- Move logic from `process.go` `selectNextRPC` into `rpc.SelectNextRPC`
- In download retry loop: on failure, `cooldown.Mark(bestRPC, reason)`; if error wraps 429 / carries Retry-After, `MarkRetryAfter`
- Before next attempt: if `cooldown.RetryAfter > 0 { log ...; time.Sleep(cooldown.RetryAfter); cooldown.RetryAfter = 0 }`
- In Phase A/B, when HTTP status is 429, mark that host in a shared cooldown passed into evaluation **or** collect 429 hosts and merge into cooldown before download — simplest acceptable approach: evaluation logs 429 and records into a `HostCooldown` created in `process.go` and passed if you thread it; if threading is too invasive for one task, mark 429s only on the download path in this task and leave probe-429 marking as a thin follow-up inside the same PR by recording in `NodeEvaluationResult.Status`/`bad` (already happens on failed health) plus explicit Mark when download returns 429

Minimum for this task (must ship):

1. Download 429 → exclude host + honor Retry-After sleep before next attempt  
2. Download hard failure → exclude host (existing behavior)  
3. Probe-path 429 → mark excluded before selection if easy; else document in commit that probe 429 already yields `bad` and is not selected

For `writeSnapshotToFile` / `DownloadSnapshot`: on `resp.StatusCode == 429`, return a typed error including parsed Retry-After so process can sleep.

```go
type HTTPStatusError struct {
    StatusCode int
    RetryAfter time.Duration
    URL        string
}
func (e *HTTPStatusError) Error() string { /* ... */ }
```

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/rpc/selection.go pkg/rpc/selection_test.go pkg/snapshot/download_utils.go cmd/solana-snapshot-finder/process.go
/usr/bin/git commit -m "$(cat <<'EOF'
fix(node-eval): extend RPC rotation with cooldown and Retry-After

EOF
)"
```

---

### Task 8: Incremental base≠full hard failure

**Files:**
- Modify: `pkg/snapshot/download_utils.go` (~337–356)
- Create: `pkg/snapshot/download_utils_test.go` (or extend)

**Interfaces:**
- Behavior change only in post-download incremental validation path inside `DownloadSnapshot`

- [ ] **Step 1: Failing regression test**

Create remote/full fixtures under `t.TempDir()`:

```go
func TestIncrementalMismatchFails(t *testing.T) {
	dir := t.TempDir()
	// Place a fake full snapshot name the finder will see:
	fullName := "snapshot-100-AbCdEfGhIjKlMnOpQrStUvWx.tar.zst"
	if err := os.WriteFile(filepath.Join(dir, fullName), []byte("full"), 0644); err != nil {
		t.Fatal(err)
	}
	remote := filepath.Join(dir, "remote")
	_ = os.MkdirAll(remote, 0755)
	incName := "incremental-snapshot-999-1000-AbCdEfGhIjKlMnOpQrStUvWx.tar.zst"
	incPath := filepath.Join(remote, incName)
	if err := os.WriteFile(incPath, bytes.Repeat([]byte("z"), 2*1024*1024), 0644); err != nil {
		t.Fatal(err)
	}

	// Call the validation logic via a small exported helper if DownloadSnapshot is too heavy.
	// Prefer extracting:
	//   func validateDownloadedIncremental(cfg config.Config, finalPath string, referenceSlot int) error
	err := validateDownloadedIncremental(config.Config{SnapshotPath: dir, IncrementalThreshold: 100000}, incPath, 1000)
	if err == nil {
		t.Fatal("expected mismatch error")
	}
	if _, statErr := os.Stat(incPath); !os.IsNotExist(statErr) {
		t.Fatal("mismatched incremental should be removed")
	}
}
```

If extracting a helper is cleaner, do that; keep `DownloadSnapshot` calling it.

- [ ] **Step 2: Run — expect fail** (today warns and keeps)

- [ ] **Step 3: Change warn-and-keep to remove + error**

Replace:

```go
if slotStart != fullSlot {
    log.Printf("Warning: Incremental snapshot might not match full snapshot, but keeping it anyway")
}
```

With:

```go
if slotStart != fullSlot {
    log.Printf("Incremental base slot %d does not match full snapshot slot %d — removing and failing attempt", slotStart, fullSlot)
    _ = os.Remove(finalPath)
    return fmt.Errorf("incremental base slot %d != full slot %d", slotStart, fullSlot)
}
```

- [ ] **Step 4: Run tests**

- [ ] **Step 5: Commit**

```bash
/usr/bin/git add pkg/snapshot/download_utils.go pkg/snapshot/download_utils_test.go
/usr/bin/git commit -m "$(cat <<'EOF'
fix(snapshot): fail and remove incremental on base-slot mismatch

EOF
)"
```

---

### Task 9: Phase logs + README

**Files:**
- Modify: `cmd/solana-snapshot-finder/process.go` (phase banners if not already added)
- Modify: `pkg/rpc/node_utils.go` (slot source / Range fallback / warm-start skip already logged in prior tasks — ensure complete)
- Modify: `README.md`
- Modify: `pkg/config/config.yaml.example` (final pass)

- [ ] **Step 1: Ensure logs exist for**

- `probe → shortlist → speed-test → select → download`
- HEAD vs RPC full-slot source
- Range fallback
- Warm-start skip below min
- Cooldown reason + Retry-After value
- Incremental mismatch hard-fail

- [ ] **Step 2: Update README Key Features / How It Works / Config** to match spec (re-score, speed_test_workers, speed_test_max_bytes, warm_start_min_nodes, stricter incremental)

- [ ] **Step 3: Run full suite**

Run: `go test ./...`

- [ ] **Step 4: Commit**

```bash
/usr/bin/git add README.md pkg/config/config.yaml.example cmd/solana-snapshot-finder/process.go pkg/rpc/node_utils.go
/usr/bin/git commit -m "$(cat <<'EOF'
docs(node-eval): document probe efficiency and reliability behavior

EOF
)"
```

---

## Spec coverage checklist

| Spec item | Task |
|-----------|------|
| Config knobs | 1 |
| Replace MeasureSpeed | 2 |
| Separate Phase B semaphore | 3 |
| HEAD-derived full slot, no RPC reconcile | 4 |
| Evaluation never incremental HEAD | 4 (unchanged ProbeIncrementalInfo) |
| Relaxation re-score only | 5 |
| Warm-start from nodes_attempt_*.json + min nodes, no age cutoff | 6 |
| Cooldown extends selectNextRPC + Retry-After | 7 |
| Incremental mismatch hard fail | 8 |
| Logging + README | 9 |
| Preserve no pre-download HEAD | (no change; verify in Task 8/9) |
| Post-selection FindMatchingIncremental remains | (explicit non-touch; verify in Task 4/9) |

## Self-review notes

- No resume work planned (non-goal)
- `ProbeIncrementalInfo` not folded into Phase A/B
- `speed_test_max_bytes` default justified in spec; plan uses 256 MiB as configured
- Commit messages follow repo conventional style used on this branch

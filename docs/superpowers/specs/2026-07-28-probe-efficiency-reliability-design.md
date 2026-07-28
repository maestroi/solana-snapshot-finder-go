# Probe Efficiency & Reliability Design

Date: 2026-07-28  
Branch context: `perf/reduce-node-probe-load`  
Status: Revised after second consistency pass — pending re-approval for planning

## Goals

- Reduce load on peer nodes during evaluation
- Shorten time-to-download without changing cluster discovery as the default
- Improve selection/download reliability (429 handling, no wasted re-probing, post-download checks)

## Non-goals

- Download resume / Range continuation across retries (snapshot URLs rotate; resume is unreliable)
- Changing default discovery to whitelist-first (peers come and go; cluster scan stays default)
- Multipath / parallel multi-source downloads
- Full cryptographic re-hash of multi-GB archives

## What already exists (baseline)

These are **not** new work; the design preserves or builds on them:

- Phase A/B split in `EvaluateNodesWithVersions` (`pkg/rpc/node_utils.go`): cheap health+HEAD+latency probe, then latency-sorted shortlist truncated to `speed_test_candidates`, then speed tests
- No pre-download HEAD before the real GET (`pkg/snapshot/download_utils.go`) — already removed to avoid self-inflicted 429s; keep that behavior
- Small-file (&lt;1MB) rejection and full-snapshot filename/slot parsing already exist
- `DumpGoodAndSlowNodesToFile` already writes `nodes_attempt_<N>.json` per relaxation attempt (`process.go`)
- `selectNextRPC` already permanently excludes failed download hosts for the rest of the retry loop (`process.go`)

## Explicit architecture / behavior changes

These are the non-incremental parts of the design — treat them as real tasks in the implementation plan, not config tweaks:

| # | Change | Why it matters |
|---|--------|----------------|
| 1 | **Relaxation re-score only** | Today `EvaluateNodesWithRelaxedRequirements` calls `EvaluateNodesWithVersions` again in full — health, HEAD, and speed test all rerun every attempt. Largest gap; architectural change to evaluate once and re-classify. |
| 2 | **Separate Phase B semaphore** | Phase B currently reuses the same `worker_count`-bounded semaphore as Phase A. `speed_test_workers` requires threading a **second** concurrency gate through Phase B, not only reading a new config default. |
| 3 | **Replace `MeasureSpeed` body** | Today: unbounded plain GET for `speed_test_seconds` — no Range, no byte cap. Timed + `speed_test_max_bytes` + Range-with-fallback **replaces** that function entirely; it is not refining an existing capped path. |
| 4 | **Cooldown extends `selectNextRPC` failure set** | No 429 / Retry-After / cooldown scaffolding exists today. Build on `selectNextRPC`'s exclude map: broaden it into an in-run cooldown set (rate-limit + hard failures), optionally honor `Retry-After` before the next attempt — not an orthogonal subsystem. |
| 5 | **Incremental slot mismatch becomes a hard failure** | Today incremental base≠full only **warns and keeps the file** (`download_utils.go`). This design changes that to **fail the attempt, remove the bad file, and rotate** — stricter than current silent leniency. |

## Success criteria

- Relaxation retries do not re-run health/HEAD/speed tests (criterion #1 above)
- Speed tests use the replaced MeasureSpeed (time + byte cap + Range fallback) under a separate `speed_test_workers` semaphore
- Full-slot parsing from HEAD redirect is best-effort only during evaluation; **evaluation** never uses incremental HEAD for slots (see Incremental slot sources)
- Rate-limited / failing hosts are excluded via the extended `selectNextRPC` cooldown set for the rest of the run
- Full downloads keep existing slot checks; **incremental mismatch with local full base slot fails and rotates** (behavior change #5)
- Warm-start only activates when the reused `nodes_attempt_*.json` cache has ≥ `warm_start_min_nodes` good/slow peers

## Architecture

Cluster discovery remains the default. Whitelist stays optional (`additional` / `only` / `disabled` as today).

```text
Fetch cluster nodes (+ optional whitelist)
        │
        ▼
Phase A: cheap probe (health + full snapshot HEAD + latency)
        │  worker_count semaphore
        │  warm-start priority if nodes_attempt_*.json has ≥ warm_start_min_nodes
        ▼
Shortlist by latency → speed_test_candidates
        │
        ▼
Phase B: speed test
        │  NEW separate speed_test_workers semaphore (not worker_count)
        │  REPLACED MeasureSpeed: timed + max bytes, Range then fallback GET
        │  full slot: HEAD filename wins if present (no RPC reconcile); else RPC
        │  IncrementalSlot (evaluation): getHighestSnapshotSlots only — never HEAD
        ▼
Classify good / slow / bad  (ONCE per run)
        │
        ├─ relaxation attempts: re-score only (no re-probe)  ← architectural change
        ▼
Select best RPC → download (preserve: no pre-download HEAD)
        │  extend selectNextRPC exclude set with 429/cooldown + Retry-After
        │  post-selection: FindMatchingIncremental / ProbeIncrementalInfo may still
        │  HEAD incremental endpoints (separate from evaluation; see below)
        ▼
Filename / slot sanity check
        │  incremental base≠full: FAIL + remove + rotate (stricter than today)
        ▼
Retention cleanup
```

### Incremental slot sources (two paths — do not conflate)

| Path | When | Slot source | HEAD `/incremental-snapshot`? |
|------|------|-------------|-------------------------------|
| **Evaluation (Phase A/B)** | Classifying nodes before download | `getHighestSnapshotSlots` only for `IncrementalSlot` | **Never.** Mass incremental HEAD is unreliable and out of scope for evaluation. |
| **Post-selection matching** | After a full slot is chosen — safety incremental via existing `FindMatchingIncremental` → `ProbeIncrementalInfo` | Filename slots from that targeted probe | **Allowed to remain.** This is a separate parallel path that matches one artifact to a known full slot; it is not evaluation “slot discovery.” If HEAD fails, matching fails as today (skip safety incremental with warning). |

Implementers must not “fold” `ProbeIncrementalInfo` into Phase A/B, remove it, or reinterpret the evaluation rule as banning this post-selection matcher. Evaluation rule and matcher path are intentionally different.

## Evaluation pipeline

### Phase A — cheap probe

- Health check on `/health` (skipped for static whitelist hosts)
- HEAD `/snapshot` (+ extensions) for availability + latency
- Opportunistic full-slot parse from redirect / Content-Disposition filename when present
- On HTTP 429 during probe: mark host in the in-run cooldown/exclude set
- Do **not** HEAD incremental endpoints during Phase A (evaluation rule above)
- Concurrency: existing `worker_count` semaphore (unchanged role: Phase A only after the split)

### Warm-start (priority only)

**Choice:** reuse the existing `nodes_attempt_*.json` dump format produced by `DumpGoodAndSlowNodesToFile`. No dedicated `nodes_cache.json` writer — zero new serialization.

- On startup, load the newest `nodes_attempt_*.json` under `snapshot_path` (highest attempt number, or most recently modified if numbering is ambiguous)
- Collect addresses previously marked `good` or `slow`
- **Minimum diversity:** use warm-start only if that set has at least `warm_start_min_nodes` entries (default **3**). If fewer, skip warm-start so traffic does not stick to a single peer
- When active: probe warm-start addresses first so healthy ones tend to appear earlier in the latency shortlist
- Always still fetch and probe the full cluster list afterward — cache never replaces discovery
- Missing/unreadable cache is ignored quietly

### Phase B — speed test

- Sort Phase A survivors by latency; keep `speed_test_candidates` (default 30)
- **Concurrency architecture change:** introduce a second semaphore bounded by `speed_test_workers` (default 5). Phase B must not share Phase A's `worker_count` gate.
- **Replace `MeasureSpeed` entirely** (`pkg/rpc/node_utils.go` today: unbounded GET for N seconds):
  - Prefer `GET` with `Range: bytes=0-(speed_test_max_bytes-1)` when the server accepts it
  - If Range is rejected (or unsupported status), fall back to a normal GET with the same stop rules
  - Stop when **either** `speed_test_seconds` elapses **or** `speed_test_max_bytes` is reached
  - Compute MB/s from bytes / elapsed so ~500 MB/s links still get a meaningful sample
- **Full slot tie-break:** if Phase A already derived a slot from the full-snapshot HEAD/redirect filename, **HEAD wins** — do not call RPC to reconcile, and do not cross-check against `getHighestSnapshotSlots`. Rationale: the download follows the same HTTP redirect, so the HEAD filename is the artifact that will be served; RPC metadata can disagree and is less relevant for what we pull. If no HEAD-derived slot exists, fall back to `getHighestSnapshotSlots` / `getSlot` as today.
- **IncrementalSlot (evaluation only):** always from `getHighestSnapshotSlots`. Never from incremental HEAD during Phase B. Post-selection `ProbeIncrementalInfo` is unchanged and separate (see table above).
- Slot threshold (`full_threshold`) stays strict — never relaxed

### `speed_test_max_bytes` vs peer-load goal

Default **256 MiB** is an intentional accuracy/load tradeoff, not an unbounded pull:

- **Vs today:** current `MeasureSpeed` has **no byte cap**. A 3s GET on a ~500 MB/s link already transfers ~1.5 GiB per candidate. Capping at 256 MiB **reduces** bytes pulled from fast peers versus status quo while still giving a usable sample at high throughput.
- **Primary load wins** in this design still come from (1) shortlisting to `speed_test_candidates` and (2) not re-running Phase A/B on relaxation — not from the byte cap alone.
- **Worst-case math:** up to `speed_test_candidates` × `speed_test_max_bytes` (e.g. 30 × 256 MiB ≈ 7.5 GiB) if every shortlisted node is fast enough to hit the cap. That is acceptable relative to today’s uncapped timed GETs on the same shortlist, but the default is a **tunable** — measure production impact and lower `speed_test_max_bytes` or `speed_test_candidates` if operators still see excess probe bandwidth.

### Relaxation retries

- Run Phase A + B **once** per process run
- Change `EvaluateNodesWithRelaxedRequirements` (or its caller) so later attempts only re-classify existing results with relaxed speed/latency thresholds — **do not** call `EvaluateNodesWithVersions` again
- If still no `good` nodes after max attempts, keep today’s fallback to best available `slow` node

## Download reliability

- **Preserve:** no pre-download HEAD (current anti-429 behavior)
- **Extend `selectNextRPC`:** today it permanently excludes hosts that failed a download for the rest of the retry loop. Broaden that exclude set into an in-run cooldown map also fed by 429 / connection refusal / repeated timeouts from probe or download. Same mechanism, more inputs — not a parallel feature.
- Honor `Retry-After` when present before the next download attempt (new; no scaffolding today)
- Failed downloads: clean partials and restart from another node (no resume)
- After successful write:
  - Filename must match expected prefix (`snapshot-` / `incremental-snapshot-`)
  - Keep existing reject of trivially small (&lt;1MB) responses
  - Full snapshot: keep existing newer-than-local / max_slot checks
  - **Behavior change (incremental):** if `slotStart !=` local full snapshot slot, **do not keep the file** — remove it, return an error, and let the download retry loop rotate to another node. Today this only logs a warning and keeps the mismatched incremental.
- Do not add full-archive cryptographic verification in this pass

## Configuration

| Key | Default | Purpose |
|-----|---------|---------|
| `speed_test_workers` | `5` | **New** Phase B semaphore bound (separate from `worker_count`) |
| `speed_test_max_bytes` | `268435456` (256 MiB) | **New** byte cap inside replaced `MeasureSpeed` (see load rationale above; tunable after measurement) |
| `speed_test_seconds` | existing | Max duration of a speed test |
| `speed_test_candidates` | `30` | Latency shortlist size (already exists) |
| `warm_start_min_nodes` | `3` | Minimum cached good/slow nodes before warm-start activates |
| `worker_count` | existing | Phase A probe concurrency only (after semaphore split) |

Existing relaxation, threshold, whitelist, and download-retry knobs remain unchanged.

## Logging

- Clear phase lines: probe → shortlist → speed-test → select → download
- Log whether full slot came from HEAD filename vs RPC (and that HEAD won without RPC reconcile when applicable)
- Log when Range speed-test fell back to non-Range GET
- Log when warm-start was skipped due to fewer than `warm_start_min_nodes` entries
- Log when an incremental is rejected for base-slot mismatch (new hard-fail path)
- Log cooldown/exclude events with reason (`429`, timeout, download failure) and any observed `Retry-After` value (seconds or HTTP-date as received) before sleeping/skipping

## Testing

- Unit: relaxation re-scores without calling into probe/speed-test paths
- Unit: evaluation incremental slots do not require HEAD; post-selection `ProbeIncrementalInfo` remains a separate callable path
- Unit: HEAD-derived full slot is used without requiring an RPC cross-check
- Unit: replaced MeasureSpeed stops on time **or** byte cap; Range rejection falls back cleanly
- Unit: Phase B uses `speed_test_workers` bound (not `worker_count`) — at least a structural/regression assertion around the split gates if practical
- Unit: cooldown/exclude set skips host in `selectNextRPC`-style selection
- Unit: when a response carries `Retry-After`, the retry path waits at least that duration before the next download attempt
- Unit: warm-start ignored when cached good/slow nodes &lt; `warm_start_min_nodes`; loads from `nodes_attempt_*.json` format
- Unit: **behavior change** — incremental base≠full returns error, removes the mismatched file, and does not treat the download as success (regression vs today’s warn-and-keep)
- Existing evaluation/download tests remain green

## Docs

- Update README and `config.yaml.example` for new knobs, Phase B semaphore split, MeasureSpeed replacement, and stricter incremental mismatch handling

## Out of scope follow-ups

- Same-URL resume only when redirected filename is unchanged
- Early exit from Phase B once N good nodes exist
- Lower default `speed_test_candidates` after measuring production impact
- Dedicated `nodes_cache.json` (rejected in favor of reusing `nodes_attempt_*.json`)

# Probe Efficiency & Reliability Design

Date: 2026-07-28  
Branch context: `perf/reduce-node-probe-load`  
Status: Approved for planning

## Goals

- Reduce load on peer nodes during evaluation
- Shorten time-to-download without changing cluster discovery as the default
- Improve selection/download reliability (429 handling, no wasted re-probing, post-download checks)

## Non-goals

- Download resume / Range continuation across retries (snapshot URLs rotate; resume is unreliable)
- Changing default discovery to whitelist-first (peers come and go; cluster scan stays default)
- Multipath / parallel multi-source downloads
- Full cryptographic re-hash of multi-GB archives

## Success criteria

- Relaxation retries do not re-run health/HEAD/speed tests
- Speed tests use timed + byte-capped pulls and a separate low concurrency limit
- Full-slot parsing from HEAD redirect is best-effort only; incremental never depends on HEAD for slots
- Rate-limited / failing hosts are cooled down and skipped for the rest of the run
- Completed downloads get filename/slot sanity checks before success
- Warm-start only activates when the cache has enough node diversity

## Architecture

Cluster discovery remains the default. Whitelist stays optional (`additional` / `only` / `disabled` as today).

```text
Fetch cluster nodes (+ optional whitelist)
        │
        ▼
Phase A: cheap probe (health + full snapshot HEAD + latency)
        │  warm-start priority if cache has ≥ warm_start_min_nodes
        ▼
Shortlist by latency → speed_test_candidates
        │
        ▼
Phase B: speed test (speed_test_workers, timed + max bytes)
        │  full slot from HEAD filename if present, else RPC
        │  incremental slots via getHighestSnapshotSlots only
        ▼
Classify good / slow / bad
        │
        ├─ relaxation attempts: re-score only (no re-probe)
        ▼
Select best RPC → download (no pre-download HEAD)
        │  429 / failures → in-run cooldown + rotate node
        ▼
Filename / slot sanity check → retention cleanup
```

## Evaluation pipeline

### Phase A — cheap probe

- Health check on `/health` (skipped for static whitelist hosts)
- HEAD `/snapshot` (+ extensions) for availability + latency
- Opportunistic full-slot parse from redirect / Content-Disposition filename when present
- Record per-host outcomes; on HTTP 429 mark host for in-run cooldown
- Incremental snapshot HEAD is not used for slot discovery (known unreliable on many nodes)

### Warm-start (priority only)

- On startup, load a recent results file under `snapshot_path` (last `nodes_attempt_*.json` or a dedicated `nodes_cache.json`)
- Collect addresses previously marked `good` or `slow`
- **Minimum diversity:** use warm-start only if that set has at least `warm_start_min_nodes` entries (default **3**). If fewer, skip warm-start so traffic does not stick to a single peer
- When active: probe warm-start addresses first so healthy ones tend to appear earlier in the latency shortlist
- Always still fetch and probe the full cluster list afterward — cache never replaces discovery
- Missing/unreadable/stale cache is ignored quietly

### Phase B — speed test

- Sort Phase A survivors by latency; keep `speed_test_candidates` (default 30)
- Concurrency uses `speed_test_workers` (default 5), **not** `worker_count`
- Measurement:
  - Prefer `GET` with `Range: bytes=0-(speed_test_max_bytes-1)` when supported
  - If Range is rejected, fall back to a normal GET
  - Stop when **either** `speed_test_seconds` elapses **or** `speed_test_max_bytes` is reached
  - Compute MB/s from bytes / elapsed so ~500 MB/s links still get a meaningful sample
- Full slot: reuse HEAD-derived slot when available; otherwise `getHighestSnapshotSlots` / `getSlot`
- Incremental slot: always from `getHighestSnapshotSlots` (or existing matching incremental probe path that does not rely on HEAD for correctness)
- Slot threshold (`full_threshold`) stays strict — never relaxed

### Relaxation retries

- Run Phase A + B **once** per process run
- Later attempts only re-classify existing results with relaxed speed/latency thresholds
- If still no `good` nodes after max attempts, keep today’s fallback to best available `slow` node

## Download reliability

- No pre-download HEAD (keeps current anti-429 behavior)
- On 429 / connection refusal / repeated timeouts: add host to in-run cooldown; `selectNextRPC` skips cooled-down hosts
- Honor `Retry-After` when present before the next download attempt
- Failed downloads: clean partials and restart from another node (no resume)
- After successful write:
  - Filename must match expected prefix (`snapshot-` / `incremental-snapshot-`)
  - Keep existing reject of trivially small (&lt;1MB) responses
  - If evaluation provided an expected full/incremental slot (&gt; 0) and the filename slot differs from it, fail the attempt and rotate
- Do not add full-archive cryptographic verification in this pass

## Configuration

| Key | Default | Purpose |
|-----|---------|---------|
| `speed_test_workers` | `5` | Concurrency for Phase B only |
| `speed_test_max_bytes` | `268435456` (256 MiB) | Cap bytes pulled during a speed test |
| `speed_test_seconds` | existing | Max duration of a speed test |
| `speed_test_candidates` | `30` | Latency shortlist size |
| `warm_start_min_nodes` | `3` | Minimum cached good/slow nodes before warm-start activates |
| `worker_count` | existing | Phase A probe concurrency only |

Existing relaxation, threshold, whitelist, and download-retry knobs remain unchanged.

## Logging

- Clear phase lines: probe → shortlist → speed-test → select → download
- Log whether full slot came from HEAD filename vs RPC
- Log when Range speed-test fell back to uncapped-Range timed GET
- Log when warm-start was skipped due to fewer than `warm_start_min_nodes` entries

## Testing

- Unit: relaxation re-scores without re-probing
- Unit: parse full snapshot slot from filename; incremental path does not require HEAD for slots
- Unit: speed-test stops on time **or** byte cap; Range rejection falls back cleanly
- Unit: cooldown excludes host from selection
- Unit: warm-start ignored when cached nodes &lt; `warm_start_min_nodes`
- Existing evaluation/download tests remain green

## Docs

- Update README and `config.yaml.example` for new knobs and phase behavior

## Out of scope follow-ups

- Same-URL resume only when redirected filename is unchanged
- Early exit from Phase B once N good nodes exist
- Lower default `speed_test_candidates` after measuring production impact

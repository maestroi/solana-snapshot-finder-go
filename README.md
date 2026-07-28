# Solana Snapshot Finder Go

## Overview

Solana Snapshot Finder is a Go utility designed to efficiently manage Solana blockchain snapshots. It automates the process of finding, downloading, and maintaining up-to-date snapshots for Solana validators, helping to reduce node restart times and ensure reliable operation.

## Key Features

- **Smart Snapshot Detection**: Identifies when new snapshots are needed based on configurable thresholds
- **Redundant Download Prevention**: Compares remote snapshot slots with local snapshots to avoid unnecessary downloads
- **Two-Phase Node Evaluation**: Cheap health+HEAD probe on every node, latency shortlist, then byte-capped speed tests on only the closest candidates
- **Single-Pass Evaluation with Re-Score**: Probes and speed-tests once per run; relaxation attempts only re-classify stored measurements (no re-probe)
- **Warm-Start Priority**: Reuses prior `nodes_attempt_*.json` results to probe known-good peers first when enough cached nodes exist
- **Download Cooldown & Retry-After**: Failed or rate-limited hosts are excluded for the rest of the run; HTTP 429 responses honor `Retry-After` before the next attempt
- **Strict Incremental Validation**: Incremental base slots older than the local full are removed and fail the attempt; bases ahead of the local full are kept as safety incrementals (for download-before-full)
- **Automated Cleanup**: Removes outdated snapshots to save disk space while maintaining necessary backups
- **Full & Incremental Support**: Handles both full and incremental snapshots with proper slot validation
- **Adaptive Retry Logic**: Gradually relaxes speed and latency requirements on retry attempts to ensure snapshot acquisition
- **Download Retry Logic**: Automatically retries failed downloads with cleanup and fallback to the next-best node

## Configuration

The tool uses a YAML configuration file. See `pkg/config/config.yaml.example` for a full example.

```yaml
rpc_address: "https://api.mainnet-beta.solana.com"
snapshot_path: "./snapshots"
min_download_speed: 100                             # Minimum acceptable download speed (MB/s)
max_latency: 200                                    # Maximum acceptable latency (ms)
worker_count: 100                                   # Phase A probe concurrency
speed_test_candidates: 30                           # Latency shortlist size for Phase B
speed_test_workers: 5                               # Phase B speed-test concurrency (separate from worker_count)
speed_test_max_bytes: 268435456                     # Byte cap per speed test (256 MiB default)
speed_test_seconds: 3                               # Max duration of each speed test (seconds)
warm_start_min_nodes: 3                             # Min cached good/slow nodes before warm-start activates
full_threshold: 100000                              # Slots threshold for full snapshot updates (Agave 3.x)
incremental_threshold: 500                          # Slots threshold for incremental updates (local check)
speed_relaxation_factor: 0.9                        # Factor to reduce speed requirements on retries
latency_relaxation_factor: 0.9                      # Factor to increase latency tolerance on retries
max_relaxation_attempts: 3                          # Maximum re-score attempts with relaxed requirements
max_download_retries: 3                             # Maximum number of download retry attempts
sleep_before_retry: 5                               # Time between relaxation attempts (seconds)
download_incremental_first: true                    # Safety mode: grab matching incremental before full download
```

### Retry Relaxation Example

Evaluation runs once. If no node meets the initial speed/latency bar, later attempts **re-score** the same measurements with relaxed thresholds — slot requirements stay strict.

With `speed_relaxation_factor: 0.9` and `latency_relaxation_factor: 0.9`:

- **Attempt 1**: Speed ≥ 100 MB/s, Latency ≤ 200 ms (original requirements)
- **Attempt 2**: Speed ≥ 90 MB/s, Latency ≤ 222 ms
- **Attempt 3**: Speed ≥ 81 MB/s, Latency ≤ 247 ms

## How It Works

The run proceeds through clear phases logged to stdout:

**probe → shortlist → speed-test → select → download**

1. **Phase A — Probe** (`worker_count` concurrency):
   - Health check on `/health` (skipped for static whitelist hosts)
   - HEAD request on full snapshot endpoint to confirm availability and derive full slot from filename when possible
   - Records latency for each healthy node with a snapshot

2. **Shortlist**:
   - Sorts survivors by latency
   - Keeps only the closest `speed_test_candidates` (default 30) for Phase B

3. **Phase B — Speed Test** (`speed_test_workers` concurrency, separate from Phase A):
   - Prefers `Range: bytes=0-(speed_test_max_bytes-1)`; falls back to a capped plain GET on HTTP 200/416 (many Solana RPC hosts ignore Range and answer 200)
   - Full slot: uses HEAD-derived filename slot when available (no RPC reconcile); otherwise `getHighestSnapshotSlots` / `getSlot`
   - Incremental slot for evaluation: always from RPC, never from incremental HEAD
   - Classifies nodes as good, slow, or bad based on speed, latency, and slot thresholds

4. **Warm-Start** (before evaluation):
   - Reads prior `nodes_attempt_*.json` for good/slow nodes
   - If count ≥ `warm_start_min_nodes`, those addresses are probed first; otherwise warm-start is skipped
   - Full cluster discovery still runs — cache only affects probe order

5. **Select**:
   - Runs once after evaluation; relaxation attempts re-score only (no re-probe)
   - Picks the fastest node classified as "good"; falls back to best slow node if all attempts exhaust

6. **Download**:
   - No HEAD pre-check before GET (node was already probed; avoids extra 429 risk)
   - On failure or HTTP 429: host enters cooldown, `Retry-After` is honored before the next attempt, rotation via `SelectNextRPC`
   - Incremental: base older than local full → remove and fail; base ahead of local full → keep (safety); equal → keep

7. **Snapshot Management**:
   - Stores snapshots under `snapshot_path` and `snapshot_path/remote`
   - Cleans up old snapshots based on configurable thresholds

## Observed performance (devnet)

Measured on a Blockdaemon devnet RPC node after deploying this branch (`speed_test_workers: 5`, `speed_test_max_bytes: 256 MiB`, `full_threshold: 100000`, `incremental_threshold: 500`):

| Scenario | What happened | Wall time |
|----------|---------------|-----------|
| Incremental-only refresh | Local full still within threshold; probed ~20 RPCs, picked ~409 MB/s peer, downloaded ~42 MB incremental | ~15 s end-to-end |
| Cold start (empty ledger) | Safety incremental → ~50 GB full at ~451 MB/s → fresher incremental | ~2 min (full download ~1m46s) |
| Restart with fresh locals | Diffs within thresholds → exit without probe/download | &lt; 1 s |

Notes from those runs:

- Warm-start reused prior `nodes_attempt_*.json` (11 cached good/slow peers)
- Evaluation stayed single-pass (no re-probe on relaxation)
- Most RPC hosts returned `200 OK` to Range speed tests, so the client fell back to plain GET as designed

## Usage

When integrated with Solana validator startup, the tool will:

1. Check if current snapshots are up-to-date
2. Find and evaluate the best RPC nodes for snapshot downloads
3. Efficiently download only the necessary snapshots
4. Clean up outdated snapshots to manage disk space

## Implementation Details

- Uses standard Go HTTP client with proper timeout and redirect handling
- Implements progress tracking for large downloads with estimated time remaining
- Employs regex pattern matching to extract slot information from filenames
- Maintains backup files to prevent data loss during download operations
- Saves evaluation results for each relaxation attempt to `nodes_attempt_N.json` files

### Slot Validation Logic

- **Node Evaluation**: Always uses `full_threshold` to find nodes capable of providing recent full snapshots (no relaxation)
- **Local File Check**: Uses `incremental_threshold` only when checking existing local snapshots
- **HEAD vs RPC**: When Phase A HEAD exposes a full snapshot filename, that slot wins at selection time without an RPC cross-check; post-download filename checks remain the correctness backstop

## Best Practices

- Adjust thresholds based on your network's slot advancement rate and restart frequency
- Lower `speed_test_max_bytes` or `speed_test_candidates` if probe bandwidth is still too high
- Use a dedicated disk with sufficient space for snapshots (95GB+ for full snapshots)

This tool significantly improves Solana validator startup times by intelligently managing snapshots and preventing redundant downloads of large files.

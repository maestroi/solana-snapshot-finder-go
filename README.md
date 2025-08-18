# Solana Snapshot Finder Go

## Overview

Solana Snapshot Finder is a Go utility designed to efficiently manage Solana blockchain snapshots. It automates the process of finding, downloading, and maintaining up-to-date snapshots for Solana validators, helping to reduce node restart times and ensure reliable operation.

## Key Features

- **Smart Snapshot Detection**: Identifies when new snapshots are needed based on configurable thresholds
- **Redundant Download Prevention**: Compares remote snapshot slots with local snapshots to avoid unnecessary downloads
- **RPC Node Selection**: Evaluates and selects the best RPC nodes for downloads based on latency and download speed
- **Automated Cleanup**: Removes outdated snapshots to save disk space while maintaining necessary backups
- **Full & Incremental Support**: Handles both full and incremental snapshots with proper slot validation
- **Adaptive Retry Logic**: Gradually relaxes speed and latency requirements on retry attempts to ensure snapshot acquisition

## Configuration

The tool uses a YAML configuration file with the following parameters:

```yaml
rpc_address: "https://api.mainnet-beta.solana.com"  # Default RPC if no better nodes found
snapshot_path: "./snapshots"                        # Base directory to store snapshots
min_download_speed: 100                             # Minimum acceptable download speed (MB/s)
max_latency: 200                                    # Maximum acceptable latency (ms)
num_of_retries: 3                                   # Number of download retry attempts
sleep_before_retry: 5                               # Time between retries (seconds)
denylist: []                                       # RPC nodes to exclude
private_rpc: false                                  # Whether to use private RPCs
worker_count: 100                                   # Number of concurrent evaluation workers
full_threshold: 25000                               # Slots threshold for full snapshot updates
incremental_threshold: 1000                         # Slots threshold for incremental updates
# Retry relaxation parameters
speed_relaxation_factor: 0.9                        # Factor to reduce speed requirements on retries
latency_relaxation_factor: 0.9                      # Factor to increase latency tolerance on retries
max_relaxation_attempts: 3                          # Maximum number of retry attempts with relaxed requirements
```

### Retry Relaxation Example

With the default settings above, the relaxation works as follows:

**With `speed_relaxation_factor: 0.8` and `latency_relaxation_factor: 0.8`:**
- **Attempt 1**: Speed ≥ 100 MB/s, Latency ≤ 200ms (original requirements)
- **Attempt 2**: Speed ≥ 80 MB/s, Latency ≤ 250ms (relaxed: 100×0.8, 200/0.8)
- **Attempt 3**: Speed ≥ 64 MB/s, Latency ≤ 313ms (relaxed: 100×0.8×0.8, 200/(0.8×0.8))
- **Attempt 4**: Speed ≥ 51 MB/s, Latency ≤ 391ms (relaxed: 100×0.8×0.8×0.8, 200/(0.8×0.8×0.8))

*Note: The relaxation factors make requirements more permissive - lower speed thresholds and higher latency tolerance.*

## How It Works

1. **Snapshot Evaluation**:
   - Checks if existing snapshots are outdated by comparing slot differences
   - Full snapshots are updated if their slot differs from the reference by more than `full_threshold`
   - Incremental snapshots are updated if they differ by more than `incremental_threshold`

2. **Download Optimization**:
   - Makes a HEAD request to check remote snapshot details before downloading
   - Extracts the filename and slot information from response headers
   - Skips downloads if the remote snapshot is not newer than the local snapshot
   - Downloads to a temporary file first, then copies to final location
   - Maintains a backup during the process for safety

3. **RPC Selection**:
   - Evaluates multiple RPC nodes for performance metrics
   - Selects the fastest and most reliable RPC node for downloads
   - Supports denylisting of problematic nodes
   - **Smart Slot Validation**: Prevents downloading from extremely old nodes that could cause sync issues

5. **Adaptive Retry with Relaxed Requirements**:
   - If no suitable RPC nodes meet initial speed/latency requirements, automatically retries with relaxed criteria
   - Gradually reduces minimum download speed requirements on each retry attempt (more permissive)
   - Progressively increases maximum latency tolerance to find acceptable nodes (more permissive)
   - Ensures snapshot acquisition even under suboptimal network conditions
   - Configurable relaxation factors allow fine-tuning of retry behavior

6. **Snapshot Management**:
   - Stores snapshots in organized directories (`snapshot_path` and `snapshot_path/remote`)
   - Maintains temporary and backup files for resilience
   - Cleans up old snapshots based on configurable thresholds

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
- Performs verification at multiple stages to ensure snapshot integrity
- **Retry Relaxation Algorithm**: 
  - On each retry attempt, reduces speed requirements by dividing by `speed_relaxation_factor * attempt_number`
  - Increases latency tolerance by multiplying by `latency_relaxation_factor * attempt_number`
  - **Relaxes slot thresholds** to allow older but still usable nodes on retry attempts
  - Saves evaluation results for each attempt to `nodes_attempt_N.json` files
  - Continues until suitable nodes are found or maximum attempts are reached

- **Slot Validation Logic**:
  - **Node Evaluation**: Always uses `full_threshold` (default: 25,000 slots) to find nodes capable of providing recent full snapshots
  - **Local File Check**: Uses `incremental_threshold` (default: 500 slots) only when checking existing local snapshots
  - **Smart Workflow**: First finds nodes with good full snapshots, then determines if incremental is needed based on local state
  - **Strict requirement**: Slot difference must be within full threshold during node evaluation (no relaxation)
  - Prioritizes nodes with closest slot to reference for most recent snapshots
  - Logs slot validation decisions for transparency and debugging

## Best Practices

- Adjust thresholds based on your network's slot advancement rate and restart frequency
- Use a dedicated disk with sufficient space for snapshots (95GB+ for full snapshots)

This tool significantly improves Solana validator startup times by intelligently managing snapshots and preventing redundant downloads of large files. 
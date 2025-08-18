package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
	"github.com/maestroi/solana-snapshot-finder-go/pkg/rpc"
	"github.com/maestroi/solana-snapshot-finder-go/pkg/snapshot"
)

// cleanupFailedDownloads removes failed downloads and temporary files to enable clean retries
func cleanupFailedDownloads(snapshotPath string) {
	log.Println("Cleaning up failed downloads...")

	// Clean up tmp directory
	tmpDir := filepath.Join(snapshotPath, "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		log.Printf("Warning: Failed to clean tmp directory: %v", err)
	}

	// Clean up partial downloads in remote directory
	remoteDir := filepath.Join(snapshotPath, "remote")
	if err := filepath.Walk(remoteDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip files we can't access
		}

		// Remove partial downloads (files that might be corrupted)
		if !info.IsDir() {
			fileName := info.Name()
			if strings.HasPrefix(fileName, "tmp-") || strings.HasPrefix(fileName, "backup-") {
				if err := os.Remove(path); err != nil {
					log.Printf("Warning: Failed to remove partial file %s: %v", fileName, err)
				} else {
					log.Printf("Removed partial file: %s", fileName)
				}
			}
		}
		return nil
	}); err != nil {
		log.Printf("Warning: Failed to walk remote directory: %v", err)
	}

	// Recreate tmp directory
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		log.Printf("Warning: Failed to recreate tmp directory: %v", err)
	}

	log.Println("Cleanup completed, ready for retry")
}

// processSnapshots manages the snapshot download process
func processSnapshots(cfg config.Config) {
	// Create snapshot directory if it doesn't exist
	if err := os.MkdirAll(cfg.SnapshotPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create snapshot directory: %v", err)
	}

	// Create tmp directory if it doesn't exist
	tmpDir := filepath.Join(cfg.SnapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create TMP directory: %v", err)
	}

	// Create remote directory if it doesn't exist
	remoteDir := filepath.Join(cfg.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, os.ModePerm); err != nil {
		log.Fatalf("Failed to create remote directory: %v", err)
	}

	// Get reference slot from RPC
	referenceSlot, err := rpc.GetReferenceSlot(cfg.RPCAddress)
	if err != nil {
		log.Fatalf("Failed to get reference slot: %v", err)
	}
	log.Printf("Reference slot: %d", referenceSlot)

	// Check if we need new snapshots
	needFull, needIncremental := snapshot.ManageSnapshots(cfg, referenceSlot)

	// If no snapshots are needed and genesis is present, exit successfully
	if !needFull && !needIncremental && snapshot.IsGenesisPresent(cfg.SnapshotPath) {
		log.Println("All snapshots are up to date. Exiting...")
		return
	}

	// Fetch RPC nodes if needed
	nodes := rpc.FetchRPCNodes(cfg)
	if len(nodes) == 0 {
		log.Fatal("No RPC nodes available.")
	}

	// Evaluate nodes with retry logic and relaxed requirements
	var results []rpc.NodeEvaluationResult
	var bestRPC string
	var outputFile string

	// Try to find suitable nodes with gradually relaxed requirements
	for attempt := 1; attempt <= cfg.MaxRelaxationAttempts; attempt++ {
		log.Printf("Evaluating nodes - Attempt %d/%d", attempt, cfg.MaxRelaxationAttempts)

		if attempt == 1 {
			// First attempt with original requirements
			results = rpc.EvaluateNodesWithVersions(nodes, cfg, referenceSlot)
		} else {
			// Subsequent attempts with relaxed requirements
			results = rpc.EvaluateNodesWithRelaxedRequirements(nodes, cfg, referenceSlot, attempt)
		}

		rpc.SummarizeResultsWithVersions(results)

		// Save good and slow nodes to file
		outputFile = filepath.Join(cfg.SnapshotPath, fmt.Sprintf("nodes_attempt_%d.json", attempt))
		rpc.DumpGoodAndSlowNodesToFile(results, outputFile)

		// Try to select best RPC
		bestRPC = rpc.SelectBestRPC(results)
		if bestRPC != "" {
			log.Printf("Found suitable RPC on attempt %d: %s", attempt, bestRPC)
			break
		}

		if attempt < cfg.MaxRelaxationAttempts {
			log.Printf("No suitable RPC found on attempt %d. Relaxing requirements and retrying...", attempt)
			time.Sleep(time.Duration(cfg.SleepBeforeRetry) * time.Second)
		}
	}

	if bestRPC == "" {
		// After exhausting all relaxation attempts, fall back to the best available node
		log.Printf("No good nodes found after %d attempts. Falling back to best available node...", cfg.MaxRelaxationAttempts)

		// Use the results from the last attempt to find the best available node
		var bestAvailableNode struct {
			rpc   string
			speed float64
		}

		for _, result := range results {
			// Consider both good and slow nodes, prioritize by speed
			if (result.Status == "good" || result.Status == "slow") && result.Speed > bestAvailableNode.speed {
				bestAvailableNode = struct {
					rpc   string
					speed float64
				}{rpc: result.RPC, speed: result.Speed}
			}
		}

		if bestAvailableNode.rpc != "" {
			bestRPC = bestAvailableNode.rpc
			log.Printf("Selected best available node: %s with speed %.2f MB/s", bestRPC, bestAvailableNode.speed)
		} else {
			log.Fatal("No suitable RPC found even with relaxed requirements after all attempts.")
		}
	}

	log.Printf("Selected RPC: %s", bestRPC)

	// Download snapshots with retry logic
	maxDownloadRetries := cfg.MaxDownloadRetries
	for downloadAttempt := 1; downloadAttempt <= maxDownloadRetries; downloadAttempt++ {
		log.Printf("Download attempt %d/%d", downloadAttempt, maxDownloadRetries)

		// Download genesis snapshot if not present
		if !snapshot.IsGenesisPresent(cfg.SnapshotPath) {
			log.Println("Genesis snapshot not found. Downloading from fastest node...")
			if err := snapshot.DownloadGenesis(bestRPC, cfg.SnapshotPath); err != nil {
				log.Printf("Failed to download genesis snapshot on attempt %d: %v", downloadAttempt, err)
				if downloadAttempt < maxDownloadRetries {
					log.Println("Cleaning up failed download and retrying...")
					cleanupFailedDownloads(cfg.SnapshotPath)
					continue
				} else {
					log.Fatalf("Failed to download genesis snapshot after %d attempts: %v", maxDownloadRetries, err)
				}
			}
			log.Println("Genesis snapshot downloaded successfully")

			// Untar the genesis file
			if err := snapshot.UntarGenesis(cfg.SnapshotPath); err != nil {
				log.Printf("Failed to untar genesis snapshot on attempt %d: %v", downloadAttempt, err)
				if downloadAttempt < maxDownloadRetries {
					log.Println("Cleaning up failed download and retrying...")
					cleanupFailedDownloads(cfg.SnapshotPath)
					continue
				} else {
					log.Fatalf("Failed to untar genesis snapshot after %d attempts: %v", maxDownloadRetries, err)
				}
			}
			log.Println("Genesis snapshot untarred successfully")
		}

		// Download full snapshot if needed
		if needFull {
			log.Println("Downloading full snapshot...")
			if err := snapshot.DownloadSnapshot(bestRPC, cfg, "snapshot-", referenceSlot); err != nil {
				log.Printf("Failed to download full snapshot on attempt %d: %v", downloadAttempt, err)
				if downloadAttempt < maxDownloadRetries {
					log.Println("Cleaning up failed download and retrying...")
					cleanupFailedDownloads(cfg.SnapshotPath)
					continue
				} else {
					log.Fatalf("Failed to download full snapshot after %d attempts: %v", maxDownloadRetries, err)
				}
			}
			log.Println("Full snapshot downloaded successfully")
		}

		// Download incremental snapshot if needed
		if needIncremental {
			log.Println("Downloading incremental snapshot...")
			if err := snapshot.DownloadSnapshot(bestRPC, cfg, "incremental-", referenceSlot); err != nil {
				log.Printf("Failed to download incremental snapshot on attempt %d: %v", downloadAttempt, err)
				if downloadAttempt < maxDownloadRetries {
					log.Println("Cleaning up failed download and retrying...")
					cleanupFailedDownloads(cfg.SnapshotPath)
					continue
				} else {
					log.Fatalf("Failed to download incremental snapshot after %d attempts: %v", maxDownloadRetries, err)
				}
			}
			log.Println("Incremental snapshot downloaded successfully")
		}

		// If we get here, all downloads succeeded
		log.Printf("All downloads completed successfully on attempt %d", downloadAttempt)
		break
	}

	// Clean up old snapshots
	if err := snapshot.CleanupOldSnapshots(cfg.SnapshotPath, referenceSlot, cfg.FullThreshold, cfg.IncrementalThreshold); err != nil {
		log.Printf("Failed to clean up old snapshots: %v", err)
	}

	log.Println("All snapshots are up to date. Exiting...")
}

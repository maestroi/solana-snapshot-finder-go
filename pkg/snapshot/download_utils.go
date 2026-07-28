package snapshot

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

type ProgressWriter struct {
	TotalBytes   int64
	Downloaded   int64
	LastLoggedAt time.Time
	StartTime    time.Time
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n := len(p)
	atomic.AddInt64(&pw.Downloaded, int64(n))

	now := time.Now()
	elapsed := now.Sub(pw.StartTime).Seconds()

	// Log progress every 3 seconds
	if now.Sub(pw.LastLoggedAt) >= 3*time.Second {
		pw.LastLoggedAt = now
		speed := float64(pw.Downloaded) / 1024 / 1024 / elapsed // MB/s
		percentage := (float64(pw.Downloaded) / float64(pw.TotalBytes)) * 100

		// Calculate estimated time remaining
		remainingBytes := pw.TotalBytes - pw.Downloaded
		remainingSeconds := float64(remainingBytes) / (float64(pw.Downloaded) / elapsed)
		var timeLeftStr string
		if remainingSeconds < 60 {
			timeLeftStr = fmt.Sprintf("%.0f seconds", remainingSeconds)
		} else {
			minutes := int(remainingSeconds / 60)
			seconds := int(remainingSeconds) % 60
			timeLeftStr = fmt.Sprintf("%d minutes, %d seconds", minutes, seconds)
		}

		log.Printf("Download progress: %.2f%% (%.2f MB/s) - Downloaded: %d bytes of %d bytes - Time remaining: %s",
			percentage, speed, pw.Downloaded, pw.TotalBytes, timeLeftStr)
	}
	return n, nil
}

func DownloadGenesis(rpcAddress, snapshotPath string) error {
	tmpDir := filepath.Join(snapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create TMP directory: %v", err)
	}

	genesisURL := fmt.Sprintf("%s/genesis.tar.bz2", rpcAddress)
	_, _, err := writeSnapshotToFile(genesisURL, tmpDir, snapshotPath, true)
	if err != nil {
		return fmt.Errorf("failed to download genesis snapshot: %v", err)
	}

	return nil
}

// copyFile is the fallback path for the rare case tmp and dst live on different
// filesystems (os.Rename returns EXDEV), so it can't just be a rename.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open temp file for copying: %v", err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %v", err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("failed to copy file to final location: %v", err)
	}
	return dstFile.Sync()
}

func writeSnapshotToFile(snapshotURL, tmpDir, baseDir string, genesis bool) (string, int64, error) {
	// Ensure the temporary directory exists
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create temporary directory %s: %v", tmpDir, err)
	}
	log.Printf("Created/verified tmp directory: %s", tmpDir)

	// Initiate the download
	resp, err := http.Get(snapshotURL)
	if err != nil {
		return "", 0, fmt.Errorf("failed to fetch snapshot: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("snapshot request returned status %d for %s", resp.StatusCode, snapshotURL)
	}

	// Get the final URL after redirects
	finalURL := resp.Request.URL.String()
	fileName := filepath.Base(finalURL)
	log.Printf("Original filename from URL: %s", fileName)

	if fileName == "" {
		return "", 0, fmt.Errorf("invalid file name parsed from URL: %s", finalURL)
	}

	// Extract the slot and hash from the response headers or URL
	if contentDisposition := resp.Header.Get("Content-Disposition"); contentDisposition != "" {
		if strings.Contains(contentDisposition, "filename=") {
			fileName = strings.Split(contentDisposition, "filename=")[1]
			fileName = strings.Trim(fileName, `"'`)
			log.Printf("Filename from Content-Disposition: %s", fileName)
		}
	}

	// Define paths for temporary and final files
	tmpFilePath := filepath.Join(tmpDir, "tmp-"+fileName)
	log.Printf("Temporary file path: %s", tmpFilePath)

	var finalFilePath string
	if genesis {
		finalFilePath = filepath.Join(baseDir, fileName)
	} else {
		remoteFilePath := filepath.Join(baseDir, "remote")
		if err := os.MkdirAll(remoteFilePath, 0755); err != nil {
			return "", 0, fmt.Errorf("failed to create remote directory %s: %v", remoteFilePath, err)
		}
		log.Printf("Created/verified remote directory: %s", remoteFilePath)
		finalFilePath = filepath.Join(remoteFilePath, fileName)
	}
	log.Printf("Final file path: %s", finalFilePath)

	// Create a temporary file for download
	tmpFile, err := os.Create(tmpFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create temporary file %s: %v", tmpFilePath, err)
	}
	log.Printf("Created temporary file: %s", tmpFilePath)

	// Track download progress
	pw := &ProgressWriter{
		TotalBytes:   resp.ContentLength,
		StartTime:    time.Now(),
		LastLoggedAt: time.Now(),
	}

	// Download to temporary file
	totalBytes, err := io.Copy(io.MultiWriter(tmpFile, pw), resp.Body)
	if err != nil {
		log.Printf("Error during download: %v", err)
		return "", 0, fmt.Errorf("error during snapshot download: %v", err)
	}
	tmpFile.Close()
	log.Printf("Download completed to temporary file. Size: %d bytes", totalBytes)

	// ponytail: 1MB floor — any real Solana snapshot is hundreds of MB
	const minSnapshotBytes = 1 << 20
	if totalBytes < minSnapshotBytes {
		os.Remove(tmpFilePath)
		return "", 0, fmt.Errorf("downloaded file is too small (%d bytes), likely an error response", totalBytes)
	}

	// Move into place. Rename is instant (same filesystem, both under snapshotPath) -
	// avoids reading and writing the whole multi-GB file a second time.
	if err := os.Rename(tmpFilePath, finalFilePath); err != nil {
		log.Printf("Rename failed (%v), falling back to copy", err)
		if err := copyFile(tmpFilePath, finalFilePath); err != nil {
			return "", 0, err
		}
		os.Remove(tmpFilePath)
	}
	log.Printf("Moved to final location: %s", finalFilePath)

	// Verify the final file exists
	if _, err := os.Stat(finalFilePath); err != nil {
		return "", 0, fmt.Errorf("final file does not exist after copy: %v", err)
	}
	log.Printf("Verified final file exists: %s", finalFilePath)

	// Print final progress
	speed := float64(totalBytes) / 1024 / 1024 / time.Since(pw.StartTime).Seconds()
	log.Printf("Download completed: %s | Speed: %.2f MB/s", finalFilePath, speed)
	return finalFilePath, totalBytes, nil
}

func DownloadSnapshot(rpcAddress string, cfg config.Config, snapshotType string, referenceSlot int) error {
	log.Printf("Downloading %s snapshot from %s", snapshotType, rpcAddress)

	start := time.Now()

	// Define the TMP directory
	tmpDir := filepath.Join(cfg.SnapshotPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return fmt.Errorf("failed to create TMP directory: %v", err)
	}

	// Ensure remote directory exists with proper permissions
	remoteDir := filepath.Join(cfg.SnapshotPath, "remote")
	if err := os.MkdirAll(remoteDir, 0755); err != nil {
		return fmt.Errorf("failed to create remote directory: %v", err)
	}
	log.Printf("Ensured remote directory exists: %s", remoteDir)

	// Get existing snapshot slot if it exists
	var existingSlot int
	var existingSnapshotPath string
	if strings.HasPrefix(snapshotType, "snapshot-") {
		var err error
		existingSnapshotPath, existingSlot, err = findRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
		if err == nil {
			log.Printf("Found existing full snapshot: %s (Slot: %d)", existingSnapshotPath, existingSlot)
		}
	}

	// Try both .tar.bz2 and .tar.zst extensions
	var finalPath string
	var sizeBytes int64
	downloadErr := fmt.Errorf("no extension returned HTTP 200 from %s", rpcAddress)

	// NOTE: no HEAD pre-check here. The node was already probed (health, snapshot
	// availability, speed) during evaluation, so an extra HEAD right before the
	// real download just doubles our request rate against the same IP and risks
	// tripping the node's own rate limiter (429) right when we need it most.
	extensions := []string{".tar.bz2", ".tar.zst"}
	for _, ext := range extensions {
		var snapshotURL string
		if strings.HasPrefix(snapshotType, "incremental") {
			snapshotURL = fmt.Sprintf("%s/incremental-snapshot%s", rpcAddress, ext)
		} else {
			snapshotURL = fmt.Sprintf("%s/snapshot%s", rpcAddress, ext)
		}
		log.Printf("Trying URL: %s", snapshotURL)

		finalPath, sizeBytes, downloadErr = writeSnapshotToFile(snapshotURL, tmpDir, cfg.SnapshotPath, false)
		if downloadErr != nil {
			log.Printf("Failed to download with %s extension: %v", ext, downloadErr)
			continue
		}

		// For full snapshots, only now do we know the real slot (from the downloaded
		// file's name) - if it's not newer than what we already have, discard it.
		if strings.HasPrefix(snapshotType, "snapshot-") {
			remoteSlot, err := ExtractFullSnapshotSlot(filepath.Base(finalPath))
			if err != nil {
				log.Printf("Warning: downloaded file doesn't match full snapshot naming: %v", err)
				os.Remove(finalPath)
				downloadErr = err
				continue
			}
			if existingSlot > 0 && remoteSlot <= existingSlot {
				log.Printf("Remote snapshot (slot %d) is not newer than existing snapshot (slot %d). Discarding.",
					remoteSlot, existingSlot)
				os.Remove(finalPath)
				return nil
			}
		}

		log.Printf("Successfully downloaded snapshot to: %s", finalPath)
		break
	}

	if downloadErr != nil {
		return fmt.Errorf("failed to download snapshot with any extension: %v", downloadErr)
	}

	// Verify the file exists in the remote directory
	if _, err := os.Stat(finalPath); err != nil {
		return fmt.Errorf("snapshot file not found after download: %v", err)
	}
	log.Printf("Verified snapshot exists at: %s", finalPath)

	// Verify the downloaded snapshot without removing files
	if strings.HasPrefix(snapshotType, "snapshot-") {
		downloadedSlot, err := ExtractFullSnapshotSlot(finalPath)
		if err != nil {
			log.Printf("Warning: Failed to extract slot from downloaded snapshot: %v", err)
			return nil
		}

		// Double-check that downloaded slot matches what we expected
		if existingSlot > 0 && downloadedSlot <= existingSlot {
			log.Printf("Warning: Downloaded snapshot (slot %d) is not newer than existing snapshot (slot %d). Removing redundant download.",
				downloadedSlot, existingSlot)
			// Remove the downloaded file since it's not newer
			if err := os.Remove(finalPath); err != nil {
				log.Printf("Warning: Failed to remove redundant downloaded snapshot: %v", err)
			}
			return nil
		}

		if downloadedSlot < referenceSlot-cfg.FullThreshold {
			log.Printf("Warning: Downloaded snapshot might be old, but keeping it anyway")
		}
	} else {
		slotStart, slotEnd, err := ExtractIncrementalSnapshotSlots(finalPath)
		if err != nil {
			log.Printf("Warning: Failed to extract slots from incremental snapshot: %v", err)
			return nil
		}

		if referenceSlot-slotEnd > cfg.IncrementalThreshold {
			log.Printf("Warning: Incremental snapshot might be old, but keeping it anyway")
		}

		_, fullSlot, err := findRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
		if err != nil {
			log.Printf("Warning: Could not find full snapshot: %v", err)
			return nil
		}

		if slotStart != fullSlot {
			log.Printf("Warning: Incremental snapshot might not match full snapshot, but keeping it anyway")
		}
	}

	duration := time.Since(start)
	seconds := int(duration.Seconds())
	if seconds < 60 {
		log.Printf("%s snapshot saved to: %s | Time: %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds, float64(sizeBytes)/(1024*1024))
	} else {
		log.Printf("%s snapshot saved to: %s | Time: %d minutes, %d seconds | Size: %.2f MB",
			snapshotType, finalPath, seconds/60, seconds%60, float64(sizeBytes)/(1024*1024))
	}

	return nil
}

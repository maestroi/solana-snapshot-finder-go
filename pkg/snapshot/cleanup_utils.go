package snapshot

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func CleanupOldSnapshots(snapshotPath string, referenceSlot, fullThreshold, incrementalThreshold int) error {
	log.Println("Cleaning up old snapshots...")

	directories := []string{snapshotPath, filepath.Join(snapshotPath, "remote")}
	gracePeriod := time.Hour

	for _, dir := range directories {
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to read directory %s: %v", dir, err)
		}

		var removedCount int
		var totalSize int64

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			filePath := filepath.Join(dir, fileName)

			fileInfo, err := os.Stat(filePath)
			if err != nil {
				log.Printf("Failed to get file info for %s: %v", fileName, err)
				continue
			}

			if time.Since(fileInfo.ModTime()) < gracePeriod {
				log.Printf("Skipping recent file (within grace period): %s", fileName)
				continue
			}

			if strings.HasPrefix(fileName, "snapshot-") && (strings.HasSuffix(fileName, ".tar.zst") || strings.HasSuffix(fileName, ".tar.bz2")) {
				slot, err := ExtractFullSnapshotSlot(fileName)
				if err != nil {
					continue
				}

				diff := referenceSlot - slot
				conservativeThreshold := int(float64(fullThreshold) * 1.5)
				if diff > conservativeThreshold {
					size := fileInfo.Size()
					if err := os.Remove(filePath); err != nil {
						log.Printf("Failed to remove old full snapshot %s: %v", fileName, err)
						continue
					}
					removedCount++
					totalSize += size
					log.Printf("Removed old full snapshot: %s (Slot: %d, Diff: %d, Size: %.2f MB)",
						fileName, slot, diff, float64(size)/(1024*1024))
				}
			}

			if strings.HasPrefix(fileName, "incremental-snapshot-") && (strings.HasSuffix(fileName, ".tar.zst") || strings.HasSuffix(fileName, ".tar.bz2")) {
				_, slotEnd, err := ExtractIncrementalSnapshotSlots(fileName)
				if err != nil {
					continue
				}

				diff := referenceSlot - slotEnd
				conservativeThreshold := int(float64(incrementalThreshold) * 1.5)
				if diff > conservativeThreshold {
					size := fileInfo.Size()
					if err := os.Remove(filePath); err != nil {
						log.Printf("Failed to remove old incremental snapshot %s: %v", fileName, err)
						continue
					}
					removedCount++
					totalSize += size
					log.Printf("Removed old incremental snapshot: %s (SlotEnd: %d, Diff: %d, Size: %.2f MB)",
						fileName, slotEnd, diff, float64(size)/(1024*1024))
				}
			}
		}

		if removedCount > 0 {
			log.Printf("Cleaned up %d old snapshots in %s (Total size freed: %.2f MB)",
				removedCount, dir, float64(totalSize)/(1024*1024))
		} else {
			log.Printf("No old snapshots to clean up in %s", dir)
		}
	}

	return nil
}


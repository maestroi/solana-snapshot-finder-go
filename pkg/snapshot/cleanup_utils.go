package snapshot

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type fullSnapshotInfo struct {
	FileName string
	Slot     int
	Size     int64
}

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

func findAllFullSnapshots(snapshotPath string) []fullSnapshotInfo {
	var snapshots []fullSnapshotInfo

	files, err := os.ReadDir(snapshotPath)
	if err != nil {
		return snapshots
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		if !strings.HasPrefix(fileName, "snapshot-") {
			continue
		}
		if !strings.HasSuffix(fileName, ".tar.zst") && !strings.HasSuffix(fileName, ".tar.bz2") {
			continue
		}
		slot, err := ExtractFullSnapshotSlot(fileName)
		if err != nil {
			continue
		}
		info, err := file.Info()
		if err != nil {
			continue
		}
		snapshots = append(snapshots, fullSnapshotInfo{
			FileName: fileName,
			Slot:     slot,
			Size:     info.Size(),
		})
	}
	return snapshots
}

func deleteIncrementalsForBase(snapshotPath string, baseSlot int) {
	pattern := fmt.Sprintf("incremental-snapshot-%d-*.tar.*", baseSlot)
	matches, err := filepath.Glob(filepath.Join(snapshotPath, pattern))
	if err != nil {
		return
	}
	for _, match := range matches {
		if err := os.Remove(match); err == nil {
			log.Printf("Deleted associated incremental: %s", filepath.Base(match))
		}
	}
}

// EnforceRetentionLimit keeps only the N most recent full snapshots in the main
// snapshot directory (not remote/). Oldest snapshots and their incrementals are deleted.
func EnforceRetentionLimit(snapshotPath string, maxSnapshots int) {
	if maxSnapshots <= 0 {
		return
	}

	fullSnapshots := findAllFullSnapshots(snapshotPath)
	if len(fullSnapshots) == 0 {
		return
	}

	sort.Slice(fullSnapshots, func(i, j int) bool {
		return fullSnapshots[i].Slot > fullSnapshots[j].Slot
	})

	if len(fullSnapshots) <= maxSnapshots {
		log.Printf("Retention limit: %d/%d full snapshots, no cleanup needed", len(fullSnapshots), maxSnapshots)
		return
	}

	toDelete := fullSnapshots[maxSnapshots:]
	var freedSpace int64
	for _, snap := range toDelete {
		log.Printf("Retention limit exceeded: removing old snapshot %s (slot %d)", snap.FileName, snap.Slot)
		fullPath := filepath.Join(snapshotPath, snap.FileName)
		if err := os.Remove(fullPath); err != nil {
			log.Printf("Failed to delete %s: %v", snap.FileName, err)
			continue
		}
		freedSpace += snap.Size
		deleteIncrementalsForBase(snapshotPath, snap.Slot)
	}

	log.Printf("Retention cleanup: removed %d full snapshots, freed %.2f GB",
		len(toDelete), float64(freedSpace)/(1024*1024*1024))
}


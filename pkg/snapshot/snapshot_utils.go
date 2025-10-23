package snapshot

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

// IncrementalSnapshot represents an incremental snapshot with its slots.
type IncrementalSnapshot struct {
	FileName  string
	SlotStart int
	SlotEnd   int
}

// ExtractFullSnapshotSlot parses a full snapshot file name to extract the slot.
func ExtractFullSnapshotSlot(fileName string) (int, error) {
	fullSnapshotRegex := `snapshot-(\d+)-[a-zA-Z0-9]+\.tar\.(zst|bz2)`
	match := regexp.MustCompile(fullSnapshotRegex).FindStringSubmatch(fileName)
	if match == nil {
		return 0, fmt.Errorf("file name does not match full snapshot pattern: %s", fileName)
	}
	slot, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, fmt.Errorf("failed to parse slot from full snapshot: %v", err)
	}
	return slot, nil
}

func ExtractIncrementalSnapshotSlots(fileName string) (int, int, error) {
	regex := regexp.MustCompile(`incremental-snapshot-(\d+)-(\d+)-[a-zA-Z0-9]+\.tar\.(zst|bz2)`)
	match := regex.FindStringSubmatch(fileName)

	if len(match) != 4 {
		return 0, 0, fmt.Errorf("invalid incremental snapshot file name: %s", fileName)
	}

	slotStart, err1 := strconv.Atoi(match[1])
	slotEnd, err2 := strconv.Atoi(match[2])
	if err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("failed to parse slots from file name: %s", fileName)
	}

	return slotStart, slotEnd, nil
}

func findRecentFullSnapshot(destDir string, referenceSlot, threshold int) (string, int, error) {
	var mostRecent string
	var mostRecentSlot int

	scanDir := func(dir string) error {
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		for _, file := range files {
			if file.IsDir() {
				continue
			}

			fileName := file.Name()
			if strings.HasPrefix(fileName, "snapshot-") && (strings.HasSuffix(fileName, ".tar.zst") || strings.HasSuffix(fileName, ".tar.bz2")) {
				slot, err := ExtractFullSnapshotSlot(fileName)
				if err != nil {
					log.Printf("Skipping file %s: %v", fileName, err)
					continue
				}

				if slot > mostRecentSlot {
					mostRecent = filepath.Join(dir, fileName)
					mostRecentSlot = slot
				}
			}
		}
		return nil
	}

	if err := scanDir(destDir); err != nil {
		return "", 0, err
	}
	if err := scanDir(filepath.Join(destDir, "remote")); err != nil {
		return "", 0, err
	}

	if mostRecent == "" {
		return "", 0, fmt.Errorf("no full snapshots found")
	}

	log.Printf("Found most recent full snapshot: %s (Slot: %d)", mostRecent, mostRecentSlot)
	return mostRecent, mostRecentSlot, nil
}

func findHighestIncrementalSnapshot(destDir string, baseSlot int) (IncrementalSnapshot, error) {
	var allSnapshots []string

	// Helper function to collect snapshots from a directory
	collectSnapshots := func(dir string) error {
		files, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		for _, file := range files {
			if !file.IsDir() && strings.HasPrefix(file.Name(), "incremental-snapshot-") {
				allSnapshots = append(allSnapshots, filepath.Join(dir, file.Name()))
			}
		}
		return nil
	}

	// Collect snapshots from both directories
	if err := collectSnapshots(destDir); err != nil {
		return IncrementalSnapshot{}, err
	}
	if err := collectSnapshots(filepath.Join(destDir, "remote")); err != nil {
		return IncrementalSnapshot{}, err
	}

	var highestSnapshot IncrementalSnapshot
	var highestSlotEnd int

	for _, filePath := range allSnapshots {
		startSlot, endSlot, err := ExtractIncrementalSnapshotSlots(filepath.Base(filePath))
		if err != nil {
			continue
		}

		if startSlot != baseSlot {
			continue
		}

		if endSlot > highestSlotEnd {
			highestSlotEnd = endSlot
			highestSnapshot = IncrementalSnapshot{
				FileName:  filePath,
				SlotStart: startSlot,
				SlotEnd:   endSlot,
			}
		}
	}

	if highestSlotEnd == 0 {
		return IncrementalSnapshot{}, fmt.Errorf("no valid incremental snapshots found starting at slot %d", baseSlot)
	}

	return highestSnapshot, nil
}

func IsGenesisPresent(snapshotPath string) bool {
	genesisPath := filepath.Join(snapshotPath, "genesis.tar.bz2")
	if _, err := os.Stat(genesisPath); err == nil {
		return true
	}
	return false
}

// Helper function to copy a file
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer sourceFile.Close()

	destFile, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %v", err)
	}
	defer destFile.Close()

	if _, err = io.Copy(destFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy file contents: %v", err)
	}

	if err = destFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync destination file: %v", err)
	}

	return nil
}

func ManageSnapshots(cfg config.Config, referenceSlot int) (bool, bool) {
	log.Println("Checking snapshots...")

	fullSnapshot, fullSlot, err := findRecentFullSnapshot(cfg.SnapshotPath, referenceSlot, 0)
	if err != nil {
		log.Printf("No full snapshots found: %v", err)
		return true, true
	}

	fullDiff := referenceSlot - fullSlot
	log.Printf("Full snapshot found: %s (Slot: %d, Diff: %d, Threshold: %d)",
		fullSnapshot, fullSlot, fullDiff, cfg.FullThreshold)

	if fullDiff > cfg.FullThreshold {
		log.Printf("Full snapshot is outdated. Diff (%d) exceeds threshold (%d)", fullDiff, cfg.FullThreshold)
		return true, true
	}

	incrementalSnapshot, err := findHighestIncrementalSnapshot(cfg.SnapshotPath, fullSlot)
	if err != nil {
		log.Printf("No valid incremental snapshots found: %v", err)
		return false, true
	}

	incrementalDiff := referenceSlot - incrementalSnapshot.SlotEnd
	log.Printf("Incremental snapshot found: %s (SlotStart: %d, SlotEnd: %d, Diff: %d, Threshold: %d)",
		incrementalSnapshot.FileName, incrementalSnapshot.SlotStart, incrementalSnapshot.SlotEnd,
		incrementalDiff, cfg.IncrementalThreshold)

	// Additional logging for debugging
	if incrementalDiff < 0 {
		log.Printf("Note: Incremental snapshot is newer than reference slot (incremental: %d > reference: %d)",
			incrementalSnapshot.SlotEnd, referenceSlot)
	}

	if incrementalDiff > cfg.IncrementalThreshold {
		log.Printf("Incremental snapshot is outdated. Diff (%d) exceeds threshold (%d)",
			incrementalDiff, cfg.IncrementalThreshold)
		return false, true
	}

	log.Println("All snapshots are within thresholds")
	return false, false
}

func UntarGenesis(snapshotPath string) error {
	genesisPath := filepath.Join(snapshotPath, "genesis.tar.bz2")
	if _, err := os.Stat(genesisPath); err != nil {
		return fmt.Errorf("genesis file not found: %v", err)
	}

	log.Printf("Untarring genesis file: %s", genesisPath)

	// Create a command to decompress and untar
	cmd := exec.Command("tar", "-xjf", genesisPath, "-C", snapshotPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to untar genesis file: %v, output: %s", err, string(output))
	}

	log.Printf("Successfully untarred genesis file to: %s", snapshotPath)
	return nil
}

package snapshot

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

func TestDownloadSnapshotReturnsHTTPStatusErrorWithRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	t.Cleanup(server.Close)

	err := DownloadSnapshot(server.URL, config.Config{SnapshotPath: t.TempDir()}, "snapshot-", 0)

	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("DownloadSnapshot() error = %v, want *HTTPStatusError", err)
	}
	if statusErr.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("StatusCode = %d, want %d", statusErr.StatusCode, http.StatusTooManyRequests)
	}
	if statusErr.RetryAfter != 3*time.Second {
		t.Fatalf("RetryAfter = %v, want %v", statusErr.RetryAfter, 3*time.Second)
	}
}

// downloadIncrementalAgainstLocalFull spins up a server that serves a single
// incremental snapshot with the given base/end slots, seeds dir with a local
// full snapshot at localFullSlot, and runs DownloadSnapshot against it.
func downloadIncrementalAgainstLocalFull(t *testing.T, localFullSlot, incBaseSlot, incEndSlot int) (dir, incPath string, err error) {
	t.Helper()
	dir = t.TempDir()
	fullName := fmt.Sprintf("snapshot-%d-AbCdEfGhIjKlMnOpQrStUvWx.tar.zst", localFullSlot)
	if writeErr := os.WriteFile(filepath.Join(dir, fullName), []byte("full"), 0644); writeErr != nil {
		t.Fatal(writeErr)
	}

	incName := fmt.Sprintf("incremental-snapshot-%d-%d-AbCdEfGhIjKlMnOpQrStUvWx.tar.zst", incBaseSlot, incEndSlot)
	body := bytes.Repeat([]byte("z"), 2*1024*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="`+incName+`"`)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	err = DownloadSnapshot(server.URL, config.Config{
		SnapshotPath:         dir,
		IncrementalThreshold: 100000,
	}, "incremental", incEndSlot)

	incPath = filepath.Join(dir, "remote", incName)
	return dir, incPath, err
}

func TestDownloadSnapshotRemovesIncrementalOlderThanLocalFull(t *testing.T) {
	_, incPath, err := downloadIncrementalAgainstLocalFull(t, 1000, 500, 999)

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("DownloadSnapshot() error = %v, want *ValidationError", err)
	}
	if _, statErr := os.Stat(incPath); !os.IsNotExist(statErr) {
		t.Fatalf("stale incremental stat error = %v, want file not found", statErr)
	}
}

func TestDownloadSnapshotKeepsIncrementalAheadOfLocalFull(t *testing.T) {
	// Simulates a "safety" incremental fetched before a newer full snapshot
	// lands: its base matches the remote full, but the *local* full on disk
	// is still the older/stale one, so base slot > local full slot.
	_, incPath, err := downloadIncrementalAgainstLocalFull(t, 100, 999, 1000)

	if err != nil {
		t.Fatalf("DownloadSnapshot() error = %v, want nil (safety incremental should be kept)", err)
	}
	if _, statErr := os.Stat(incPath); statErr != nil {
		t.Fatalf("safety incremental stat error = %v, want file present", statErr)
	}
}

func TestDownloadSnapshotKeepsIncrementalMatchingLocalFull(t *testing.T) {
	_, incPath, err := downloadIncrementalAgainstLocalFull(t, 100, 100, 1000)

	if err != nil {
		t.Fatalf("DownloadSnapshot() error = %v, want nil", err)
	}
	if _, statErr := os.Stat(incPath); statErr != nil {
		t.Fatalf("matching incremental stat error = %v, want file present", statErr)
	}
}

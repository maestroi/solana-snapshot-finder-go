package snapshot

import (
	"bytes"
	"errors"
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

func TestDownloadSnapshotRemovesIncrementalWithMismatchedBaseSlot(t *testing.T) {
	dir := t.TempDir()
	fullName := "snapshot-100-AbCdEfGhIjKlMnOpQrStUvWx.tar.zst"
	if err := os.WriteFile(filepath.Join(dir, fullName), []byte("full"), 0644); err != nil {
		t.Fatal(err)
	}

	incName := "incremental-snapshot-999-1000-AbCdEfGhIjKlMnOpQrStUvWx.tar.zst"
	body := bytes.Repeat([]byte("z"), 2*1024*1024)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="`+incName+`"`)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	err := DownloadSnapshot(server.URL, config.Config{
		SnapshotPath:         dir,
		IncrementalThreshold: 100000,
	}, "incremental", 1000)

	if err == nil {
		t.Fatal("DownloadSnapshot() error = nil, want base-slot mismatch error")
	}
	incPath := filepath.Join(dir, "remote", incName)
	if _, statErr := os.Stat(incPath); !os.IsNotExist(statErr) {
		t.Fatalf("mismatched incremental stat error = %v, want file not found", statErr)
	}
}

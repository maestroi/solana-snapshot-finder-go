package snapshot

import (
	"errors"
	"net/http"
	"net/http/httptest"
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

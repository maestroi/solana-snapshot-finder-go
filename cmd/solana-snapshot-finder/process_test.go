package main

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/rpc"
	"github.com/maestroi/solana-snapshot-finder-go/pkg/snapshot"
)

func TestRecordDownloadFailureExcludesHostAndKeepsRetryAfter(t *testing.T) {
	cooldown := &rpc.HostCooldown{}
	statusErr := &snapshot.HTTPStatusError{
		StatusCode: http.StatusTooManyRequests,
		RetryAfter: 4 * time.Second,
		URL:        "http://limited/snapshot.tar.zst",
	}

	recordDownloadFailure(cooldown, "http://limited", fmt.Errorf("download failed: %w", statusErr))

	if !cooldown.IsExcluded("http://limited") {
		t.Fatal("recordDownloadFailure() did not exclude failed host")
	}
	if cooldown.RetryAfter != 4*time.Second {
		t.Fatalf("RetryAfter = %v, want %v", cooldown.RetryAfter, 4*time.Second)
	}
}

func TestRecordDownloadFailureExcludesHardFailure(t *testing.T) {
	cooldown := &rpc.HostCooldown{}

	recordDownloadFailure(cooldown, "http://broken", errors.New("connection reset"))

	if !cooldown.IsExcluded("http://broken") {
		t.Fatal("recordDownloadFailure() did not exclude failed host")
	}
}

func TestWaitForRetrySleepsOnceAndClearsDelay(t *testing.T) {
	cooldown := &rpc.HostCooldown{RetryAfter: 3 * time.Second}
	var slept time.Duration

	waitForRetry(cooldown, func(delay time.Duration) {
		slept = delay
	})

	if slept != 3*time.Second {
		t.Fatalf("slept for %v, want %v", slept, 3*time.Second)
	}
	if cooldown.RetryAfter != 0 {
		t.Fatalf("RetryAfter = %v, want 0", cooldown.RetryAfter)
	}
}

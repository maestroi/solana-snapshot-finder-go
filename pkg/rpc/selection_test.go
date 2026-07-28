package rpc

import (
	"net/http"
	"testing"
	"time"
)

func TestSelectNextRPCSkipsExcluded(t *testing.T) {
	results := []NodeEvaluationResult{
		{RPC: "http://fast", Speed: 100, Status: "good"},
		{RPC: "http://other", Speed: 80, Status: "good"},
	}
	cooldown := &HostCooldown{Excluded: map[string]string{"http://fast": "429"}}

	if got := SelectNextRPC(results, cooldown); got != "http://other" {
		t.Fatalf("SelectNextRPC() = %q, want %q", got, "http://other")
	}
}

func TestSelectNextRPCFallsBackToFastestSlowNode(t *testing.T) {
	results := []NodeEvaluationResult{
		{RPC: "http://slow", Speed: 20, Status: "slow"},
		{RPC: "http://faster", Speed: 40, Status: "slow"},
	}

	if got := SelectNextRPC(results, nil); got != "http://faster" {
		t.Fatalf("SelectNextRPC() = %q, want %q", got, "http://faster")
	}
}

func TestParseRetryAfterSeconds(t *testing.T) {
	headers := http.Header{}
	headers.Set("Retry-After", "7")

	got, ok := ParseRetryAfter(headers, time.Now())
	if !ok || got != 7*time.Second {
		t.Fatalf("ParseRetryAfter() = %v, %v; want %v, true", got, ok, 7*time.Second)
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	now := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	headers := http.Header{}
	headers.Set("Retry-After", now.Add(5*time.Second).Format(http.TimeFormat))

	got, ok := ParseRetryAfter(headers, now)
	if !ok || got != 5*time.Second {
		t.Fatalf("ParseRetryAfter() = %v, %v; want %v, true", got, ok, 5*time.Second)
	}
}

func TestParseRetryAfterClampsExcessiveDelay(t *testing.T) {
	headers := http.Header{}
	headers.Set("Retry-After", "3600")

	got, ok := ParseRetryAfter(headers, time.Now())
	if !ok || got != maxRetryAfter {
		t.Fatalf("ParseRetryAfter() = %v, %v; want %v, true", got, ok, maxRetryAfter)
	}
}

func TestParseRetryAfterClampsExcessiveHTTPDate(t *testing.T) {
	now := time.Date(2026, time.July, 28, 12, 0, 0, 0, time.UTC)
	headers := http.Header{}
	headers.Set("Retry-After", now.Add(2*time.Hour).Format(http.TimeFormat))

	got, ok := ParseRetryAfter(headers, now)
	if !ok || got != maxRetryAfter {
		t.Fatalf("ParseRetryAfter() = %v, %v; want %v, true", got, ok, maxRetryAfter)
	}
}

func TestParseRetryAfterRejectsDeltaSecondsOverflow(t *testing.T) {
	headers := http.Header{}
	headers.Set("Retry-After", "9223372036854775807")

	if duration, ok := ParseRetryAfter(headers, time.Now()); ok {
		t.Fatalf("ParseRetryAfter() = %v, true; want false for duration overflow", duration)
	}
}

func TestMarkRetryAfterKeepsMax(t *testing.T) {
	cooldown := &HostCooldown{}
	cooldown.MarkRetryAfter("http://a", 2*time.Second)
	cooldown.MarkRetryAfter("http://b", 5*time.Second)

	if cooldown.RetryAfter != 5*time.Second {
		t.Fatalf("RetryAfter = %v, want %v", cooldown.RetryAfter, 5*time.Second)
	}
	if !cooldown.IsExcluded("http://a") || !cooldown.IsExcluded("http://b") {
		t.Fatal("MarkRetryAfter did not exclude both RPCs")
	}
}

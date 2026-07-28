package rpc

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type HostCooldown struct {
	Excluded   map[string]string
	RetryAfter time.Duration
}

func (h *HostCooldown) Mark(rpc, reason string) {
	if h == nil {
		return
	}
	if h.Excluded == nil {
		h.Excluded = make(map[string]string)
	}
	h.Excluded[rpc] = reason
}

func (h *HostCooldown) MarkRetryAfter(rpc string, d time.Duration) {
	if h == nil {
		return
	}
	h.Mark(rpc, fmt.Sprintf("retry after %s", d))
	if d > h.RetryAfter {
		h.RetryAfter = d
	}
}

func (h *HostCooldown) IsExcluded(rpc string) bool {
	if h == nil {
		return false
	}
	_, excluded := h.Excluded[rpc]
	return excluded
}

func SelectNextRPC(results []NodeEvaluationResult, cooldown *HostCooldown) string {
	best := ""
	bestSpeed := 0.0
	for _, result := range results {
		if cooldown.IsExcluded(result.RPC) {
			continue
		}
		if result.Status == "good" && result.Speed > bestSpeed {
			best, bestSpeed = result.RPC, result.Speed
		}
	}
	if best != "" {
		return best
	}
	for _, result := range results {
		if cooldown.IsExcluded(result.RPC) {
			continue
		}
		if result.Status == "slow" && result.Speed > bestSpeed {
			best, bestSpeed = result.RPC, result.Speed
		}
	}
	return best
}

func ParseRetryAfter(headers http.Header, now time.Time) (time.Duration, bool) {
	value := strings.TrimSpace(headers.Get("Retry-After"))
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		const maxDurationSeconds = int64((1<<63)-1) / int64(time.Second)
		if seconds > maxDurationSeconds {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	retryAt, err := http.ParseTime(value)
	if err != nil || !retryAt.After(now) {
		return 0, false
	}
	return retryAt.Sub(now), true
}

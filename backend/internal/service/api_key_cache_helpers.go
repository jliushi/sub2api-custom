package service

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
)

// isTimeoutOrConnectionError checks if an auth DB error is transient enough to
// trip the circuit breaker and allow stale cache fallback.
func isTimeoutOrConnectionError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}

	msg := strings.ToLower(err.Error())
	transientFragments := []string{
		"connection refused",
		"connection reset",
		"connection aborted",
		"connection timed out",
		"connection timeout",
		"actively refused",
		"no connection could be made",
		"i/o timeout",
		"deadline exceeded",
		"statement timeout",
		"lock timeout",
		"database is locked",
		"database table is locked",
		"database is busy",
		"too many connections",
		"pool exhausted",
		"bad connection",
		"server closed the connection",
		"broken pipe",
		"network is unreachable",
		"no such host",
		"database is closed",
		"no space left on device",
		"disk full",
		"disk i/o error",
		"could not serialize access",
		"deadlock detected",
		"sqlstate 08006",
		"sqlstate 08001",
		"sqlstate 08003",
		"sqlstate 08004",
		"sqlstate 08007",
		"sqlstate 40001",
		"sqlstate 40p01",
		"sqlstate 53300",
		"sqlstate 57p01",
		"sqlstate 57p02",
		"sqlstate 57p03",
	}
	for _, fragment := range transientFragments {
		if strings.Contains(msg, fragment) {
			return true
		}
	}

	return false
}

// getStaleCacheEntry attempts to retrieve a stale cache entry
// This is used as a fallback when DB is unavailable. The stale window is still
// bounded by the configured L1/L2 cache TTLs; expired Redis keys are not served.
func (s *APIKeyService) getStaleCacheEntry(ctx context.Context, cacheKey string) *APIKeyAuthCacheEntry {
	// Try L1 cache first (in-memory)
	if s.authCacheL1 != nil {
		if val, ok := s.authCacheL1.Get(cacheKey); ok {
			if entry, ok := val.(*APIKeyAuthCacheEntry); ok {
				if isUsableAuthCacheEntry(entry) {
					return entry
				}
			}
		}
	}

	if err := ctx.Err(); err != nil {
		return nil
	}

	// Try L2 cache (Redis). Redis TTL remains the upper bound for stale auth data.
	if s.cache != nil {
		entry, _ := s.cache.GetAuthCache(ctx, cacheKey)
		if isUsableAuthCacheEntry(entry) {
			return entry
		}
	}

	return nil
}

func isUsableAuthCacheEntry(entry *APIKeyAuthCacheEntry) bool {
	if entry == nil {
		return false
	}
	if entry.NotFound {
		return true
	}
	return entry.Snapshot != nil && entry.Snapshot.Version == apiKeyAuthSnapshotVersion
}

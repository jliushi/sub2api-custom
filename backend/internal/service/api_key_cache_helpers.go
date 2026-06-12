package service

import (
	"context"
	"errors"
	"net"
	"syscall"
)

// isTimeoutOrConnectionError checks if error is a timeout or connection error
func isTimeoutOrConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context timeout
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Check for network timeout
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Check for connection errors
	if errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) {
		return true
	}

	return false
}

// getStaleCacheEntry attempts to retrieve a stale cache entry
// This is used as a fallback when DB is unavailable
func (s *APIKeyService) getStaleCacheEntry(cacheKey string) *APIKeyAuthCacheEntry {
	// Try L1 cache first (in-memory)
	if s.authCacheL1 != nil {
		if val, ok := s.authCacheL1.Get(cacheKey); ok {
			if entry, ok := val.(*APIKeyAuthCacheEntry); ok {
				return entry
			}
		}
	}

	// Try L2 cache (Redis) without TTL check
	// In degraded mode, we accept stale data
	if s.cache != nil {
		// Note: This requires the cache implementation to support fetching expired entries
		// For now, we rely on L1 having a longer practical retention
		entry, _ := s.cache.GetAuthCache(context.Background(), cacheKey)
		return entry
	}

	return nil
}

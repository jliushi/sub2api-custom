package service

import (
	"context"
	"log/slog"
	"strings"
	"sync"
)

// AdjustAuthCacheAfterBilling applies the local post-request billing delta to
// the auth snapshot cache. It deliberately does not load from the database:
// local-first billing uses this to keep enforcement real-time while Supabase is
// updated by the hourly ledger flush.
func (s *APIKeyService) AdjustAuthCacheAfterBilling(ctx context.Context, apiKey *APIKey, balanceCost, quotaCost float64) {
	if s == nil || apiKey == nil || strings.TrimSpace(apiKey.Key) == "" {
		return
	}
	if balanceCost <= 0 && quotaCost <= 0 {
		return
	}

	unlock := s.LockAuthCacheBilling(apiKey)
	defer unlock()
	s.adjustAuthCacheAfterBillingLocked(ctx, apiKey, balanceCost, quotaCost)
}

func (s *APIKeyService) LockAuthCacheBilling(apiKey *APIKey) func() {
	if s == nil || apiKey == nil || strings.TrimSpace(apiKey.Key) == "" {
		return func() {}
	}
	lock := s.authCacheAdjustLock(s.authCacheKey(apiKey.Key))
	lock.Lock()
	return lock.Unlock
}

func (s *APIKeyService) adjustAuthCacheAfterBillingLocked(ctx context.Context, apiKey *APIKey, balanceCost, quotaCost float64) {
	if s == nil || apiKey == nil || strings.TrimSpace(apiKey.Key) == "" {
		return
	}
	if balanceCost <= 0 && quotaCost <= 0 {
		return
	}

	cacheKey := s.authCacheKey(apiKey.Key)
	entry, ok := s.getAuthCacheEntry(ctx, cacheKey)
	if !ok || entry == nil || entry.NotFound || entry.Snapshot == nil {
		return
	}
	if entry.Snapshot.Version != apiKeyAuthSnapshotVersion {
		return
	}

	nextEntry := *entry
	nextSnapshot := *entry.Snapshot
	nextEntry.Snapshot = &nextSnapshot

	applyIncrementalLocalBillingOverlay(&nextSnapshot, balanceCost, quotaCost)
	s.setAuthCacheEntry(ctx, cacheKey, &nextEntry, s.authCfg.l2TTL)
}

func (s *APIKeyService) applyLocalBillingOverlay(ctx context.Context, snapshot *APIKeyAuthSnapshot) bool {
	if snapshot == nil {
		return false
	}
	deltas, ok, err := pendingLocalBillingDeltas(ctx, snapshot.APIKeyID, snapshot.UserID)
	if err != nil {
		slog.Warn("local billing delta overlay failed", "api_key_id", snapshot.APIKeyID, "user_id", snapshot.UserID, "error", err)
		return false
	}
	if !ok {
		return false
	}
	applyPendingLocalBillingDeltas(snapshot, deltas)
	return true
}

func applyPendingLocalBillingDeltas(snapshot *APIKeyAuthSnapshot, deltas *LocalBillingDeltas) {
	if snapshot == nil {
		return
	}
	if deltas == nil {
		deltas = &LocalBillingDeltas{}
	}
	snapshot.User.Balance -= deltas.BalanceCost
	snapshot.QuotaUsed += deltas.APIKeyQuotaCost
	if snapshot.Status == StatusActive && snapshot.Quota > 0 && snapshot.QuotaUsed >= snapshot.Quota {
		snapshot.Status = StatusAPIKeyQuotaExhausted
	}
}

func applyIncrementalLocalBillingOverlay(snapshot *APIKeyAuthSnapshot, balanceCost, quotaCost float64) {
	if snapshot == nil {
		return
	}
	if balanceCost > 0 {
		snapshot.User.Balance -= balanceCost
	}
	if quotaCost > 0 {
		snapshot.QuotaUsed += quotaCost
		if snapshot.Status == StatusActive && snapshot.Quota > 0 && snapshot.QuotaUsed >= snapshot.Quota {
			snapshot.Status = StatusAPIKeyQuotaExhausted
		}
	}
}

func (s *APIKeyService) authCacheAdjustLock(cacheKey string) *sync.Mutex {
	actual, _ := s.authAdjustLocks.LoadOrStore(cacheKey, &sync.Mutex{})
	lock, _ := actual.(*sync.Mutex)
	if lock == nil {
		return &sync.Mutex{}
	}
	return lock
}

package service

import (
	"context"
	"sync"
)

// LocalBillingDeltas is the not-yet-persisted local-first billing overlay for
// one auth snapshot. Balance is user-scoped; API key quota is key-scoped.
type LocalBillingDeltas struct {
	BalanceCost     float64
	APIKeyQuotaCost float64
}

// LocalBillingDeltaProvider exposes pending local-first billing deltas without
// making service depend on the repository package.
type LocalBillingDeltaProvider interface {
	PendingBillingDeltas(ctx context.Context, apiKeyID, userID int64) (*LocalBillingDeltas, error)
}

type LocalBillingSyncResult struct {
	Flushed bool `json:"flushed"`
}

type LocalBillingSyncProvider interface {
	FlushLocalBilling(ctx context.Context) (*LocalBillingSyncResult, error)
}

var localBillingDeltaProvider struct {
	sync.RWMutex
	provider LocalBillingDeltaProvider
}

var localBillingSyncProvider struct {
	sync.RWMutex
	provider LocalBillingSyncProvider
}

func RegisterLocalBillingDeltaProvider(provider LocalBillingDeltaProvider) {
	localBillingDeltaProvider.Lock()
	localBillingDeltaProvider.provider = provider
	localBillingDeltaProvider.Unlock()
}

func RegisterLocalBillingSyncProvider(provider LocalBillingSyncProvider) {
	localBillingSyncProvider.Lock()
	localBillingSyncProvider.provider = provider
	localBillingSyncProvider.Unlock()
}

func FlushLocalBilling(ctx context.Context) (*LocalBillingSyncResult, error) {
	localBillingSyncProvider.RLock()
	provider := localBillingSyncProvider.provider
	localBillingSyncProvider.RUnlock()
	if provider == nil {
		return &LocalBillingSyncResult{Flushed: false}, nil
	}
	result, err := provider.FlushLocalBilling(ctx)
	if result == nil {
		result = &LocalBillingSyncResult{}
	}
	return result, err
}

func pendingLocalBillingDeltas(ctx context.Context, apiKeyID, userID int64) (*LocalBillingDeltas, bool, error) {
	localBillingDeltaProvider.RLock()
	provider := localBillingDeltaProvider.provider
	localBillingDeltaProvider.RUnlock()
	if provider == nil {
		return nil, false, nil
	}
	deltas, err := provider.PendingBillingDeltas(ctx, apiKeyID, userID)
	if err != nil {
		return nil, true, err
	}
	if deltas == nil {
		deltas = &LocalBillingDeltas{}
	}
	return deltas, true, nil
}

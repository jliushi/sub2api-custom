package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyPendingLocalBillingDeltasDeductsBalanceAndQuota(t *testing.T) {
	snapshot := &APIKeyAuthSnapshot{
		Status:    StatusActive,
		Quota:     10,
		QuotaUsed: 3,
		User: APIKeyAuthUserSnapshot{
			Balance: 20,
		},
	}

	applyPendingLocalBillingDeltas(snapshot, &LocalBillingDeltas{
		BalanceCost:     2.5,
		APIKeyQuotaCost: 4,
	})

	require.Equal(t, 17.5, snapshot.User.Balance)
	require.Equal(t, 7.0, snapshot.QuotaUsed)
	require.Equal(t, StatusActive, snapshot.Status)
}

func TestApplyPendingLocalBillingDeltasMarksQuotaExhausted(t *testing.T) {
	snapshot := &APIKeyAuthSnapshot{
		Status:    StatusActive,
		Quota:     10,
		QuotaUsed: 9,
		User: APIKeyAuthUserSnapshot{
			Balance: 20,
		},
	}

	applyPendingLocalBillingDeltas(snapshot, &LocalBillingDeltas{
		BalanceCost:     1,
		APIKeyQuotaCost: 1,
	})

	require.Equal(t, 19.0, snapshot.User.Balance)
	require.Equal(t, 10.0, snapshot.QuotaUsed)
	require.Equal(t, StatusAPIKeyQuotaExhausted, snapshot.Status)
}

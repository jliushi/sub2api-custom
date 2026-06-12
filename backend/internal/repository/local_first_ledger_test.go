package repository

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type localFirstBillingRepoStub struct {
	service.UsageBillingRepository
	failRequestID string
	applied       []string
}

func (s *localFirstBillingRepoStub) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	if cmd.RequestID == s.failRequestID {
		return nil, errors.New("forced billing failure")
	}
	s.applied = append(s.applied, cmd.RequestID)
	return &service.UsageBillingApplyResult{Applied: true}, nil
}

type localFirstUsageRepoStub struct {
	service.UsageLogRepository
	created []string
}

func (s *localFirstUsageRepoStub) Create(ctx context.Context, usage *service.UsageLog) (bool, error) {
	s.created = append(s.created, usage.RequestID)
	return true, nil
}

type localFirstBackupStoreStub struct {
	backups int
	reasons []string
	usage   int
	billing int
}

func (s *localFirstBackupStoreStub) Restore(ctx context.Context, ledger *localFirstLedger) error {
	return nil
}

func (s *localFirstBackupStoreStub) Backup(ctx context.Context, ledger *localFirstLedger, reason string) error {
	s.backups++
	s.reasons = append(s.reasons, reason)
	snapshot, err := ledger.buildBackupSnapshot(ctx, reason)
	if err != nil {
		return err
	}
	s.usage = len(snapshot.Usage)
	s.billing = len(snapshot.Billing)
	return nil
}

func newLocalFirstLedgerForTest(t *testing.T) *localFirstLedger {
	t.Helper()

	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "ledger.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ledger := &localFirstLedger{
		db:         db,
		syncedTTL:  24 * time.Hour,
		maxRetries: 1,
		stopCh:     make(chan struct{}),
	}
	require.NoError(t, ledger.init(context.Background()))
	return ledger
}

func TestLocalFirstLedgerFlushSkipsFailedBillingAndContinues(t *testing.T) {
	ctx := context.Background()
	ledger := newLocalFirstLedgerForTest(t)
	billingBase := &localFirstBillingRepoStub{failRequestID: "bad"}
	usageBase := &localFirstUsageRepoStub{}
	ledger.billingBase = billingBase
	ledger.usageBase = usageBase

	applied, err := ledger.RecordBilling(ctx, &service.UsageBillingCommand{
		RequestID:       "bad",
		APIKeyID:        10,
		UserID:          20,
		BalanceCost:     1,
		APIKeyQuotaCost: 1,
	})
	require.NoError(t, err)
	require.True(t, applied)
	applied, err = ledger.RecordBilling(ctx, &service.UsageBillingCommand{
		RequestID:       "good",
		APIKeyID:        10,
		UserID:          20,
		BalanceCost:     2,
		APIKeyQuotaCost: 2,
	})
	require.NoError(t, err)
	require.True(t, applied)

	_, inserted, err := ledger.RecordUsage(ctx, &service.UsageLog{RequestID: "usage-good", APIKeyID: 10})
	require.NoError(t, err)
	require.True(t, inserted)

	require.NoError(t, ledger.Flush(ctx))
	require.Equal(t, []string{"good"}, billingBase.applied)
	require.Equal(t, []string{"usage-good"}, usageBase.created)

	var badStatus, goodStatus int
	require.NoError(t, ledger.db.QueryRowContext(ctx, `
		SELECT synced FROM local_billing_events WHERE request_id = ?
	`, "bad").Scan(&badStatus))
	require.NoError(t, ledger.db.QueryRowContext(ctx, `
		SELECT synced FROM local_billing_events WHERE request_id = ?
	`, "good").Scan(&goodStatus))
	require.Equal(t, localFirstStatusFailed, badStatus)
	require.Equal(t, localFirstStatusSynced, goodStatus)
}

func TestLocalFirstLedgerBacksUpWhenFlushToSupabaseFails(t *testing.T) {
	ctx := context.Background()
	ledger := newLocalFirstLedgerForTest(t)
	backup := &localFirstBackupStoreStub{}
	ledger.backup = backup
	ledger.billingBase = &localFirstBillingRepoStub{failRequestID: "bad"}
	ledger.usageBase = &localFirstUsageRepoStub{}

	applied, err := ledger.RecordBilling(ctx, &service.UsageBillingCommand{
		RequestID:       "bad",
		APIKeyID:        10,
		UserID:          20,
		BalanceCost:     1,
		APIKeyQuotaCost: 1,
	})
	require.NoError(t, err)
	require.True(t, applied)

	require.NoError(t, ledger.Flush(ctx))
	require.Equal(t, 1, backup.backups)
	require.Equal(t, 1, backup.billing)
	require.Equal(t, 0, backup.usage)
	require.Contains(t, backup.reasons[0], "supabase_flush_failed")
}

func TestLocalFirstLedgerRestoresGitHubBackupSnapshot(t *testing.T) {
	ctx := context.Background()
	source := newLocalFirstLedgerForTest(t)
	applied, err := source.RecordBilling(ctx, &service.UsageBillingCommand{
		RequestID:       "restore-billing",
		APIKeyID:        31,
		UserID:          41,
		BalanceCost:     1.5,
		APIKeyQuotaCost: 1.5,
	})
	require.NoError(t, err)
	require.True(t, applied)
	_, inserted, err := source.RecordUsage(ctx, &service.UsageLog{RequestID: "restore-usage", APIKeyID: 31})
	require.NoError(t, err)
	require.True(t, inserted)

	snapshot, err := source.buildBackupSnapshot(ctx, "test")
	require.NoError(t, err)
	require.Len(t, snapshot.Billing, 1)
	require.Len(t, snapshot.Usage, 1)

	target := newLocalFirstLedgerForTest(t)
	restoredUsage, restoredBilling, err := target.restoreBackupSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, 1, restoredUsage)
	require.Equal(t, 1, restoredBilling)

	restoredUsage, restoredBilling, err = target.restoreBackupSnapshot(ctx, snapshot)
	require.NoError(t, err)
	require.Equal(t, 0, restoredUsage)
	require.Equal(t, 0, restoredBilling)

	var usageStatus, billingStatus int
	require.NoError(t, target.db.QueryRowContext(ctx, `
		SELECT synced FROM local_usage_events WHERE request_id = ?
	`, "restore-usage").Scan(&usageStatus))
	require.NoError(t, target.db.QueryRowContext(ctx, `
		SELECT synced FROM local_billing_events WHERE request_id = ?
	`, "restore-billing").Scan(&billingStatus))
	require.Equal(t, localFirstStatusPending, usageStatus)
	require.Equal(t, localFirstStatusPending, billingStatus)
}

func TestLocalFirstGitHubBackupEncryptionRoundTrip(t *testing.T) {
	raw := []byte(`{"version":1,"usage":[{"request_id":"r1"}]}`)
	encrypted, err := encryptLocalFirstGitHubPayload(raw, "backup-secret")
	require.NoError(t, err)
	require.NotContains(t, string(encrypted), "request_id")
	restored, err := decryptLocalFirstGitHubPayload(encrypted, "backup-secret")
	require.NoError(t, err)
	require.Equal(t, raw, restored)
	_, err = decryptLocalFirstGitHubPayload(encrypted, "wrong-secret")
	require.Error(t, err)
}

func TestLocalFirstLedgerPendingBillingDeltasExcludeSyncedEvents(t *testing.T) {
	ctx := context.Background()
	ledger := newLocalFirstLedgerForTest(t)

	applied, err := ledger.RecordBilling(ctx, &service.UsageBillingCommand{
		RequestID:       "synced",
		APIKeyID:        11,
		UserID:          22,
		BalanceCost:     1.25,
		APIKeyQuotaCost: 2.5,
	})
	require.NoError(t, err)
	require.True(t, applied)
	applied, err = ledger.RecordBilling(ctx, &service.UsageBillingCommand{
		RequestID:       "pending",
		APIKeyID:        11,
		UserID:          22,
		BalanceCost:     3.75,
		APIKeyQuotaCost: 4.5,
	})
	require.NoError(t, err)
	require.True(t, applied)
	var pendingID int64
	require.NoError(t, ledger.db.QueryRowContext(ctx, `
		SELECT id FROM local_billing_events WHERE request_id = ?
	`, "pending").Scan(&pendingID))
	require.NoError(t, ledger.markBillingEventFailure(ctx, pendingID, errors.New("keep enforcing locally"), true))
	_, err = ledger.db.ExecContext(ctx, `
		UPDATE local_billing_events
		SET synced = ?, synced_at = CURRENT_TIMESTAMP
		WHERE request_id = ?
	`, localFirstStatusSynced, "synced")
	require.NoError(t, err)

	deltas, err := ledger.PendingBillingDeltas(ctx, 11, 22)
	require.NoError(t, err)
	require.Equal(t, 3.75, deltas.BalanceCost)
	require.Equal(t, 4.5, deltas.APIKeyQuotaCost)
}

package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	_ "modernc.org/sqlite"
)

const (
	localFirstEnvEnabled       = "LOCAL_FIRST_BILLING"
	localFirstEnvDBPath        = "LOCAL_BILLING_DB"
	localFirstEnvFlushInterval = "LOCAL_BILLING_FLUSH_INTERVAL_SECONDS"
	localFirstEnvSyncedTTL     = "LOCAL_BILLING_SYNCED_RETENTION_SECONDS"
	localFirstEnvMaxRetries    = "LOCAL_BILLING_MAX_EVENT_RETRIES"
	localFirstDefaultDBPath    = "/tmp/sub2api-local-billing.sqlite"
	localFirstDefaultFlush     = time.Hour
	localFirstDefaultSyncedTTL = 24 * time.Hour
	localFirstDefaultRetries   = 24
	localFirstFlushLimit       = 10000
	localFirstStatusFailed     = -1
	localFirstStatusPending    = 0
	localFirstStatusSynced     = 1
)

var (
	localFirstGlobalMu     sync.Mutex
	localFirstGlobalLedger *localFirstLedger
)

func localFirstBillingEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(localFirstEnvEnabled))) {
	case "1", "true", "yes", "on", "enabled":
		return true
	default:
		return false
	}
}

func localFirstBillingDBPath() string {
	if v := strings.TrimSpace(os.Getenv(localFirstEnvDBPath)); v != "" {
		return v
	}
	return localFirstDefaultDBPath
}

func localFirstFlushInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(localFirstEnvFlushInterval))
	if raw == "" {
		return localFirstDefaultFlush
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return localFirstDefaultFlush
	}
	return time.Duration(seconds) * time.Second
}

func localFirstSyncedRetention() time.Duration {
	raw := strings.TrimSpace(os.Getenv(localFirstEnvSyncedTTL))
	if raw == "" {
		return localFirstDefaultSyncedTTL
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return localFirstDefaultSyncedTTL
	}
	return time.Duration(seconds) * time.Second
}

func localFirstMaxEventRetries() int {
	raw := strings.TrimSpace(os.Getenv(localFirstEnvMaxRetries))
	if raw == "" {
		return localFirstDefaultRetries
	}
	retries, err := strconv.Atoi(raw)
	if err != nil {
		return localFirstDefaultRetries
	}
	return retries
}

func defaultLocalFirstLedger() (*localFirstLedger, error) {
	localFirstGlobalMu.Lock()
	defer localFirstGlobalMu.Unlock()

	if localFirstGlobalLedger != nil {
		return localFirstGlobalLedger, nil
	}

	path := localFirstBillingDBPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	// SQLite connection string with pragmas using modernc.org/sqlite syntax
	// Using _pragma parameters ensures each connection gets the same settings
	connStr := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=cache_size(-64000)&cache=shared"
	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		return nil, err
	}
	// Keep single connection to avoid "database is locked" errors
	// SQLite with WAL mode is optimized for this use case
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)
	db.SetConnMaxIdleTime(10 * time.Minute)

	ledger := &localFirstLedger{
		db:            db,
		flushInterval: localFirstFlushInterval(),
		syncedTTL:     localFirstSyncedRetention(),
		maxRetries:    localFirstMaxEventRetries(),
		backup:        newLocalFirstGitHubBackupFromEnv(),
		stopCh:        make(chan struct{}),
	}
	if err := ledger.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	if ledger.backup != nil {
		if err := ledger.backup.Restore(context.Background(), ledger); err != nil {
			log.Printf("[local-first] GitHub backup restore skipped: %v", err)
		}
	}
	service.RegisterLocalBillingDeltaProvider(ledger)
	service.RegisterLocalBillingSyncProvider(ledger)
	localFirstGlobalLedger = ledger
	return ledger, nil
}

type localFirstLedger struct {
	db            *sql.DB
	flushInterval time.Duration
	syncedTTL     time.Duration
	maxRetries    int

	mu          sync.Mutex
	usageBase   service.UsageLogRepository
	billingBase service.UsageBillingRepository
	backup      localFirstBackupStore

	startOnce sync.Once
	stopCh    chan struct{}
}

func (l *localFirstLedger) init(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS local_usage_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT NOT NULL,
			api_key_id INTEGER NOT NULL,
			payload TEXT NOT NULL,
			synced INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			synced_at TEXT,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			last_attempt_at TEXT,
			failed_at TEXT
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_local_usage_request_api_key
			ON local_usage_events(request_id, api_key_id)`,
		`CREATE INDEX IF NOT EXISTS idx_local_usage_unsynced
			ON local_usage_events(synced, id)`,
		`CREATE TABLE IF NOT EXISTS local_billing_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			request_id TEXT NOT NULL,
			api_key_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL DEFAULT 0,
			fingerprint TEXT NOT NULL,
			payload TEXT NOT NULL,
			synced INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			synced_at TEXT,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			last_attempt_at TEXT,
			failed_at TEXT
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_local_billing_request_api_key
			ON local_billing_events(request_id, api_key_id)`,
		`CREATE INDEX IF NOT EXISTS idx_local_billing_unsynced
			ON local_billing_events(synced, id)`,
	}
	for _, stmt := range stmts {
		if _, err := l.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err := l.ensureLegacyColumns(ctx); err != nil {
		return err
	}
	return nil
}

func (l *localFirstLedger) ensureLegacyColumns(ctx context.Context) error {
	additions := []struct {
		table string
		name  string
		ddl   string
	}{
		{"local_usage_events", "attempt_count", "attempt_count INTEGER NOT NULL DEFAULT 0"},
		{"local_usage_events", "last_error", "last_error TEXT"},
		{"local_usage_events", "last_attempt_at", "last_attempt_at TEXT"},
		{"local_usage_events", "failed_at", "failed_at TEXT"},
		{"local_billing_events", "user_id", "user_id INTEGER NOT NULL DEFAULT 0"},
		{"local_billing_events", "attempt_count", "attempt_count INTEGER NOT NULL DEFAULT 0"},
		{"local_billing_events", "last_error", "last_error TEXT"},
		{"local_billing_events", "last_attempt_at", "last_attempt_at TEXT"},
		{"local_billing_events", "failed_at", "failed_at TEXT"},
	}
	for _, addition := range additions {
		if err := l.addColumnIfMissing(ctx, addition.table, addition.name, addition.ddl); err != nil {
			return err
		}
	}
	return nil
}

func (l *localFirstLedger) addColumnIfMissing(ctx context.Context, table, name, ddl string) error {
	rows, err := l.db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var cid int
		var colName, colType string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &colName, &colType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if strings.EqualFold(colName, name) {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = l.db.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+ddl)
	return err
}

func (l *localFirstLedger) setUsageBase(base service.UsageLogRepository) {
	l.mu.Lock()
	l.usageBase = base
	l.mu.Unlock()
	l.start()
}

func (l *localFirstLedger) setBillingBase(base service.UsageBillingRepository) {
	l.mu.Lock()
	l.billingBase = base
	l.mu.Unlock()
	l.start()
}

func (l *localFirstLedger) start() {
	l.startOnce.Do(func() {
		go l.run()
	})
}

func (l *localFirstLedger) run() {
	ticker := time.NewTicker(l.flushInterval)
	defer ticker.Stop()
	log.Printf("[local-first] billing ledger enabled, flushing every %s", l.flushInterval)
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), minDuration(l.flushInterval/2, 30*time.Minute))
			if err := l.Flush(ctx); err != nil {
				log.Printf("[local-first] flush failed: %v", err)
			}
			cancel()
		case <-l.stopCh:
			return
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= 0 || a > b {
		return b
	}
	return a
}

func (l *localFirstLedger) RecordUsage(ctx context.Context, usage *service.UsageLog) (int64, bool, error) {
	if usage == nil {
		return 0, false, errors.New("nil usage log")
	}
	if usage.CreatedAt.IsZero() {
		usage.CreatedAt = time.Now().UTC()
	}
	payloadLog := *usage
	payloadLog.User = nil
	payloadLog.APIKey = nil
	payloadLog.Account = nil
	payloadLog.Group = nil
	payloadLog.Subscription = nil
	payload, err := json.Marshal(&payloadLog)
	if err != nil {
		return 0, false, err
	}
	res, err := l.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO local_usage_events (request_id, api_key_id, payload)
		VALUES (?, ?, ?)
	`, usage.RequestID, usage.APIKeyID, string(payload))
	if err != nil {
		return 0, false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if affected == 0 {
		return 0, false, nil
	}
	id, _ := res.LastInsertId()
	return id, true, nil
}

func (l *localFirstLedger) RecordBilling(ctx context.Context, cmd *service.UsageBillingCommand) (applied bool, err error) {
	if cmd == nil {
		return false, nil
	}
	cmd.Normalize()
	if cmd.RequestID == "" {
		return false, service.ErrUsageBillingRequestIDRequired
	}

	var existing string
	err = l.db.QueryRowContext(ctx, `
		SELECT fingerprint
		FROM local_billing_events
		WHERE request_id = ? AND api_key_id = ?
	`, cmd.RequestID, cmd.APIKeyID).Scan(&existing)
	if err == nil {
		if existing != cmd.RequestFingerprint {
			return false, service.ErrUsageBillingRequestConflict
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	payload, err := json.Marshal(cmd)
	if err != nil {
		return false, err
	}
	res, err := l.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO local_billing_events (request_id, api_key_id, user_id, fingerprint, payload)
		VALUES (?, ?, ?, ?, ?)
	`, cmd.RequestID, cmd.APIKeyID, cmd.UserID, cmd.RequestFingerprint, string(payload))
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected > 0 {
		return true, nil
	}

	err = l.db.QueryRowContext(ctx, `
		SELECT fingerprint
		FROM local_billing_events
		WHERE request_id = ? AND api_key_id = ?
	`, cmd.RequestID, cmd.APIKeyID).Scan(&existing)
	if err != nil {
		return false, err
	}
	if existing != cmd.RequestFingerprint {
		return false, service.ErrUsageBillingRequestConflict
	}
	return false, nil
}

func (l *localFirstLedger) PendingBillingDeltas(ctx context.Context, apiKeyID, userID int64) (*service.LocalBillingDeltas, error) {
	if l == nil || l.db == nil {
		return &service.LocalBillingDeltas{}, nil
	}
	rows, err := l.db.QueryContext(ctx, `
		SELECT payload
		FROM local_billing_events
		WHERE synced <> ? AND (api_key_id = ? OR user_id = ?)
	`, localFirstStatusSynced, apiKeyID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deltas service.LocalBillingDeltas
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var cmd service.UsageBillingCommand
		if err := json.Unmarshal([]byte(payload), &cmd); err != nil {
			log.Printf("[local-first] skip undecodable pending billing delta: %v", err)
			continue
		}
		if cmd.UserID == userID {
			deltas.BalanceCost += cmd.BalanceCost
		}
		if cmd.APIKeyID == apiKeyID {
			deltas.APIKeyQuotaCost += cmd.APIKeyQuotaCost
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &deltas, nil
}

func (l *localFirstLedger) Flush(ctx context.Context) error {
	billingStats, err := l.flushBilling(ctx)
	if err != nil {
		return err
	}
	usageStats, err := l.flushUsage(ctx)
	if err != nil {
		return err
	}
	l.backupAfterFlushFailure(ctx, billingStats.failed+usageStats.failed)
	if err := l.cleanupSynced(ctx); err != nil {
		return err
	}
	return nil
}

func (l *localFirstLedger) FlushLocalBilling(ctx context.Context) (*service.LocalBillingSyncResult, error) {
	if l == nil {
		return &service.LocalBillingSyncResult{Flushed: false}, nil
	}
	if err := l.Flush(ctx); err != nil {
		return &service.LocalBillingSyncResult{Flushed: true}, err
	}
	return &service.LocalBillingSyncResult{Flushed: true}, nil
}

type localFirstFlushStats struct {
	selected int
	synced   int
	failed   int
}

func (l *localFirstLedger) flushBilling(ctx context.Context) (localFirstFlushStats, error) {
	l.mu.Lock()
	base := l.billingBase
	l.mu.Unlock()
	if base == nil {
		return localFirstFlushStats{}, nil
	}

	rows, err := l.db.QueryContext(ctx, `
		SELECT id, payload, attempt_count
		FROM local_billing_events
		WHERE synced = ?
		ORDER BY id ASC
		LIMIT ?
	`, localFirstStatusPending, localFirstFlushLimit)
	if err != nil {
		return localFirstFlushStats{}, err
	}
	defer rows.Close()

	type event struct {
		id           int64
		payload      string
		attemptCount int
	}
	var events []event
	for rows.Next() {
		var ev event
		if err := rows.Scan(&ev.id, &ev.payload, &ev.attemptCount); err != nil {
			return localFirstFlushStats{}, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return localFirstFlushStats{}, err
	}

	var syncedCount, failedCount int
	for _, ev := range events {
		var cmd service.UsageBillingCommand
		if err := json.Unmarshal([]byte(ev.payload), &cmd); err != nil {
			if markErr := l.markBillingEventFailure(ctx, ev.id, err, true); markErr != nil {
				return localFirstFlushStats{}, markErr
			}
			failedCount++
			log.Printf("[local-first] skipped bad billing event %d: %v", ev.id, err)
			continue
		}
		if _, err := base.Apply(ctx, &cmd); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return localFirstFlushStats{}, ctxErr
			}
			if markErr := l.markBillingEventFailure(ctx, ev.id, err, false); markErr != nil {
				return localFirstFlushStats{}, markErr
			}
			failedCount++
			log.Printf("[local-first] billing event %d flush failed (attempt %d/%s): %v", ev.id, ev.attemptCount+1, l.retryLimitLabel(), err)
			continue
		}
		if _, err := l.db.ExecContext(ctx, `
			UPDATE local_billing_events
			SET synced = ?, synced_at = CURRENT_TIMESTAMP, last_error = NULL, failed_at = NULL
			WHERE id = ?
		`, localFirstStatusSynced, ev.id); err != nil {
			return localFirstFlushStats{}, err
		}
		syncedCount++
	}
	if len(events) > 0 {
		log.Printf("[local-first] processed billing events: selected=%d synced=%d failed=%d", len(events), syncedCount, failedCount)
	}
	return localFirstFlushStats{selected: len(events), synced: syncedCount, failed: failedCount}, nil
}

func (l *localFirstLedger) cleanupSynced(ctx context.Context) error {
	if l.syncedTTL <= 0 {
		return nil
	}
	modifier := fmt.Sprintf("-%d seconds", int(l.syncedTTL.Seconds()))
	if _, err := l.db.ExecContext(ctx, `
		DELETE FROM local_billing_events
		WHERE synced = 1 AND synced_at IS NOT NULL AND synced_at < datetime('now', ?)
	`, modifier); err != nil {
		return err
	}
	if _, err := l.db.ExecContext(ctx, `
		DELETE FROM local_usage_events
		WHERE synced = 1 AND synced_at IS NOT NULL AND synced_at < datetime('now', ?)
	`, modifier); err != nil {
		return err
	}
	return nil
}

func (l *localFirstLedger) flushUsage(ctx context.Context) (localFirstFlushStats, error) {
	l.mu.Lock()
	base := l.usageBase
	l.mu.Unlock()
	if base == nil {
		return localFirstFlushStats{}, nil
	}

	rows, err := l.db.QueryContext(ctx, `
		SELECT id, payload, attempt_count
		FROM local_usage_events
		WHERE synced = ?
		ORDER BY id ASC
		LIMIT ?
	`, localFirstStatusPending, localFirstFlushLimit)
	if err != nil {
		return localFirstFlushStats{}, err
	}
	defer rows.Close()

	type event struct {
		id           int64
		payload      string
		attemptCount int
	}
	var events []event
	for rows.Next() {
		var ev event
		if err := rows.Scan(&ev.id, &ev.payload, &ev.attemptCount); err != nil {
			return localFirstFlushStats{}, err
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return localFirstFlushStats{}, err
	}

	var syncedCount, failedCount int
	for _, ev := range events {
		var usage service.UsageLog
		if err := json.Unmarshal([]byte(ev.payload), &usage); err != nil {
			if markErr := l.markUsageEventFailure(ctx, ev.id, err, true); markErr != nil {
				return localFirstFlushStats{}, markErr
			}
			failedCount++
			log.Printf("[local-first] skipped bad usage event %d: %v", ev.id, err)
			continue
		}
		if _, err := base.Create(ctx, &usage); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return localFirstFlushStats{}, ctxErr
			}
			if markErr := l.markUsageEventFailure(ctx, ev.id, err, false); markErr != nil {
				return localFirstFlushStats{}, markErr
			}
			failedCount++
			log.Printf("[local-first] usage event %d flush failed (attempt %d/%s): %v", ev.id, ev.attemptCount+1, l.retryLimitLabel(), err)
			continue
		}
		if _, err := l.db.ExecContext(ctx, `
			UPDATE local_usage_events
			SET synced = ?, synced_at = CURRENT_TIMESTAMP, last_error = NULL, failed_at = NULL
			WHERE id = ?
		`, localFirstStatusSynced, ev.id); err != nil {
			return localFirstFlushStats{}, err
		}
		syncedCount++
	}
	if len(events) > 0 {
		log.Printf("[local-first] processed usage events: selected=%d synced=%d failed=%d", len(events), syncedCount, failedCount)
	}
	return localFirstFlushStats{selected: len(events), synced: syncedCount, failed: failedCount}, nil
}

func (l *localFirstLedger) backupAfterFlushFailure(ctx context.Context, failedCount int) {
	if l == nil || l.backup == nil || failedCount <= 0 {
		return
	}
	if err := l.backup.Backup(ctx, l, fmt.Sprintf("supabase_flush_failed:%d", failedCount)); err != nil {
		log.Printf("[local-first] GitHub backup failed after Supabase flush failure: %v", err)
	}
}

func (l *localFirstLedger) markBillingEventFailure(ctx context.Context, id int64, failure error, terminal bool) error {
	return l.markEventFailure(ctx, "local_billing_events", id, failure, terminal)
}

func (l *localFirstLedger) markUsageEventFailure(ctx context.Context, id int64, failure error, terminal bool) error {
	return l.markEventFailure(ctx, "local_usage_events", id, failure, terminal)
}

func (l *localFirstLedger) markEventFailure(ctx context.Context, table string, id int64, failure error, terminal bool) error {
	switch table {
	case "local_billing_events", "local_usage_events":
	default:
		return fmt.Errorf("invalid local-first event table %q", table)
	}

	msg := truncateLocalFirstError(failure)
	if terminal {
		_, err := l.db.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET synced = ?, attempt_count = attempt_count + 1, last_error = ?, last_attempt_at = CURRENT_TIMESTAMP, failed_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`, table), localFirstStatusFailed, msg, id)
		return err
	}

	if l.maxRetries <= 0 {
		_, err := l.db.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET attempt_count = attempt_count + 1, last_error = ?, last_attempt_at = CURRENT_TIMESTAMP
			WHERE id = ?
		`, table), msg, id)
		return err
	}

	_, err := l.db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE %s
		SET attempt_count = attempt_count + 1,
			last_error = ?,
			last_attempt_at = CURRENT_TIMESTAMP,
			synced = CASE WHEN attempt_count + 1 >= ? THEN ? ELSE ? END,
			failed_at = CASE WHEN attempt_count + 1 >= ? THEN CURRENT_TIMESTAMP ELSE failed_at END
		WHERE id = ?
	`, table), msg, l.maxRetries, localFirstStatusFailed, localFirstStatusPending, l.maxRetries, id)
	return err
}

func (l *localFirstLedger) retryLimitLabel() string {
	if l == nil || l.maxRetries <= 0 {
		return "unlimited"
	}
	return strconv.Itoa(l.maxRetries)
}

func truncateLocalFirstError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.TrimSpace(err.Error())
	const maxLen = 1000
	if len(msg) > maxLen {
		return msg[:maxLen]
	}
	return msg
}

type localFirstUsageLogRepository struct {
	service.UsageLogRepository
	ledger *localFirstLedger
}

func NewLocalFirstUsageLogRepository(base service.UsageLogRepository) service.UsageLogRepository {
	if !localFirstBillingEnabled() || base == nil {
		return base
	}
	ledger, err := defaultLocalFirstLedger()
	if err != nil {
		log.Printf("[local-first] disabled usage ledger: %v", err)
		return base
	}
	ledger.setUsageBase(base)
	return &localFirstUsageLogRepository{UsageLogRepository: base, ledger: ledger}
}

func (r *localFirstUsageLogRepository) Create(ctx context.Context, usage *service.UsageLog) (bool, error) {
	id, inserted, err := r.ledger.RecordUsage(ctx, usage)
	if err != nil {
		return false, err
	}
	if usage != nil && usage.ID == 0 {
		usage.ID = id
	}
	return inserted, nil
}

type localFirstUsageBillingRepository struct {
	service.UsageBillingRepository
	ledger *localFirstLedger
}

func NewLocalFirstUsageBillingRepository(base service.UsageBillingRepository) service.UsageBillingRepository {
	if !localFirstBillingEnabled() || base == nil {
		return base
	}
	ledger, err := defaultLocalFirstLedger()
	if err != nil {
		log.Printf("[local-first] disabled billing ledger: %v", err)
		return base
	}
	ledger.setBillingBase(base)
	return &localFirstUsageBillingRepository{UsageBillingRepository: base, ledger: ledger}
}

func (r *localFirstUsageBillingRepository) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	applied, err := r.ledger.RecordBilling(ctx, cmd)
	if err != nil {
		return nil, err
	}
	return &service.UsageBillingApplyResult{Applied: applied}, nil
}

package repository

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

const (
	localFirstGitHubEnvEnabled       = "LOCAL_BILLING_GITHUB_BACKUP_ENABLED"
	localFirstGitHubEnvToken         = "LOCAL_BILLING_GITHUB_TOKEN"
	localFirstGitHubEnvRepo          = "LOCAL_BILLING_GITHUB_REPO"
	localFirstGitHubEnvBranch        = "LOCAL_BILLING_GITHUB_BRANCH"
	localFirstGitHubEnvPath          = "LOCAL_BILLING_GITHUB_PATH"
	localFirstGitHubEnvEncryptionKey = "LOCAL_BILLING_GITHUB_ENCRYPTION_KEY"
	localFirstGitHubEnvMinInterval   = "LOCAL_BILLING_GITHUB_MIN_INTERVAL_SECONDS"
	localFirstGitHubEnvAPIURL        = "LOCAL_BILLING_GITHUB_API_URL"

	localFirstGitHubDefaultBranch      = "main"
	localFirstGitHubDefaultPath        = "local-billing/latest.snapshot.gz.enc"
	localFirstGitHubDefaultMinInterval = time.Hour
	localFirstGitHubDefaultAPIURL      = "https://api.github.com"
	localFirstGitHubCipherMagic        = "s2apilgb1"
	localFirstGitHubSnapshotVersion    = 1
)

type localFirstBackupStore interface {
	Restore(ctx context.Context, ledger *localFirstLedger) error
	Backup(ctx context.Context, ledger *localFirstLedger, reason string) error
}

type localFirstGitHubBackup struct {
	token         string
	repo          string
	branch        string
	path          string
	encryptionKey string
	minInterval   time.Duration
	apiURL        string
	client        *http.Client

	mu           sync.Mutex
	lastBackupAt time.Time
}

type localFirstBackupSnapshot struct {
	Version   int                            `json:"version"`
	CreatedAt time.Time                      `json:"created_at"`
	Reason    string                         `json:"reason"`
	Usage     []localFirstUsageBackupEvent   `json:"usage"`
	Billing   []localFirstBillingBackupEvent `json:"billing"`
}

type localFirstUsageBackupEvent struct {
	RequestID      string `json:"request_id"`
	APIKeyID       int64  `json:"api_key_id"`
	Payload        string `json:"payload"`
	CreatedAt      string `json:"created_at,omitempty"`
	LastError      string `json:"last_error,omitempty"`
	LastAttemptAt  string `json:"last_attempt_at,omitempty"`
	FailedAt       string `json:"failed_at,omitempty"`
	PreviousStatus int    `json:"previous_status"`
}

type localFirstBillingBackupEvent struct {
	RequestID      string `json:"request_id"`
	APIKeyID       int64  `json:"api_key_id"`
	UserID         int64  `json:"user_id"`
	Fingerprint    string `json:"fingerprint"`
	Payload        string `json:"payload"`
	CreatedAt      string `json:"created_at,omitempty"`
	LastError      string `json:"last_error,omitempty"`
	LastAttemptAt  string `json:"last_attempt_at,omitempty"`
	FailedAt       string `json:"failed_at,omitempty"`
	PreviousStatus int    `json:"previous_status"`
}

func newLocalFirstGitHubBackupFromEnv() localFirstBackupStore {
	if !localFirstGitHubEnabled() {
		return nil
	}
	token := strings.TrimSpace(os.Getenv(localFirstGitHubEnvToken))
	repo := strings.TrimSpace(os.Getenv(localFirstGitHubEnvRepo))
	encryptionKey := strings.TrimSpace(os.Getenv(localFirstGitHubEnvEncryptionKey))
	if token == "" || repo == "" || encryptionKey == "" {
		log.Printf("[local-first] GitHub backup disabled: %s, %s, and %s are required", localFirstGitHubEnvToken, localFirstGitHubEnvRepo, localFirstGitHubEnvEncryptionKey)
		return nil
	}
	if !strings.Contains(repo, "/") {
		log.Printf("[local-first] GitHub backup disabled: %s must be owner/repo", localFirstGitHubEnvRepo)
		return nil
	}
	branch := strings.TrimSpace(os.Getenv(localFirstGitHubEnvBranch))
	if branch == "" {
		branch = localFirstGitHubDefaultBranch
	}
	path := strings.Trim(strings.TrimSpace(os.Getenv(localFirstGitHubEnvPath)), "/")
	if path == "" {
		path = localFirstGitHubDefaultPath
	}
	apiURL := strings.TrimRight(strings.TrimSpace(os.Getenv(localFirstGitHubEnvAPIURL)), "/")
	if apiURL == "" {
		apiURL = localFirstGitHubDefaultAPIURL
	}
	return &localFirstGitHubBackup{
		token:         token,
		repo:          repo,
		branch:        branch,
		path:          path,
		encryptionKey: encryptionKey,
		minInterval:   localFirstGitHubMinInterval(),
		apiURL:        apiURL,
		client:        &http.Client{Timeout: 60 * time.Second},
	}
}

func localFirstGitHubEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(localFirstGitHubEnvEnabled))) {
	case "1", "true", "yes", "on", "enabled":
		return true
	default:
		return false
	}
}

func localFirstGitHubMinInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(localFirstGitHubEnvMinInterval))
	if raw == "" {
		return localFirstGitHubDefaultMinInterval
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return localFirstGitHubDefaultMinInterval
	}
	return time.Duration(seconds) * time.Second
}

func (b *localFirstGitHubBackup) Restore(ctx context.Context, ledger *localFirstLedger) error {
	if b == nil || ledger == nil {
		return nil
	}
	encrypted, err := b.download(ctx)
	if err != nil {
		if errors.Is(err, errLocalFirstGitHubBackupNotFound) {
			log.Printf("[local-first] no GitHub backup found at %s/%s", b.repo, b.path)
			return nil
		}
		return err
	}
	plain, err := decryptLocalFirstGitHubPayload(encrypted, b.encryptionKey)
	if err != nil {
		return fmt.Errorf("decrypt GitHub backup: %w", err)
	}
	var snapshot localFirstBackupSnapshot
	if err := json.Unmarshal(plain, &snapshot); err != nil {
		return fmt.Errorf("decode GitHub backup snapshot: %w", err)
	}
	restoredUsage, restoredBilling, err := ledger.restoreBackupSnapshot(ctx, &snapshot)
	if err != nil {
		return err
	}
	log.Printf("[local-first] restored GitHub backup: usage=%d billing=%d created_at=%s", restoredUsage, restoredBilling, snapshot.CreatedAt.Format(time.RFC3339))
	return nil
}

func (b *localFirstGitHubBackup) Backup(ctx context.Context, ledger *localFirstLedger, reason string) error {
	if b == nil || ledger == nil {
		return nil
	}
	if !b.shouldBackupNow() {
		return nil
	}
	snapshot, err := ledger.buildBackupSnapshot(ctx, reason)
	if err != nil {
		return err
	}
	if len(snapshot.Usage) == 0 && len(snapshot.Billing) == 0 {
		return nil
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	encrypted, err := encryptLocalFirstGitHubPayload(raw, b.encryptionKey)
	if err != nil {
		return err
	}
	if err := b.upload(ctx, encrypted, snapshot); err != nil {
		return err
	}
	b.markBackedUp()
	log.Printf("[local-first] backed up unsynced ledger to GitHub: usage=%d billing=%d reason=%s", len(snapshot.Usage), len(snapshot.Billing), reason)
	return nil
}

func (b *localFirstGitHubBackup) shouldBackupNow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lastBackupAt.IsZero() {
		return true
	}
	return time.Since(b.lastBackupAt) >= b.minInterval
}

func (b *localFirstGitHubBackup) markBackedUp() {
	b.mu.Lock()
	b.lastBackupAt = time.Now()
	b.mu.Unlock()
}

func (l *localFirstLedger) buildBackupSnapshot(ctx context.Context, reason string) (*localFirstBackupSnapshot, error) {
	if l == nil || l.db == nil {
		return &localFirstBackupSnapshot{}, nil
	}
	snapshot := &localFirstBackupSnapshot{
		Version:   localFirstGitHubSnapshotVersion,
		CreatedAt: time.Now().UTC(),
		Reason:    reason,
	}
	if err := l.collectUsageBackupEvents(ctx, snapshot); err != nil {
		return nil, err
	}
	if err := l.collectBillingBackupEvents(ctx, snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (l *localFirstLedger) collectUsageBackupEvents(ctx context.Context, snapshot *localFirstBackupSnapshot) error {
	rows, err := l.db.QueryContext(ctx, `
		SELECT request_id, api_key_id, payload, synced, created_at, COALESCE(last_error, ''), COALESCE(last_attempt_at, ''), COALESCE(failed_at, '')
		FROM local_usage_events
		WHERE synced <> ?
		ORDER BY id ASC
	`, localFirstStatusSynced)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ev localFirstUsageBackupEvent
		if err := rows.Scan(&ev.RequestID, &ev.APIKeyID, &ev.Payload, &ev.PreviousStatus, &ev.CreatedAt, &ev.LastError, &ev.LastAttemptAt, &ev.FailedAt); err != nil {
			return err
		}
		var usage service.UsageLog
		if err := json.Unmarshal([]byte(ev.Payload), &usage); err != nil {
			log.Printf("[local-first] skip invalid usage event in GitHub backup request_id=%s: %v", ev.RequestID, err)
			continue
		}
		snapshot.Usage = append(snapshot.Usage, ev)
	}
	return rows.Err()
}

func (l *localFirstLedger) collectBillingBackupEvents(ctx context.Context, snapshot *localFirstBackupSnapshot) error {
	rows, err := l.db.QueryContext(ctx, `
		SELECT request_id, api_key_id, user_id, fingerprint, payload, synced, created_at, COALESCE(last_error, ''), COALESCE(last_attempt_at, ''), COALESCE(failed_at, '')
		FROM local_billing_events
		WHERE synced <> ?
		ORDER BY id ASC
	`, localFirstStatusSynced)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var ev localFirstBillingBackupEvent
		if err := rows.Scan(&ev.RequestID, &ev.APIKeyID, &ev.UserID, &ev.Fingerprint, &ev.Payload, &ev.PreviousStatus, &ev.CreatedAt, &ev.LastError, &ev.LastAttemptAt, &ev.FailedAt); err != nil {
			return err
		}
		var cmd service.UsageBillingCommand
		if err := json.Unmarshal([]byte(ev.Payload), &cmd); err != nil {
			log.Printf("[local-first] skip invalid billing event in GitHub backup request_id=%s: %v", ev.RequestID, err)
			continue
		}
		snapshot.Billing = append(snapshot.Billing, ev)
	}
	return rows.Err()
}

func (l *localFirstLedger) restoreBackupSnapshot(ctx context.Context, snapshot *localFirstBackupSnapshot) (int, int, error) {
	if l == nil || l.db == nil || snapshot == nil {
		return 0, 0, nil
	}
	if snapshot.Version != localFirstGitHubSnapshotVersion {
		return 0, 0, fmt.Errorf("unsupported local-first backup version %d", snapshot.Version)
	}
	tx, err := l.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	commit := false
	defer func() {
		if !commit {
			_ = tx.Rollback()
		}
	}()

	usageCount := 0
	for _, ev := range snapshot.Usage {
		if ev.RequestID == "" || ev.APIKeyID == 0 || ev.Payload == "" {
			continue
		}
		var usage service.UsageLog
		if err := json.Unmarshal([]byte(ev.Payload), &usage); err != nil {
			continue
		}
		res, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO local_usage_events (request_id, api_key_id, payload, synced, created_at, attempt_count, last_error, last_attempt_at, failed_at)
			VALUES (?, ?, ?, ?, COALESCE(NULLIF(?, ''), CURRENT_TIMESTAMP), 0, ?, NULLIF(?, ''), NULLIF(?, ''))
		`, ev.RequestID, ev.APIKeyID, ev.Payload, localFirstStatusPending, ev.CreatedAt, ev.LastError, ev.LastAttemptAt, ev.FailedAt)
		if err != nil {
			return 0, 0, err
		}
		affected, _ := res.RowsAffected()
		usageCount += int(affected)
	}

	billingCount := 0
	for _, ev := range snapshot.Billing {
		if ev.RequestID == "" || ev.APIKeyID == 0 || ev.Payload == "" || ev.Fingerprint == "" {
			continue
		}
		var cmd service.UsageBillingCommand
		if err := json.Unmarshal([]byte(ev.Payload), &cmd); err != nil {
			continue
		}
		res, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO local_billing_events (request_id, api_key_id, user_id, fingerprint, payload, synced, created_at, attempt_count, last_error, last_attempt_at, failed_at)
			VALUES (?, ?, ?, ?, ?, ?, COALESCE(NULLIF(?, ''), CURRENT_TIMESTAMP), 0, ?, NULLIF(?, ''), NULLIF(?, ''))
		`, ev.RequestID, ev.APIKeyID, ev.UserID, ev.Fingerprint, ev.Payload, localFirstStatusPending, ev.CreatedAt, ev.LastError, ev.LastAttemptAt, ev.FailedAt)
		if err != nil {
			return 0, 0, err
		}
		affected, _ := res.RowsAffected()
		billingCount += int(affected)
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	commit = true
	return usageCount, billingCount, nil
}

var errLocalFirstGitHubBackupNotFound = errors.New("GitHub backup not found")

type githubContentsResponse struct {
	SHA      string `json:"sha"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

func (b *localFirstGitHubBackup) download(ctx context.Context) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.contentsURL()+"?ref="+url.QueryEscape(b.branch), nil)
	if err != nil {
		return nil, err
	}
	b.setGitHubHeaders(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, errLocalFirstGitHubBackupNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("GitHub download failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out githubContentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	if out.Encoding != "" && out.Encoding != "base64" {
		return nil, fmt.Errorf("unsupported GitHub content encoding %q", out.Encoding)
	}
	content := strings.NewReplacer("\n", "", "\r", "").Replace(out.Content)
	decoded, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func (b *localFirstGitHubBackup) upload(ctx context.Context, encrypted []byte, snapshot *localFirstBackupSnapshot) error {
	sha, err := b.currentSHA(ctx)
	if err != nil && !errors.Is(err, errLocalFirstGitHubBackupNotFound) {
		return err
	}
	payload := map[string]any{
		"message": fmt.Sprintf("backup local billing ledger %s", snapshot.CreatedAt.Format(time.RFC3339)),
		"branch":  b.branch,
		"content": base64.StdEncoding.EncodeToString(encrypted),
	}
	if sha != "" {
		payload["sha"] = sha
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, b.contentsURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	b.setGitHubHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("GitHub upload failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (b *localFirstGitHubBackup) currentSHA(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.contentsURL()+"?ref="+url.QueryEscape(b.branch), nil)
	if err != nil {
		return "", err
	}
	b.setGitHubHeaders(req)
	resp, err := b.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return "", errLocalFirstGitHubBackupNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("GitHub sha lookup failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out githubContentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.SHA, nil
}

func (b *localFirstGitHubBackup) contentsURL() string {
	return b.apiURL + "/repos/" + b.repo + "/contents/" + escapeGitHubPath(b.path)
}

func escapeGitHubPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func (b *localFirstGitHubBackup) setGitHubHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+b.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "sub2api-local-first-ledger")
}

func encryptLocalFirstGitHubPayload(plain []byte, secret string) ([]byte, error) {
	compressed, err := gzipBytes(plain)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(deriveLocalFirstGitHubKey(secret))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, compressed, nil)
	out := make([]byte, 0, len(localFirstGitHubCipherMagic)+len(nonce)+len(ciphertext))
	out = append(out, []byte(localFirstGitHubCipherMagic)...)
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

func decryptLocalFirstGitHubPayload(encrypted []byte, secret string) ([]byte, error) {
	if len(encrypted) < len(localFirstGitHubCipherMagic) {
		return nil, errors.New("encrypted backup too short")
	}
	if string(encrypted[:len(localFirstGitHubCipherMagic)]) != localFirstGitHubCipherMagic {
		return nil, errors.New("unsupported encrypted backup format")
	}
	block, err := aes.NewCipher(deriveLocalFirstGitHubKey(secret))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	offset := len(localFirstGitHubCipherMagic)
	if len(encrypted) < offset+gcm.NonceSize() {
		return nil, errors.New("encrypted backup missing nonce")
	}
	nonce := encrypted[offset : offset+gcm.NonceSize()]
	ciphertext := encrypted[offset+gcm.NonceSize():]
	compressed, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return gunzipBytes(compressed)
}

func deriveLocalFirstGitHubKey(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

func gzipBytes(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(in); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzipBytes(in []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	return io.ReadAll(zr)
}

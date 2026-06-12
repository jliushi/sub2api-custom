package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
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
)

const (
	adminDataGitHubEnvEnabled       = "ADMIN_DATA_GITHUB_BACKUP_ENABLED"
	adminDataGitHubEnvToken         = "ADMIN_DATA_GITHUB_TOKEN"
	adminDataGitHubEnvRepo          = "ADMIN_DATA_GITHUB_REPO"
	adminDataGitHubEnvBranch        = "ADMIN_DATA_GITHUB_BRANCH"
	adminDataGitHubEnvPrefix        = "ADMIN_DATA_GITHUB_PREFIX"
	adminDataGitHubEnvEncryptionKey = "ADMIN_DATA_GITHUB_ENCRYPTION_KEY"
	adminDataGitHubEnvInterval      = "ADMIN_DATA_GITHUB_INTERVAL_SECONDS"
	adminDataGitHubEnvAPIURL        = "ADMIN_DATA_GITHUB_API_URL"
	adminDataGitHubEnvRunOnStartup  = "ADMIN_DATA_GITHUB_RUN_ON_STARTUP"
	adminDataGitHubEnvTables        = "ADMIN_DATA_GITHUB_TABLES"

	adminDataGitHubFallbackToken         = "LOCAL_BILLING_GITHUB_TOKEN"
	adminDataGitHubFallbackRepo          = "LOCAL_BILLING_GITHUB_REPO"
	adminDataGitHubFallbackBranch        = "LOCAL_BILLING_GITHUB_BRANCH"
	adminDataGitHubFallbackEncryptionKey = "LOCAL_BILLING_GITHUB_ENCRYPTION_KEY"

	adminDataGitHubDefaultBranch   = "main"
	adminDataGitHubDefaultPrefix   = "data-backups"
	adminDataGitHubDefaultAPIURL   = "https://api.github.com"
	adminDataGitHubDefaultInterval = time.Hour
	adminDataGitHubCipherMagic     = "s2apiadb1"
	adminDataGitHubSnapshotVersion = 1
	adminDataGitHubFileName        = "sub2api-admin-data.snapshot.json.gz.enc"
	adminDataGitHubManifestName    = "manifest.json"
)

var errAdminDataGitHubNotFound = errors.New("admin data GitHub backup not found")

var defaultAdminDataGitHubTables = []string{
	"settings",
	"security_secrets",
	"users",
	"groups",
	"proxies",
	"subscription_plans",
	"user_subscriptions",
	"user_attribute_definitions",
	"user_attribute_values",
	"user_allowed_groups",
	"user_platform_quotas",
	"user_affiliates",
	"user_affiliate_ledger",
	"accounts",
	"account_groups",
	"api_keys",
	"channels",
	"channel_groups",
	"channel_model_pricing",
	"channel_model_pricing_intervals",
	"channel_account_stats_pricing_rules",
	"channel_account_stats_model_pricing",
	"channel_account_stats_pricing_intervals",
	"channel_monitors",
	"channel_monitor_request_templates",
	"tls_fingerprint_profiles",
	"error_passthrough_rules",
	"announcements",
	"announcement_reads",
	"redeem_codes",
	"promo_codes",
	"promo_code_usages",
	"payment_provider_instances",
	"payment_orders",
	"payment_audit_logs",
	"auth_identities",
	"auth_identity_channels",
}

var adminDataTableSortColumns = map[string][]string{
	"account_groups": {"account_id", "group_id"},
	"channel_account_stats_pricing_intervals": {"pricing_id", "min_tokens", "max_tokens"},
	"channel_groups":                  {"channel_id", "group_id"},
	"channel_model_pricing_intervals": {"pricing_id", "min_tokens", "max_tokens"},
	"user_allowed_groups":             {"user_id", "group_id"},
}

type AdminDataGitHubBackupService struct {
	db     *sql.DB
	cfg    adminDataGitHubBackupConfig
	client *http.Client

	stopCh chan struct{}
	doneCh chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	runMu     sync.Mutex
}

type adminDataGitHubBackupConfig struct {
	Enabled       bool
	Token         string
	Repo          string
	Branch        string
	Prefix        string
	EncryptionKey string
	Interval      time.Duration
	APIURL        string
	RunOnStartup  bool
	Tables        []string
}

type adminDataBackupSnapshot struct {
	Version   int                    `json:"version"`
	CreatedAt time.Time              `json:"created_at"`
	Source    string                 `json:"source"`
	Tables    []adminDataBackupTable `json:"tables"`
}

type adminDataBackupContent struct {
	Version int                    `json:"version"`
	Tables  []adminDataBackupTable `json:"tables"`
}

type adminDataBackupTable struct {
	Name    string            `json:"name"`
	Columns []string          `json:"columns"`
	Rows    []json.RawMessage `json:"rows"`
}

type AdminDataBackupManifest struct {
	Version          int                            `json:"version"`
	BackupID         string                         `json:"backup_id"`
	CreatedAt        time.Time                      `json:"created_at"`
	Reason           string                         `json:"reason"`
	Format           string                         `json:"format"`
	Compression      string                         `json:"compression"`
	Encryption       string                         `json:"encryption"`
	ContentSHA256    string                         `json:"content_sha256"`
	SnapshotSHA256   string                         `json:"snapshot_sha256"`
	EncryptedSHA256  string                         `json:"encrypted_sha256"`
	EncryptedSize    int                            `json:"encrypted_size"`
	TableCount       int                            `json:"table_count"`
	RowCount         int                            `json:"row_count"`
	Tables           []AdminDataBackupTableManifest `json:"tables"`
	Paths            map[string]string              `json:"paths"`
	SkippedUnchanged bool                           `json:"skipped_unchanged,omitempty"`
}

type AdminDataBackupTableManifest struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Rows    int      `json:"rows"`
}

type adminDataGitHubContentsResponse struct {
	SHA      string `json:"sha"`
	Content  string `json:"content"`
	Encoding string `json:"encoding"`
}

func ProvideAdminDataGitHubBackupService(db *sql.DB) *AdminDataGitHubBackupService {
	cfg := adminDataGitHubBackupConfigFromEnv()
	if !cfg.Enabled {
		return nil
	}
	if db == nil {
		log.Printf("[admin-data-backup] disabled: nil database")
		return nil
	}
	if missing := cfg.missingFields(); len(missing) > 0 {
		log.Printf("[admin-data-backup] disabled: missing %s", strings.Join(missing, ", "))
		return nil
	}
	svc := NewAdminDataGitHubBackupService(db, cfg)
	svc.Start()
	return svc
}

func NewAdminDataGitHubBackupService(db *sql.DB, cfg adminDataGitHubBackupConfig) *AdminDataGitHubBackupService {
	return &AdminDataGitHubBackupService{
		db:     db,
		cfg:    cfg,
		client: &http.Client{Timeout: 60 * time.Second},
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

func adminDataGitHubBackupConfigFromEnv() adminDataGitHubBackupConfig {
	return adminDataGitHubBackupConfig{
		Enabled:       adminDataEnvEnabled(adminDataGitHubEnvEnabled, false),
		Token:         adminDataEnvFallback(adminDataGitHubEnvToken, adminDataGitHubFallbackToken),
		Repo:          adminDataEnvFallback(adminDataGitHubEnvRepo, adminDataGitHubFallbackRepo),
		Branch:        adminDataDefaultString(adminDataEnvFallback(adminDataGitHubEnvBranch, adminDataGitHubFallbackBranch), adminDataGitHubDefaultBranch),
		Prefix:        strings.Trim(adminDataDefaultString(os.Getenv(adminDataGitHubEnvPrefix), adminDataGitHubDefaultPrefix), "/"),
		EncryptionKey: adminDataEnvFallback(adminDataGitHubEnvEncryptionKey, adminDataGitHubFallbackEncryptionKey),
		Interval:      adminDataEnvDuration(adminDataGitHubEnvInterval, adminDataGitHubDefaultInterval),
		APIURL:        strings.TrimRight(adminDataDefaultString(os.Getenv(adminDataGitHubEnvAPIURL), adminDataGitHubDefaultAPIURL), "/"),
		RunOnStartup:  adminDataEnvEnabled(adminDataGitHubEnvRunOnStartup, true),
		Tables:        adminDataEnvTables(),
	}
}

func (c adminDataGitHubBackupConfig) missingFields() []string {
	var missing []string
	if strings.TrimSpace(c.Token) == "" {
		missing = append(missing, adminDataGitHubEnvToken)
	}
	if strings.TrimSpace(c.Repo) == "" || !strings.Contains(c.Repo, "/") {
		missing = append(missing, adminDataGitHubEnvRepo)
	}
	if strings.TrimSpace(c.EncryptionKey) == "" {
		missing = append(missing, adminDataGitHubEnvEncryptionKey)
	}
	if strings.TrimSpace(c.Branch) == "" {
		missing = append(missing, adminDataGitHubEnvBranch)
	}
	if strings.TrimSpace(c.Prefix) == "" {
		missing = append(missing, adminDataGitHubEnvPrefix)
	}
	if strings.TrimSpace(c.APIURL) == "" {
		missing = append(missing, adminDataGitHubEnvAPIURL)
	}
	if len(c.Tables) == 0 {
		missing = append(missing, adminDataGitHubEnvTables)
	}
	return missing
}

func adminDataEnvFallback(primary, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(primary)); value != "" {
		return value
	}
	return strings.TrimSpace(os.Getenv(fallback))
}

func adminDataDefaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func adminDataEnvEnabled(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	switch strings.ToLower(raw) {
	case "1", "true", "yes", "on", "enabled":
		return true
	case "0", "false", "no", "off", "disabled":
		return false
	default:
		return fallback
	}
}

func adminDataEnvDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func adminDataEnvTables() []string {
	raw := strings.TrimSpace(os.Getenv(adminDataGitHubEnvTables))
	if raw == "" {
		return append([]string(nil), defaultAdminDataGitHubTables...)
	}
	parts := strings.Split(raw, ",")
	tables := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		table := strings.TrimSpace(part)
		if table == "" {
			continue
		}
		if !isSafeAdminDataIdentifier(table) {
			log.Printf("[admin-data-backup] ignoring unsafe table name from %s: %q", adminDataGitHubEnvTables, table)
			continue
		}
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		tables = append(tables, table)
	}
	return tables
}

func (s *AdminDataGitHubBackupService) Start() {
	if s == nil {
		return
	}
	s.startOnce.Do(func() {
		go s.run()
	})
}

func (s *AdminDataGitHubBackupService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopCh)
		select {
		case <-s.doneCh:
		case <-time.After(30 * time.Second):
			log.Printf("[admin-data-backup] stop timeout")
		}
	})
}

func (s *AdminDataGitHubBackupService) run() {
	defer close(s.doneCh)
	log.Printf("[admin-data-backup] enabled, repo=%s prefix=%s interval=%s", s.cfg.Repo, s.cfg.Prefix, s.cfg.Interval)

	if s.cfg.RunOnStartup {
		s.runOnceWithTimeout("startup")
	}

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.runOnceWithTimeout("scheduled")
		case <-s.stopCh:
			return
		}
	}
}

func (s *AdminDataGitHubBackupService) runOnceWithTimeout(reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), minAdminDataDuration(s.cfg.Interval/2, 30*time.Minute))
	defer cancel()
	if _, err := s.BackupNow(ctx, reason); err != nil {
		log.Printf("[admin-data-backup] %s backup failed: %v", reason, err)
	}
}

func minAdminDataDuration(a, b time.Duration) time.Duration {
	if a <= 0 || a > b {
		return b
	}
	return a
}

func (s *AdminDataGitHubBackupService) BackupNow(ctx context.Context, reason string) (*AdminDataBackupManifest, error) {
	if s == nil {
		return nil, nil
	}
	if !s.runMu.TryLock() {
		log.Printf("[admin-data-backup] skipped %s backup: previous run still active", reason)
		return nil, nil
	}
	defer s.runMu.Unlock()

	now := time.Now().UTC()
	snapshot, tableManifests, contentSHA, err := s.buildSnapshot(ctx, now)
	if err != nil {
		return nil, err
	}
	latestManifest, err := s.downloadManifest(ctx, s.latestManifestPath())
	if err != nil && !errors.Is(err, errAdminDataGitHubNotFound) {
		return nil, err
	}
	if latestManifest != nil && latestManifest.ContentSHA256 == contentSHA {
		latestManifest.SkippedUnchanged = true
		log.Printf("[admin-data-backup] unchanged, skipped GitHub commit: content_sha256=%s rows=%d", contentSHA, latestManifest.RowCount)
		return latestManifest, nil
	}

	snapshotBytes, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	encrypted, err := encryptAdminDataGitHubPayload(snapshotBytes, s.cfg.EncryptionKey)
	if err != nil {
		return nil, err
	}

	backupID := now.Format("20060102T150405Z")
	timestampedDataPath := s.timestampedDataPath(backupID)
	timestampedManifestPath := s.timestampedManifestPath(backupID)
	manifest := &AdminDataBackupManifest{
		Version:         adminDataGitHubSnapshotVersion,
		BackupID:        backupID,
		CreatedAt:       now,
		Reason:          reason,
		Format:          "sub2api-admin-data-v1",
		Compression:     "gzip",
		Encryption:      "AES-256-GCM; key=sha256(ADMIN_DATA_GITHUB_ENCRYPTION_KEY); magic=" + adminDataGitHubCipherMagic,
		ContentSHA256:   contentSHA,
		SnapshotSHA256:  sha256Hex(snapshotBytes),
		EncryptedSHA256: sha256Hex(encrypted),
		EncryptedSize:   len(encrypted),
		TableCount:      len(tableManifests),
		RowCount:        adminDataRowCount(tableManifests),
		Tables:          tableManifests,
		Paths: map[string]string{
			"timestamped_data":     timestampedDataPath,
			"timestamped_manifest": timestampedManifestPath,
			"latest_data":          s.latestDataPath(),
			"latest_manifest":      s.latestManifestPath(),
		},
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, err
	}

	message := fmt.Sprintf("backup admin data %s", backupID)
	if err := s.uploadFile(ctx, timestampedDataPath, encrypted, message); err != nil {
		return nil, err
	}
	if err := s.uploadFile(ctx, timestampedManifestPath, manifestBytes, message); err != nil {
		return nil, err
	}
	if err := s.uploadFile(ctx, s.latestDataPath(), encrypted, message); err != nil {
		return nil, err
	}
	if err := s.uploadFile(ctx, s.latestManifestPath(), manifestBytes, message); err != nil {
		return nil, err
	}

	log.Printf("[admin-data-backup] uploaded GitHub backup: id=%s tables=%d rows=%d encrypted_size=%d content_sha256=%s", backupID, manifest.TableCount, manifest.RowCount, manifest.EncryptedSize, manifest.ContentSHA256)
	return manifest, nil
}

func (s *AdminDataGitHubBackupService) buildSnapshot(ctx context.Context, now time.Time) (*adminDataBackupSnapshot, []AdminDataBackupTableManifest, string, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, nil, "", err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	snapshot := &adminDataBackupSnapshot{
		Version:   adminDataGitHubSnapshotVersion,
		CreatedAt: now,
		Source:    "postgresql",
		Tables:    make([]adminDataBackupTable, 0, len(s.cfg.Tables)),
	}
	manifests := make([]AdminDataBackupTableManifest, 0, len(s.cfg.Tables))
	for _, tableName := range s.cfg.Tables {
		table, err := collectAdminDataTable(ctx, tx, tableName)
		if err != nil {
			return nil, nil, "", err
		}
		if table == nil {
			continue
		}
		snapshot.Tables = append(snapshot.Tables, *table)
		manifests = append(manifests, AdminDataBackupTableManifest{
			Name:    table.Name,
			Columns: append([]string(nil), table.Columns...),
			Rows:    len(table.Rows),
		})
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, "", err
	}
	committed = true

	contentBytes, err := json.Marshal(adminDataBackupContent{
		Version: adminDataGitHubSnapshotVersion,
		Tables:  snapshot.Tables,
	})
	if err != nil {
		return nil, nil, "", err
	}
	return snapshot, manifests, sha256Hex(contentBytes), nil
}

func collectAdminDataTable(ctx context.Context, tx *sql.Tx, tableName string) (*adminDataBackupTable, error) {
	if !isSafeAdminDataIdentifier(tableName) {
		return nil, fmt.Errorf("unsafe table name %q", tableName)
	}
	columns, err := adminDataTableColumns(ctx, tx, tableName)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, nil
	}

	query := adminDataSelectRowsSQL(tableName, columns)
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query table %s: %w", tableName, err)
	}
	defer func() { _ = rows.Close() }()

	table := &adminDataBackupTable{
		Name:    tableName,
		Columns: append([]string(nil), columns...),
	}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan table %s: %w", tableName, err)
		}
		table.Rows = append(table.Rows, append(json.RawMessage(nil), raw...))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate table %s: %w", tableName, err)
	}
	return table, nil
}

func adminDataTableColumns(ctx context.Context, tx *sql.Tx, tableName string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position
	`, tableName)
	if err != nil {
		return nil, fmt.Errorf("load columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, err
		}
		if isSafeAdminDataIdentifier(column) {
			columns = append(columns, column)
		}
	}
	return columns, rows.Err()
}

func adminDataSelectRowsSQL(tableName string, columns []string) string {
	quotedColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteAdminDataIdentifier(column))
	}
	orderBy := adminDataOrderBySQL(tableName, columns)
	if orderBy != "" {
		orderBy = " ORDER BY " + orderBy
	}
	return fmt.Sprintf(
		"SELECT row_to_json(_admin_data_row) FROM (SELECT %s FROM public.%s%s) AS _admin_data_row",
		strings.Join(quotedColumns, ", "),
		quoteAdminDataIdentifier(tableName),
		orderBy,
	)
}

func adminDataOrderBySQL(tableName string, columns []string) string {
	columnSet := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		columnSet[column] = struct{}{}
	}
	if _, ok := columnSet["id"]; ok {
		return quoteAdminDataIdentifier("id")
	}
	sortColumns := adminDataTableSortColumns[tableName]
	parts := make([]string, 0, len(sortColumns))
	for _, column := range sortColumns {
		if _, ok := columnSet[column]; ok {
			parts = append(parts, quoteAdminDataIdentifier(column))
		}
	}
	return strings.Join(parts, ", ")
}

func isSafeAdminDataIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' && i > 0 || r == '_' {
			continue
		}
		return false
	}
	return true
}

func quoteAdminDataIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func adminDataRowCount(tables []AdminDataBackupTableManifest) int {
	total := 0
	for _, table := range tables {
		total += table.Rows
	}
	return total
}

func (s *AdminDataGitHubBackupService) latestDataPath() string {
	return s.cfg.Prefix + "/latest/" + adminDataGitHubFileName
}

func (s *AdminDataGitHubBackupService) latestManifestPath() string {
	return s.cfg.Prefix + "/latest/" + adminDataGitHubManifestName
}

func (s *AdminDataGitHubBackupService) timestampedDataPath(backupID string) string {
	return s.cfg.Prefix + "/" + backupID + "/" + adminDataGitHubFileName
}

func (s *AdminDataGitHubBackupService) timestampedManifestPath(backupID string) string {
	return s.cfg.Prefix + "/" + backupID + "/" + adminDataGitHubManifestName
}

func (s *AdminDataGitHubBackupService) downloadManifest(ctx context.Context, path string) (*AdminDataBackupManifest, error) {
	content, _, err := s.downloadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	content = bytes.TrimPrefix(content, []byte{0xEF, 0xBB, 0xBF})
	var manifest AdminDataBackupManifest
	if err := json.Unmarshal(content, &manifest); err != nil {
		log.Printf("[admin-data-backup] ignoring incompatible GitHub manifest %s: %v", path, err)
		return nil, errAdminDataGitHubNotFound
	}
	return &manifest, nil
}

func (s *AdminDataGitHubBackupService) downloadFile(ctx context.Context, path string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.contentsURL(path)+"?ref="+url.QueryEscape(s.cfg.Branch), nil)
	if err != nil {
		return nil, "", err
	}
	s.setGitHubHeaders(req)
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotFound {
		return nil, "", errAdminDataGitHubNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, "", fmt.Errorf("GitHub download failed path=%s status=%d body=%s", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out adminDataGitHubContentsResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, "", err
	}
	if out.Encoding != "" && out.Encoding != "base64" {
		return nil, "", fmt.Errorf("unsupported GitHub content encoding %q", out.Encoding)
	}
	content := strings.NewReplacer("\n", "", "\r", "").Replace(out.Content)
	decoded, err := base64.StdEncoding.DecodeString(content)
	if err != nil {
		return nil, "", err
	}
	return decoded, out.SHA, nil
}

func (s *AdminDataGitHubBackupService) uploadFile(ctx context.Context, path string, content []byte, message string) error {
	_, sha, err := s.downloadFile(ctx, path)
	if err != nil && !errors.Is(err, errAdminDataGitHubNotFound) {
		return err
	}
	payload := map[string]any{
		"message": message,
		"branch":  s.cfg.Branch,
		"content": base64.StdEncoding.EncodeToString(content),
	}
	if sha != "" {
		payload["sha"] = sha
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.contentsURL(path), bytes.NewReader(body))
	if err != nil {
		return err
	}
	s.setGitHubHeaders(req)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("GitHub upload failed path=%s status=%d body=%s", path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (s *AdminDataGitHubBackupService) contentsURL(path string) string {
	return s.cfg.APIURL + "/repos/" + s.cfg.Repo + "/contents/" + escapeAdminDataGitHubPath(path)
}

func escapeAdminDataGitHubPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func (s *AdminDataGitHubBackupService) setGitHubHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "sub2api-admin-data-backup")
}

func encryptAdminDataGitHubPayload(plain []byte, secret string) ([]byte, error) {
	compressed, err := gzipAdminDataBytes(plain)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(deriveAdminDataGitHubKey(secret))
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
	out := make([]byte, 0, len(adminDataGitHubCipherMagic)+len(nonce)+len(ciphertext))
	out = append(out, []byte(adminDataGitHubCipherMagic)...)
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

func decryptAdminDataGitHubPayload(encrypted []byte, secret string) ([]byte, error) {
	if len(encrypted) < len(adminDataGitHubCipherMagic) {
		return nil, errors.New("encrypted admin data backup too short")
	}
	if string(encrypted[:len(adminDataGitHubCipherMagic)]) != adminDataGitHubCipherMagic {
		return nil, errors.New("unsupported encrypted admin data backup format")
	}
	block, err := aes.NewCipher(deriveAdminDataGitHubKey(secret))
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	offset := len(adminDataGitHubCipherMagic)
	if len(encrypted) < offset+gcm.NonceSize() {
		return nil, errors.New("encrypted admin data backup missing nonce")
	}
	nonce := encrypted[offset : offset+gcm.NonceSize()]
	ciphertext := encrypted[offset+gcm.NonceSize():]
	compressed, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}
	return gunzipAdminDataBytes(compressed)
}

func deriveAdminDataGitHubKey(secret string) []byte {
	sum := sha256.Sum256([]byte(secret))
	return sum[:]
}

func gzipAdminDataBytes(in []byte) ([]byte, error) {
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

func gunzipAdminDataBytes(in []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer func() { _ = zr.Close() }()
	return io.ReadAll(zr)
}

func sha256Hex(in []byte) string {
	sum := sha256.Sum256(in)
	return hex.EncodeToString(sum[:])
}

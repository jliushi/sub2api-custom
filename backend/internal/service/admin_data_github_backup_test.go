package service

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdminDataGitHubBackupEncryptRoundTrip(t *testing.T) {
	plain := []byte(`{"version":1,"tables":[{"name":"users","rows":[{"id":1}]}]}`)
	secret := "test-admin-data-secret"

	encrypted, err := encryptAdminDataGitHubPayload(plain, secret)
	require.NoError(t, err)
	require.NotContains(t, string(encrypted), string(plain))

	restored, err := decryptAdminDataGitHubPayload(encrypted, secret)
	require.NoError(t, err)
	require.Equal(t, plain, restored)

	_, err = decryptAdminDataGitHubPayload(encrypted, "different-secret")
	require.Error(t, err)
}

func TestAdminDataBackupContentHashIgnoresCreatedAt(t *testing.T) {
	tables := []adminDataBackupTable{
		{
			Name:    "users",
			Columns: []string{"id", "email"},
			Rows: []json.RawMessage{
				json.RawMessage(`{"id":1,"email":"a@example.test"}`),
			},
		},
	}
	first := adminDataBackupSnapshot{
		Version:   adminDataGitHubSnapshotVersion,
		CreatedAt: time.Date(2026, 6, 8, 1, 0, 0, 0, time.UTC),
		Source:    "postgresql",
		Tables:    tables,
	}
	second := first
	second.CreatedAt = first.CreatedAt.Add(time.Hour)

	firstContent, err := json.Marshal(adminDataBackupContent{Version: first.Version, Tables: first.Tables})
	require.NoError(t, err)
	secondContent, err := json.Marshal(adminDataBackupContent{Version: second.Version, Tables: second.Tables})
	require.NoError(t, err)
	require.Equal(t, sha256Hex(firstContent), sha256Hex(secondContent))

	firstSnapshot, err := json.Marshal(first)
	require.NoError(t, err)
	secondSnapshot, err := json.Marshal(second)
	require.NoError(t, err)
	require.NotEqual(t, sha256Hex(firstSnapshot), sha256Hex(secondSnapshot))
}

func TestAdminDataSelectRowsSQLOrdersCompositeTables(t *testing.T) {
	sql := adminDataSelectRowsSQL("account_groups", []string{"account_id", "group_id", "priority"})
	require.Contains(t, sql, `FROM public."account_groups"`)
	require.Contains(t, sql, `ORDER BY "account_id", "group_id"`)
}

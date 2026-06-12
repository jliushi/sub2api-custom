//go:build unit

package service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type authCacheTimeoutError struct{}

func (authCacheTimeoutError) Error() string   { return "network timeout" }
func (authCacheTimeoutError) Timeout() bool   { return true }
func (authCacheTimeoutError) Temporary() bool { return true }

func TestIsTimeoutOrConnectionError_RecognizesTransientDBFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "wrapped deadline", err: fmt.Errorf("query api key: %w", context.DeadlineExceeded)},
		{name: "net timeout", err: authCacheTimeoutError{}},
		{name: "windows refused", err: errors.New("dial tcp: connectex: No connection could be made because the target machine actively refused it")},
		{name: "postgres too many connections", err: errors.New("ERROR: sorry, too many clients already (SQLSTATE 53300)")},
		{name: "postgres shutting down", err: errors.New("server closed the connection unexpectedly SQLSTATE 57P01")},
		{name: "sqlite lock", err: errors.New("database is locked")},
		{name: "disk full", err: errors.New("write failed: no space left on device")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.True(t, isTimeoutOrConnectionError(tt.err))
		})
	}
}

func TestIsTimeoutOrConnectionError_IgnoresApplicationErrors(t *testing.T) {
	require.False(t, isTimeoutOrConnectionError(nil))
	require.False(t, isTimeoutOrConnectionError(context.Canceled))
	require.False(t, isTimeoutOrConnectionError(errors.New("unique constraint violation")))
}

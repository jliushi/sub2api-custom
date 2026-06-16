package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

// fallbackTestScheduler 是一个可注入的 OpenAIAccountScheduler 桩，
// 让 Select 返回预设错误，用于验证 selectAccountWithScheduler 在
// 「快照/基础设施错误」时回退到直连 DB 的普通选路。
type fallbackTestScheduler struct {
	selectErr error
	selectCnt int
	decision  OpenAIAccountScheduleDecision
	selection *AccountSelectionResult
}

func (f *fallbackTestScheduler) Select(ctx context.Context, req OpenAIAccountScheduleRequest) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
	f.selectCnt++
	if f.selectErr != nil {
		return nil, f.decision, f.selectErr
	}
	return f.selection, f.decision, nil
}

func (f *fallbackTestScheduler) ReportResult(int64, bool, *int) {}
func (f *fallbackTestScheduler) ReportSwitch()                  {}
func (f *fallbackTestScheduler) SnapshotMetrics() OpenAIAccountSchedulerMetricsSnapshot {
	return OpenAIAccountSchedulerMetricsSnapshot{}
}

// ctxAwareOpenAIAccountRepo 在 ctx 取消时返回 ctx.Err()，用于验证自愈重建脱离请求 ctx。
type ctxAwareOpenAIAccountRepo struct {
	schedulerTestOpenAIAccountRepo
}

func (r ctxAwareOpenAIAccountRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, groupID int64, platform string) ([]Account, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return r.schedulerTestOpenAIAccountRepo.ListSchedulableByGroupIDAndPlatform(ctx, groupID, platform)
}

func newFallbackTestService(t *testing.T, scheduler OpenAIAccountScheduler, accounts []Account) *OpenAIGatewayService {
	t.Helper()
	accountRepo := schedulerTestOpenAIAccountRepo{accounts: accounts}
	accountsByID := make(map[int64]*Account, len(accounts))
	for i := range accounts {
		cloned := accounts[i]
		accountsByID[cloned.ID] = &cloned
	}
	snapshotCache := &openAISnapshotCacheStub{accountsByID: accountsByID}
	snapshotService := &SchedulerSnapshotService{cache: snapshotCache}
	svc := &OpenAIGatewayService{
		accountRepo:        accountRepo,
		cache:              &schedulerTestGatewayCache{},
		cfg:                &config.Config{},
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
		schedulerSnapshot:  snapshotService,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
		openaiAccountStats: newOpenAIAccountRuntimeStats(),
	}
	// 注入桩调度器：getOpenAIAccountScheduler 的 once.Do 见非 nil 不会覆盖。
	svc.openaiScheduler = scheduler
	return svc
}

func fallbackTestAccount(id int64) Account {
	return Account{
		ID:          id,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Priority:    1,
	}
}

// 基础设施错误（context.Canceled）且父 ctx 仍存活 → 回退直连 DB 选路成功。
func TestOpenAIGatewayService_SelectAccountWithScheduler_InfraErrorFallsBackToDirectDB(t *testing.T) {
	groupID := int64(30301)
	stub := &fallbackTestScheduler{selectErr: context.Canceled}
	svc := newFallbackTestService(t, stub, []Account{fallbackTestAccount(40401)})

	selection, decision, err := svc.SelectAccountWithScheduler(context.Background(), &groupID, "", "session_infra", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.NotNil(t, selection.Account)
	require.Equal(t, int64(40401), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.Equal(t, 1, stub.selectCnt, "fallback 不应再次调用快照调度器")
}

// ErrSchedulerCacheNotReady 同样触发直连 DB 回退。
func TestOpenAIGatewayService_SelectAccountWithScheduler_CacheNotReadyFallsBackToDirectDB(t *testing.T) {
	groupID := int64(30302)
	stub := &fallbackTestScheduler{selectErr: ErrSchedulerCacheNotReady}
	svc := newFallbackTestService(t, stub, []Account{fallbackTestAccount(40402)})

	selection, _, err := svc.SelectAccountWithScheduler(context.Background(), &groupID, "", "session_cache", "gpt-5.5", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.NotNil(t, selection.Account)
	require.Equal(t, int64(40402), selection.Account.ID)
}

// 父请求 ctx 已取消 → 原样返回错误，不做 DB 回退（客户端已离开）。
func TestOpenAIGatewayService_SelectAccountWithScheduler_ParentCtxCanceledReturnsError(t *testing.T) {
	groupID := int64(30303)
	stub := &fallbackTestScheduler{selectErr: context.Canceled}
	svc := newFallbackTestService(t, stub, []Account{fallbackTestAccount(40403)})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "session_dead", "gpt-5.4", nil, OpenAIUpstreamTransportAny, false)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, selection)
	require.Equal(t, 1, stub.selectCnt, "父 ctx 取消时不应触发回退选路")
}

// 自愈/重建脱离请求 ctx：即便传入已取消的 ctx，bucket 重建仍应基于 WithoutCancel 成功执行。
func TestSchedulerSnapshotService_RefreshOpenAIBucketAfterNoAvailable_SurvivesRequestCancellation(t *testing.T) {
	accountRepo := ctxAwareOpenAIAccountRepo{
		schedulerTestOpenAIAccountRepo: schedulerTestOpenAIAccountRepo{
			accounts: []Account{fallbackTestAccount(40404)},
		},
	}
	snapshotCache := &openAISnapshotCacheStub{accountsByID: map[int64]*Account{}}
	snapshotService := NewSchedulerSnapshotService(snapshotCache, nil, accountRepo, nil, &config.Config{})

	groupID := int64(30304)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	refreshed, err := snapshotService.RefreshOpenAIBucketAfterNoAvailable(ctx, &groupID, "test_cancel")
	require.NoError(t, err)
	require.True(t, refreshed)
	require.Len(t, snapshotCache.snapshotAccounts, 1, "重建应在脱离取消的 ctx 下成功写入快照")
}

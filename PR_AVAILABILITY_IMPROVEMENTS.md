# 可用性改进：熔断器和超时控制

## 问题描述

### 生产事故
本周发生 **2 次因上游 Supabase 失效 10 秒导致整个 sub2api 服务完全不可用**的严重事故。

### 根本原因
1. **认证缓存失效时强依赖 DB 查询**，无降级策略
2. **DB 查询无超时限制**，Supabase 10秒超时导致所有请求阻塞
3. **singleflight 成为故障放大器**，一个请求失败导致所有等待的请求同时失败
4. **SQLite 单连接瓶颈**，高并发时性能差

### 故障链路
```
上游 Supabase 失效 10 秒
    ↓
缓存未命中的请求调用 loadAuthCacheEntry()
    ↓
GetByKeyForAuth() 阻塞等待 DB 响应（无超时）
    ↓
singleflight 合并的所有请求一起等待
    ↓
10 秒后 Supabase 超时，所有请求同时失败
    ↓
新请求继续触发查询，继续失败
    ↓
整个服务停止响应
```

## 解决方案

### 1. 熔断器实现 (Circuit Breaker)

**文件：** `backend/internal/pkg/circuitbreaker/breaker.go`

**功能：**
- 三种状态：Closed (正常) → Open (熔断) → HalfOpen (测试恢复)
- 连续失败 5 次后打开熔断器
- 熔断 30 秒后进入半开状态
- 半开状态下允许 3 个测试请求
- 测试成功 2 次后关闭熔断器

**效果：**
- 防止持续向故障的 DB 发送请求
- 给 DB 恢复的时间窗口
- 自动测试 DB 是否恢复

### 2. 查询超时控制

**文件：** `backend/internal/service/api_key_auth_cache_impl.go`

**改动：**
```go
// 添加 500ms 超时
queryCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
defer cancel()

apiKey, err := s.apiKeyRepo.GetByKeyForAuth(queryCtx, key)
```

**效果：**
- DB 查询最多阻塞 500ms
- 超时后立即降级，不再等待
- 防止请求无限期阻塞

### 3. Stale Cache 降级策略

**文件：** `backend/internal/service/api_key_cache_helpers.go`

**功能：**
- 熔断器打开时，尝试使用过期的缓存数据
- 超时错误时，使用 stale cache 作为降级
- 保证服务在 DB 不可用时仍能提供有限服务

**降级优先级：**
1. L1 缓存 (Ristretto 内存缓存)
2. L2 缓存 (Redis)
3. 如果都没有，返回错误（但不会阻塞）

### 4. SQLite 性能优化

**文件：** `backend/internal/repository/local_first_ledger.go`

**改动：**
- 连接池：1 → 4 连接
- 空闲连接：1 → 2
- 添加 `busy_timeout=5000`（锁等待超时 5 秒）
- 添加 `cache_size=-64000`（64MB 缓存）

**效果：**
- 更好的并发写入能力
- WAL 模式下可以同时多读一写
- 减少锁竞争

## 测试验证

### 单元测试
```bash
cd backend
go test ./internal/pkg/circuitbreaker -v
```

**结果：** ✅ 全部通过
- TestCircuitBreaker_NormalOperation
- TestCircuitBreaker_OpensAfterFailures
- TestCircuitBreaker_HalfOpenAfterTimeout
- TestCircuitBreaker_ClosesAfterSuccessesInHalfOpen
- TestCircuitBreaker_ReopensOnFailureInHalfOpen
- TestCircuitBreaker_Call
- TestCircuitBreaker_HalfOpenMaxCalls

### 预期效果

**修复前：**
- Supabase 10秒故障 → 服务完全不可用
- 所有请求阻塞 10 秒后同时失败
- 恢复需要手动重启服务

**修复后：**
- Supabase 故障 → 熔断器在 2.5 秒内打开（5次失败 × 500ms）
- 已缓存的 Key 继续正常工作（使用 stale cache）
- 30 秒后自动测试恢复
- 无需人工干预

## 部署建议

### 1. 立即部署（P0）
这些修改修复了严重的可用性问题，建议尽快部署到生产环境。

### 2. 监控指标

部署后需要监控：
- 熔断器状态（是否经常打开）
- DB 查询超时次数
- Stale cache 使用次数
- SQLite 写入延迟

### 3. 告警设置

建议添加告警：
```yaml
- alert: CircuitBreakerOpen
  expr: circuit_breaker_state == 1
  for: 1m
  severity: critical
  annotations:
    summary: "熔断器已打开，DB 可能不可用"

- alert: HighDBQueryTimeout
  expr: rate(db_query_timeout_total[5m]) > 10
  for: 2m
  severity: warning
  annotations:
    summary: "DB 查询超时频繁"
```

## 后续改进 (P1/P2)

这是第一批紧急修复，后续还需要：

**P1 - 本周内：**
- [ ] 添加 Prometheus 监控指标
- [ ] 实现降级认证模式（最小权限快照）
- [ ] 添加异步计费（失败不阻塞请求）

**P2 - 下周：**
- [ ] 缓存异步预热机制
- [ ] SQLite 批量写入优化
- [ ] 完整的故障演练

## 风险评估

### 兼容性
- ✅ 向后兼容，不影响现有功能
- ✅ 降级策略仅在故障时生效
- ✅ 正常情况下行为不变

### 性能影响
- ✅ 添加 500ms 超时，正常查询不受影响（通常 < 50ms）
- ✅ SQLite 连接池增加，并发性能提升
- ⚠️ 熔断器增加少量 CPU 开销（可忽略）

### 数据一致性
- ✅ 使用 stale cache 时，数据可能略微过期（最多几分钟）
- ✅ 计费数据有本地 ledger 兜底，不会丢失
- ✅ 用户余额可能短时间内不准确，但不会造成超额扣费

## 回滚方案

如果出现问题，可以快速回滚：

```bash
git revert b6686119
git push origin fix/availability-improvements
```

或直接切回 main 分支重新部署。

## 相关文档

- [详细故障分析报告](../../../sub2api_custom_detailed_failure_analysis.md)
- [原始代码审查报告](../../../sub2api_custom_code_review.md)

---

**作者：** jiangliushi666  
**日期：** 2026-06-12  
**优先级：** P0 (紧急)  
**影响范围：** 认证流程、计费流程  
**测试状态：** ✅ 单元测试通过

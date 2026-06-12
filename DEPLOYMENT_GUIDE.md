# Sub2API 可用性修复实施指南

## 当前状态

✅ 已完成：
- 熔断器实现
- DB 查询超时控制 (500ms)
- Stale cache 降级策略
- SQLite 连接池优化 (1→4)
- SQLite 性能调优

📍 代码位置：
- 分支：`fix/availability-improvements`
- Commit：`b6686119`
- 远程：https://github.com/jiangliushi666/sub2api-custom/tree/fix/availability-improvements

## 部署步骤

### 步骤 1：构建新版本

```powershell
# 1. 切换到项目目录
Set-Location 'F:\vibe\state\sub2api-custom'

# 2. 确保在正确的分支
git checkout fix/availability-improvements
git pull origin fix/availability-improvements

# 3. 构建前端
Set-Location 'frontend'
corepack prepare pnpm@9 --activate
pnpm install --frozen-lockfile
pnpm run build

# 4. 构建后端（Linux amd64）
Set-Location '..\backend'
$env:GOOS='linux'
$env:GOARCH='amd64'
$env:CGO_ENABLED='0'
go build -tags embed -trimpath -ldflags '-s -w' -o 'F:\vibe\state\hf-sub2api\gateway' ./cmd/server

# 5. 验证二进制文件
if (Test-Path 'F:\vibe\state\hf-sub2api\gateway') {
    Write-Host "✅ 构建成功！" -ForegroundColor Green
    Get-Item 'F:\vibe\state\hf-sub2api\gateway' | Select-Object Name, Length, LastWriteTime
} else {
    Write-Host "❌ 构建失败！" -ForegroundColor Red
}
```

### 步骤 2：备份当前版本

```powershell
# 创建备份目录
$backupDir = "F:\vibe\backups\hf-sub2api-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
New-Item -ItemType Directory -Path $backupDir

# 备份当前运行的 gateway
Copy-Item 'F:\vibe\state\hf-sub2api\gateway' "$backupDir\gateway.backup"

Write-Host "✅ 备份完成：$backupDir" -ForegroundColor Green
```

### 步骤 3：上传到 HF Space

```powershell
# 切换到 HF Space 目录
Set-Location 'F:\vibe\state\hf-sub2api'

# 上传新的 gateway 二进制
hf upload xiemin1/sub2api gateway gateway --repo-type space --commit-message 'feat: add circuit breaker and timeout control to prevent cascading failures

- Add circuit breaker with 5-failure threshold and 30s timeout
- Add 500ms query timeout to prevent blocking
- Add stale cache fallback for DB unavailability
- Optimize SQLite pool from 1 to 4 connections
- Add SQLite performance tuning (busy_timeout, cache_size)

Fixes: Production incidents where Supabase 10s failures caused complete service outages'

Write-Host "✅ 上传完成！HF Space 将自动重启" -ForegroundColor Green
```

### 步骤 4：验证部署

等待 HF Space 重启完成后（约 2-3 分钟），执行以下验证：

```powershell
# 1. 检查健康状态
$healthUrl = "https://xiemin1-sub2api.hf.space/health"
$health = Invoke-RestMethod -Uri $healthUrl -Method Get
Write-Host "Health: $($health.status)" -ForegroundColor Cyan

# 2. 检查根路径（验证前端）
$rootUrl = "https://xiemin1-sub2api.hf.space/"
$root = Invoke-WebRequest -Uri $rootUrl -Method Get
if ($root.StatusCode -eq 200) {
    Write-Host "✅ 前端正常" -ForegroundColor Green
} else {
    Write-Host "❌ 前端异常：$($root.StatusCode)" -ForegroundColor Red
}

# 3. 检查日志（查找熔断器和本地账单日志）
# 需要在 HF Space 界面手动查看日志，确认有以下行：
# - [local-first] billing ledger enabled, flushing every 1h0m0s
# - [settings-cache] read cache enabled, ttl=1h0m0s
```

### 步骤 5：监控关键指标（部署后 1 小时内）

```powershell
# 创建监控脚本
@'
# 每分钟检查一次服务状态
$logFile = "F:\vibe\state\sub2api-monitor-$(Get-Date -Format 'yyyyMMdd').log"

while ($true) {
    $timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    
    try {
        $health = Invoke-RestMethod -Uri "https://xiemin1-sub2api.hf.space/health" -TimeoutSec 5
        $status = $health.status
        Write-Host "[$timestamp] ✅ Status: $status" -ForegroundColor Green
        "$timestamp | OK | $status" | Out-File -Append $logFile
    } catch {
        Write-Host "[$timestamp] ❌ Error: $($_.Exception.Message)" -ForegroundColor Red
        "$timestamp | ERROR | $($_.Exception.Message)" | Out-File -Append $logFile
    }
    
    Start-Sleep -Seconds 60
}
'@ | Out-File -FilePath 'F:\vibe\scripts\monitor-sub2api.ps1' -Encoding UTF8

Write-Host "监控脚本已创建：F:\vibe\scripts\monitor-sub2api.ps1"
Write-Host "运行监控：powershell -File F:\vibe\scripts\monitor-sub2api.ps1"
```

## 测试场景

部署完成后，建议测试以下场景：

### 场景 1：正常请求

```bash
# 使用一个有效的 API Key 测试
curl -X POST https://xiemin1-sub2api.hf.space/v1/chat/completions \
  -H "Authorization: Bearer sk-xxx" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-3-5-sonnet-20241022",
    "messages": [{"role": "user", "content": "Hello"}],
    "max_tokens": 10
  }'

# 预期：正常返回，无明显延迟增加
```

### 场景 2：缓存命中

```bash
# 连续发送相同 API Key 的请求，观察响应时间
for i in {1..10}; do
  time curl -s -X POST https://xiemin1-sub2api.hf.space/v1/chat/completions \
    -H "Authorization: Bearer sk-xxx" \
    -H "Content-Type: application/json" \
    -d '{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"test"}],"max_tokens":5}'
  echo ""
done

# 预期：第 2-10 次请求明显快于第 1 次（缓存命中）
```

### 场景 3：不存在的 API Key

```bash
# 测试不存在的 Key
curl -X POST https://xiemin1-sub2api.hf.space/v1/chat/completions \
  -H "Authorization: Bearer sk-nonexistent" \
  -H "Content-Type: application/json" \
  -d '{"model":"claude-3-5-sonnet-20241022","messages":[{"role":"user","content":"test"}],"max_tokens":5}'

# 预期：快速返回 401 错误，不会阻塞
```

## 回滚步骤（如有问题）

如果部署后发现问题，执行以下回滚：

```powershell
# 方案 1：使用备份的二进制
$latestBackup = Get-ChildItem 'F:\vibe\backups\hf-sub2api-*' | Sort-Object LastWriteTime -Descending | Select-Object -First 1
Copy-Item "$latestBackup\gateway.backup" 'F:\vibe\state\hf-sub2api\gateway'

# 上传旧版本
Set-Location 'F:\vibe\state\hf-sub2api'
hf upload xiemin1/sub2api gateway gateway --repo-type space --commit-message 'revert: rollback to previous version'

Write-Host "✅ 已回滚到备份版本" -ForegroundColor Yellow

# 方案 2：从 main 分支重新构建
Set-Location 'F:\vibe\state\sub2api-custom'
git checkout main
git pull origin main

# 然后重复"步骤 1：构建新版本"的命令
```

## 常见问题

### Q1: 部署后服务一直显示 "Building"

**A:** 这是正常的，HF Space 需要 2-3 分钟启动。如果超过 5 分钟，检查 HF Space 日志是否有错误。

### Q2: 健康检查失败

**A:** 
1. 检查 HF Space 日志是否有 panic 或错误
2. 确认二进制文件是否正确构建（Linux amd64）
3. 确认 `start-space.sh` 中的环境变量没有冲突

### Q3: 前端返回 404

**A:** 
1. 确认构建时使用了 `-tags embed`
2. 确认 `frontend/dist` 目录存在
3. 重新构建前端和后端

### Q4: 如何确认熔断器是否工作？

**A:** 
1. 查看 HF Space 日志，搜索 "circuit"
2. 如果 Supabase 短暂不可用，应该看到熔断器状态变化
3. 可以通过模拟 DB 故障来测试（不建议在生产环境）

## 监控和告警

### 重要日志关键字

部署后，在 HF Space 日志中关注以下关键字：

- ✅ 正常启动：`[local-first] billing ledger enabled`
- ✅ 正常启动：`[settings-cache] read cache enabled`
- ⚠️ 熔断器触发：`circuit breaker open`
- ⚠️ 查询超时：`context deadline exceeded`
- ⚠️ 降级策略：`stale cache`
- ❌ SQLite 错误：`database disk image is malformed`

### 建议的监控频率

**前 24 小时：** 每小时检查一次
**前 1 周：** 每天检查一次
**长期：** 每周检查一次，或在收到告警时检查

## 下一步

部署并验证稳定后，继续实施 P1 优先级的改进：

1. [ ] 添加 Prometheus 监控指标
2. [ ] 实现降级认证模式
3. [ ] 添加异步计费
4. [ ] 进行完整的故障演练

---

**联系方式：**
- GitHub Issue: https://github.com/jiangliushi666/sub2api-custom/issues
- 详细分析：见 `sub2api_custom_detailed_failure_analysis.md`

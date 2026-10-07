# preview-environment-controller-go

本地可跑的预览环境编排能力：用本地进程模拟环境实例，统一管理环境的
**申请、占用、释放、租约续期、空闲回收** 全生命周期；既可作为库使用，
也可通过 `previewd` 常驻为本地 HTTP 服务，供开发机与 CI 上的多个使用方共用。

## 架构

```
cmd/previewctl        CLI 一次性演示入口（申请/释放/回收完整生命周期）
cmd/previewd          常驻服务入口（HTTP API + 周期空闲回收 + 优雅停机）
internal/preview
  ├── types.go        状态机、环境对象、租约、回收报告、可区分错误码
  ├── backend.go      Backend 接口 + 本地进程后端（sleep 进程模拟实例）
  └── controller.go   编排核心：申请/释放/续期/回收/停机，单互斥锁保证并发一致
internal/service
  ├── server.go       HTTP 服务生命周期、路由、请求标识中间件、统一错误映射
  ├── handlers.go     申请/释放/续期/回收/查询/停机 handler
  └── types.go        请求/响应 DTO
```

## 常驻服务（previewd）

```bash
go run ./cmd/previewd -addr :18080 -ttl 30m -idle-ttl 10m -drain-timeout 60s
```

启动后长期对外提供服务，所有 HTTP 入口最终都落到同一个带互斥锁的
`Controller`：无论请求从哪个入口进来，同一时刻对同一操作的结论与失败原因
完全一致，并发压上来也不会出现“这边成功、那边看不到变化”。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/environments/acquire` | 申请（并发同名收敛，只建一个实例） |
| POST | `/v1/environments/release` | 释放占用 |
| POST | `/v1/environments/renew` | 占用方给自己的租约续期 |
| POST | `/v1/reclaim` | 触发一次空闲回收扫描（返回回收/跳过/失败明细） |
| GET  | `/v1/environments/{name}` | 查询单个环境 |
| GET  | `/v1/environments` | 列出全部环境 |
| POST | `/v1/shutdown` | 触发停机收尾（幂等） |

每个请求可用 `X-Request-ID` 头携带申请标识；未携带时服务自动生成，
并在响应头 `X-Request-ID` 与所有日志中回显。所有响应为统一信封
`{request_id, data?, error?}`。

### 停机语义

收到 `SIGINT`/`SIGTERM` 或 `POST /v1/shutdown` 后（两条路径共用同一轮收尾，
可重复触发）：

1. **立即停止接受新申请**：此后所有 acquire 稳定返回 `503 SHUTTING_DOWN`，
   不是断连也不是半响应；release/renew/get 等进行中语义的请求仍有明确结果。
2. **优雅等待**：等进行中请求完成、占用者离开；占用者一离开立即回收该实例
   （停机路径忽略空闲阈值），直到本服务拉起过的实例全部停止。
3. **超时强制**：超过 `-drain-timeout` 仍有残留（占用者不释放）时，强制停止
   全部实例，保证退出后不留后台进程；强制阶段仍失败会返回错误，绝不谎报成功。

## 租约续期

每个占用者在申请时获得一份独立租约（`Environment.Leases[holder]`）。

- `Renew(name, holder, extend)` 只延长**该占用者自己**的到期时间，
  不影响同一环境上的其他占用者，也不改变环境的初始有效期 `ExpiresAt`。
- 到期判定以当前时刻与该占用者租约到期点比较：到期时刻（含）起不可再续。
- 三类拒绝互相区分，且拒绝不改动任何状态（先判定、后写入）：

| 场景 | 错误码 | HTTP |
|---|---|---|
| 环境不存在或已回收 | `NOT_FOUND` | 404 |
| 调用方并未占用该环境 | `NOT_HELD` | 409 |
| 已错过续期时间点（租约已到期） | `LEASE_EXPIRED` | 409 |

错过续期窗口的占用方可以**重新申请**（acquire 会重新发放完整租约）拿回使用权。

## 空闲回收（替代旧的“自创建起过期”口径）

- 空闲计时从**最后一个占用者离开**（`ReleasedAt`）开始；中途有人离开但仍有
  其他人占用时不计时。
- 空闲超过 `IdleTTL`（`-idle-ttl`，默认取 TTL）才可回收。
- **仍在使用的环境，即使早过了初始有效期，也绝不回收**，保持可用，并在
  扫描结果/日志中给出跳过依据（`ReclaimReport.Skipped`）。
- `ReclaimIdle` 返回 `ReclaimReport{Reclaimed, Skipped, Failures}`：
  回收失败计入 `Failures`、状态回滚 Ready 且原因落在 `Environment.ReclaimErr`，
  可安全重试，绝不会对外报成功但其实没清掉。HTTP 入口在存在失败时返回
  `502`，同时给出可重试的明细；全部成功返回 `200`。
- `previewd` 默认每 30s 自动扫描一次（`-reclaim-every` 调整）。

### 与原有行为的兼容范围

- `Acquire`/`Release`/`Destroy`/`Get` 语义与错误码保持不变；同名并发收敛、
  占用保护（`ENV_IN_USE`）、释放校验（`NOT_HELD`/`NOT_FOUND`）、配额与端口
  耗尽（`QUOTA_EXCEEDED`/`PORT_EXHAUSTED`）、后端故障（`BACKEND_ERROR`）照旧。
- 旧的 `Reclaim(reqID) ([]string, error)` 签名保留（现有调用与用例无需改动），
  其判定口径已升级为“空闲超过 IdleTTL”；需要失败明细时改用 `ReclaimIdle`。
  `IdleTTL` 为 0 时回退为 `TTL`，旧配置行为等价。
- `Environment.ExpiresAt` 保留，表示环境初始有效期（创建时间 + TTL）；
  续期不改它，每占用者的有效时间看 `Leases`。
- 新增错误码 `LEASE_EXPIRED`、`SHUTTING_DOWN`，与既有错误码并列、可区分。

## 本地验证

```bash
# 全部单测（含续期临界、空闲/在用混合、回收失败重试、停机收尾、HTTP 并发）
go test ./... -v

# 竞态检测
go test -race ./...

# 旧 CLI 演示（保持可用）
go run ./cmd/previewctl -ttl 5s

# 常驻服务 + curl 手测
go run ./cmd/previewd -addr :18080 -idle-ttl 3s
curl -s -XPOST localhost:18080/v1/environments/acquire \
  -d '{"name":"demo","holder":"u1"}'
curl -s -XPOST localhost:18080/v1/environments/renew \
  -d '{"name":"demo","holder":"u1","extend":"10m"}'
curl -s localhost:18080/v1/environments/demo
curl -s -XPOST localhost:18080/v1/environments/release \
  -d '{"name":"demo","holder":"u1"}'
# 等待 idle-ttl 后：
curl -s -XPOST localhost:18080/v1/reclaim -d '{}'
# 收尾退出（可重复调用）：
curl -s -XPOST localhost:18080/v1/shutdown
```

测试与运行日志均打印申请标识（`req=...`）、状态迁移
（`state=Ready->Reclaiming->Reclaimed`）与判定依据（新建/复用/续期/跳过原因/
拒绝原因码）。

# preview-environment-controller-go

本地可跑的预览环境编排能力：用本地进程模拟环境实例，统一管理环境的
**申请、占用、续期、释放、空闲回收** 全生命周期。既可以作为库直接调用
（`internal/preview.Controller`），也可以作为常驻 HTTP 服务长期运行
（`previewctl serve`），供开发机与 CI 上的多个使用方并发调用。

## 架构

```
cmd/previewctl
  ├── demo（默认）   本地演示入口：完整生命周期
  └── serve          常驻 HTTP 服务入口（信号触发优雅停机）
internal/preview
  ├── types.go       状态机、环境对象、可区分错误码、回收报告
  ├── backend.go     Backend 接口 + 本地进程后端（sleep 进程模拟实例）
  ├── controller.go  编排核心：申请/释放/销毁/回收/查询，单互斥锁保证并发一致
  ├── lease.go       租约续期：只延长调用方自己那份可用时间
  └── lifecycle.go   StopAll：停机收尾，停止全部自拉起实例（幂等、可重试）
internal/service
  ├── server.go      常驻 HTTP 服务、停机闸门（draining）与优雅停机
  └── handlers.go    申请/释放/回收/查询/续期入口、错误码->HTTP 映射
```

## 生命周期状态机

```
(无) --Acquire--> Provisioning --后端启动成功--> Ready
Ready --Acquire(同名)--> Ready            （复用，追加占用者并发放独立租约）
Ready --Renew(占用者)--> Ready            （只延长该占用者的租约）
Ready --Release(最后占用者离开)--> Ready(空闲, IdleAt 起算)
Ready(空闲且 IdleAt+IdleTTL 到期) --Reclaim--> Reclaiming --> Reclaimed
Ready --Destroy(无占用)--> Reclaiming --> Reclaimed
Reclaiming --停止失败--> Ready            （回滚，记录 ReclaimErr，可重试）
Reclaimed --Acquire(同名)--> Provisioning （替换为新实例，端口可复用）
```

关键不变式：

- **并发收敛**：所有入口共用控制器一把互斥锁，并发申请同名环境只会创建
  一个实例，其余申请者收敛到同一环境（`created=false`、同一端口）。
  HTTP 层任何入口进来的同一操作，结论与失败原因完全一致。
- **幂等**：重复申请返回同一环境；重复回收/重复停机是空操作；后端
  `Stop` 幂等。
- **不留半成品**：新建时任一步骤（配额/端口/后端启动）失败都会回滚；
  回收失败回滚状态并记录原因，可安全重试，绝不对外谎报成功。

## 租约模型与续期

- 每个占用者在（首次）申请时获得一份**独立租约**，默认时长为 `-ttl`。
  各占用者的租约到期时间记录在 `LeaseExpiry`，环境整体到期时间
  `expiresAt` 取所有占用者租约的最大值。
- `Renew(name, holder, extend)` 只延长 **holder 自己** 那份租约，
  其他占用者的到期时间与环境结论不受影响。
- 续期必须发生在自己租约到期**之前**（`now < leaseExpiry`）；
  恰好到期即视为错过。四种拒绝原因可互相区分：

  | 情况 | 错误码 | HTTP |
  |---|---|---|
  | 环境不存在或已回收 | `NOT_FOUND` | 404 |
  | 环境正在回收中 | `NAME_CONFLICT` | 409 |
  | 调用方并未占用该环境 | `NOT_HELD` | 403 |
  | 自己的租约已到期，错过续期窗口 | `LEASE_EXPIRED` | 403 |

  任何续期失败都**先判定后写入**，不会改坏环境状态或改到一半。

## 空闲回收语义（新口径，替代旧的“TTL 过期即回收”）

- 回收不再看环境创建了多久，而是看**空闲多久**：最后一个占用者离开的
  时刻记为 `idleAt`，`now >= idleAt + idle-ttl` 才允许回收。
- **还在被使用的环境绝不回收**：哪怕已经超过整体到期时间，也照常可查、
  可复用（同名申请），回收扫描会把它放进 `skipped` 并注明 `why=busy`。
- 全部离开后又有人回来：`idleAt` 清零；再次全部离开后从新的离开点起算。
- 回收结果以报告返回：`reclaimed`（成功）、`failures`（失败，可安全
  重试，HTTP 状态 207）、`skipped`（跳过及原因 `busy` /
  `idle-not-long-enough`）。回收没成功时入口明确可见，不会报成功。

`why` 取值：`idle-expired`（空闲超阈值回收）、`busy`（在用，跳过）、
`idle-not-long-enough`（空闲未满阈值，跳过）。

## 常驻 HTTP 服务

`previewctl serve` 启动后长期运行，提供：

| 方法与路径 | 含义 |
|---|---|
| `POST /v1/envs` | 申请：`{"name","holder"}` |
| `GET  /v1/envs` | 列出全部环境 |
| `GET  /v1/envs/{name}` | 查询单个环境 |
| `POST /v1/envs/{name}/renew` | 续期：`{"holder","extend":"30s"}` |
| `DELETE /v1/envs/{name}?holder=h` | 释放该占用者 |
| `DELETE /v1/envs/{name}` | 空闲销毁（无 holder 参数） |
| `POST /v1/reclaim` | 空闲回收扫描，返回成功/失败/跳过报告 |

- 请求可用 `X-Request-Id` 头携带申请标识；未带则服务自动生成，日志与
  响应体都带该标识，便于追踪状态迁移与判定依据。
- 所有错误响应形如 `{"requestId","code","error"}`，`code` 与库错误码
  一一对应。

### 优雅停机（SIGINT / SIGTERM）

1. 置 draining 状态：**保持监听**，但新申请、续期稳定返回
   `503 SERVICE_DRAINING`（不会静默断开、不会只做一半）；
2. 等待已在途的请求结束；
3. 等待仍在用的环境被释放（宽限期 `-grace`）——等待期内**仍允许
   释放**，以便环境尽快变空闲；超时则强制停止剩余在用实例；
4. `StopAll` 停止服务拉起过的**全部**实例，退出后不留后台进程；
   停止失败会做有限次安全重试，仍失败则非零退出并在报告中列出；
5. 实例收尾完成后才关闭 HTTP 监听。

`Shutdown` 可被重复触发（例如连按两次 Ctrl-C），返回第一次的同一结论，
不会二次执行或报错。

## 隔离与配额规则

| 规则 | 实现 | 失败原因码 |
|---|---|---|
| 命名隔离 | 环境以名称作为唯一键；同名申请收敛复用 | — |
| 命名冲突 | 同名环境处于 `Reclaiming` 时拒绝新申请/续期 | `NAME_CONFLICT` |
| 端口隔离 | 端口从 `[PortBase, PortEnd]` 池独占分配，回收后归还 | `PORT_EXHAUSTED` |
| 配额控制 | 未回收环境数不得超过 `MaxEnvs` | `QUOTA_EXCEEDED` |
| 占用保护 | 仍有占用者的环境禁止销毁；空闲回收跳过在用环境 | `ENV_IN_USE` |
| 释放校验 | 释放者必须确实占用该环境 | `NOT_HELD` / `NOT_FOUND` |
| 续期校验 | 见上文续期四种原因 | `NOT_FOUND`/`NAME_CONFLICT`/`NOT_HELD`/`LEASE_EXPIRED` |
| 停机保护 | 停机期间新申请/续期 | `SERVICE_DRAINING` |
| 后端故障 | 进程启动/停止失败 | `BACKEND_ERROR`（可重试） |

所有错误都是带 `Code` 的 `*preview.Error`，调用方可用 `errors.As`
区分失败原因；每次判定都打印日志，包含申请标识（req）、状态迁移和
判定依据，不存在静默失败或半完成状态。

## 与原有行为的兼容范围

- **申请语义不变**：同名未回收环境复用、已回收环境替换为新实例、
  并发收敛、配额/端口校验与回滚完全保留。
- **释放语义不变**：`Release` 仍只移除占用者、保留环境；`NOT_HELD` /
  `NOT_FOUND` 的判定不变。新增的是：最后一个占用者离开时记录 `idleAt`，
  作为空闲回收的起算点。
- **回收口径变化**：从“创建后 TTL 到期且空闲”改为“最后占用者离开后
  空闲满 `idle-ttl`”。新口径下，在用环境即使超过旧 TTL 也不会被回收；
  构造控制器时需设置 `Config.IdleTTL`。`Reclaim` 返回值由名称列表升级
  为 `*ReclaimReport`（成功/失败/跳过都可见）。
- **销毁语义不变**：`Destroy` 仍拒绝在用环境（`ENV_IN_USE`）。
- **新增**：`Renew`、`StopAll`、`Controller.List` 与常驻 HTTP 服务。

## 本地验证

```bash
# 全部单测：覆盖并发申请同名环境、临界时刻续期、占用者续期隔离、
# 空闲/在用混合回收、停机期间请求、回收失败重试、停机收尾无残留
go test ./... -v

# 竞态检测（必须通过）
go test -race ./...

# 库/CLI 演示：真实启动本地 sleep 进程作为环境实例
go run ./cmd/previewctl demo -ttl 5s -idle-ttl 1s

# 常驻服务
go run ./cmd/previewctl serve -addr 127.0.0.1:18080 -ttl 30s -idle-ttl 2s
```

另开终端手动验证常驻服务：

```bash
# 申请（带申请标识，便于在服务日志中追踪）
curl -s -H 'X-Request-Id: demo-1' -d '{"name":"demo","holder":"alice"}' \
  http://127.0.0.1:18080/v1/envs
# 续期 10 秒（只延长 alice 自己）
curl -s -d '{"holder":"alice","extend":"10s"}' \
  http://127.0.0.1:18080/v1/envs/demo/renew
# 查询 / 列表
curl -s http://127.0.0.1:18080/v1/envs/demo
curl -s http://127.0.0.1:18080/v1/envs
# 释放 -> 等待 idle-ttl -> 空闲回收
curl -s -X DELETE 'http://127.0.0.1:18080/v1/envs/demo?holder=alice'
curl -s -X POST http://127.0.0.1:18080/v1/reclaim
# 停机（服务端终端按 Ctrl-C，或 kill -TERM）：观察日志中
# draining -> 在途结束 -> 等待空闲 -> stop-all -> 监听关闭，
# 且 pgrep -f 'sleep 3600' 无残留进程
```

测试与运行日志均打印申请标识（`req=...`）、状态迁移
（`state=Ready->Reclaiming`）与判定依据（新建/复用/拒绝原因码/
`why=busy|idle-expired|idle-not-long-enough`）。

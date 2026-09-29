# 设计说明：预览环境编排器

## 1. 生命周期状态机

每个预览环境 (`internal/environment.Environment`) 有五个状态：

```
                          (provision start)
   (none) ──────────────► Pending ──(backend ready)──► Ready
                              ▲                           │  ▲
                              │ (revive after Released)   │  │
                              │                    (last holder
                              │                     leaves)
                              │                           ▼  │
                              │                        InUse │
                              │                        (≥1 holder)
                              │                           │
                              │      Ready/InUse          │
                              │   (idle release-all)      │
                              ▼                           ▼
                           Released ◄─────────────────────┘
                              │ (back-end stop succeeds)
                              ▼
                           Reclaimed   (终态；同名记录保留， claim_version++)
```

迁移由以下操作触发（全部在控制器的互斥锁内完成记录，后端调用在锁外执行）：

| 操作 | 允许的起始状态 | 目标状态 | 说明 |
| --- | --- | --- | --- |
| `Apply(target, holder?)` | 无记录 | `Pending → Ready[→ InUse]` | 分配端口、启动后端；带 holder 时直接 InUse |
| `Apply` 重复/并发 | `Pending/Ready/InUse` | 不变（收敛） | 返回同一实例，同端口、同 `claim_version`，只刷新 TTL / 追加 holder |
| `Apply` | `Released`（同 target） | `Pending → Ready` | **复用记录、重新供给**（revive），`claim_version++` |
| `Apply` | `Reclaimed` | `Pending → Ready` | 终态记录保留作审计，同名开启新代次，`claim_version++` |
| `Occupy(holder)` | `Ready/InUse` | `InUse` | 同 holder 重复占用幂等 |
| `Release(holder)` | `InUse/Ready` | `InUse` 或 `Ready` | 移除一个 holder；最后一个离开时回 Ready；未知 holder 释放幂等 |
| `ReleaseAll` | `Ready` | `Released` | 仅允许**空闲**环境；有 holder 一律 `InUse` 拒绝；同步停后端、还端口 |
| `Reclaim` | `Released`；或 TTL 已过的 `Ready`（手动或 reaper） | `Reclaimed` | Released 只做收尾记账；过期 Ready 需停后端、还端口；对 `Reclaimed` 幂等 |
| 后台 tick | `Released` 或 TTL 已过的 `Ready` | `Reclaimed` | 停后端失败则记录并在下一 tick 重试 |

> **TTL 语义**：`Occupy` 与收敛式 `Apply` 都会把 `ExpiresAt` 向后刷新。
> reaper 只回收 `Ready` 且已过期的环境；`InUse` 环境即使名义到期也继续运行。
> `Released` 环境的后端与端口在 `ReleaseAll` 时已释放，但名称仍保留、仍占配额，
> 直到 `Reclaim` 完成记账（因此"停后端失败、需要重试"只会发生在过期回收路径，
> 以及 `ReleaseAll` 自身的停止调用上）。

### 收敛与代次（claim_version）

- 同一 `target` 存在活动记录时，`Apply` 不创建任何资源，直接返回现有实例。
- 并发 `Apply` 使用 per-target single-flight（`sync.Map` 登记 leader）：
  后来者等待同一个 `done` channel，不会二次分配端口或二次启动后端。
- 失败的 leader 会**回滚**，后来者拿到错误后由调用方重试。
- `Reclaimed` 之后的申请是**新一代资源**：记录同名，`claim_version + 1`，
  事件日志里有 `superseded` 与新的 `apply/ready` 事件，状态变化可追溯。

### 事件日志（可解释性）

每次迁移都向 `Environment.Events` 追加：

```json
{"from":"Pending","to":"Ready","reason":"ready",
 "detail":"endpoint=http://127.0.0.1:19000 pid=20001","request_id":"req-1"}
```

控制器 stderr 日志与 `go test -v` 输出都带 `[req=<request_id>]` 前缀，
打印申请标识、迁移方向和判定依据（如拒绝原因与当前 holder 列表）。

## 2. 隔离与配额规则

### 2.1 命名隔离

- 不显式传名时，名字由 target 稳定派生：`pe-<sanitized(target)>`，
  因此同一 target 天然收敛、不同 target 天然不撞名。
- 显式指定的名字若已被**不同 target** 的活动/Released 记录占用 →
  **`NameConflict`**（即使处于 `Released` 也冲突，因为名称保留到 Reclaim）。
- 只有 `Reclaimed`（资源已销毁）后同名才可被任意 target 重新使用。

### 2.2 配额

- `MaxEnvironments` 同时计入：`Pending/Ready/InUse` + `Released`。
  `Reclaimed` 不占配额（资源与名称保留语义已结束）。
- 创建路径上**先判配额、再判端口**，因此两个上限同时打满时错误码确定：
  先返回 `QuotaExhausted`。

### 2.3 端口池

- 闭区间 `[PortLow, PortHigh]` 内的硬预留，两个环境绝不会拿到同一端口。
- `Allocate` 失败 → **`PortsExhausted`**（与配额耗尽不同码、不同 HTTP 状态体）。
- `Release(port)` 对未占用端口是 no-op，保证回收重试幂等。

### 2.4 拒绝原因对照（三入口一致）

| 情形 | 错误码 | HTTP | 结果是否修改状态 |
| --- | --- | --- | --- |
| 参数缺失 | `InvalidRequest` | 400 | 否 |
| 同名不同 target | `NameConflict` | 409 | 否 |
| 配额满 | `QuotaExhausted` | 503 | 否 |
| 端口满 | `PortsExhausted` | 503 | 否 |
| 有 holder 时 release-all / reclaim | `InUse` | 409 | 否 |
| Pending 中释放 / 回收 | `NotReady` | 409 | 否 |
| Ready 未过期手动 reclaim | `NotReclaimable` | 409 | 否 |
| 名称不存在 | `NotFound` | 404 | 否 |
| 后端启动/停止失败 | `ProvisionerFailure` | 502 | 启动失败回滚；停止失败状态不变 |

所有失败都是"要么全成、要么不动"：启动失败回滚端口+配额+记录，
停止失败不转移状态，因此不存在静默失败或只改一半。

## 3. 回收（Reaper）

- `Reaper`（`internal/controller/reaper.go`）按固定间隔调用
  `Controller.ReclaimExpired`；也可用 `POST /v1/reclaim-expired` 手动触发。
- 扫描（锁内）只挑两类：`Released`；`Ready` 且 `ExpiresAt <= now`。
  扫描结果快照后在锁外逐个停止后端，避免长耗时后端阻塞其它请求。
- `Released`：后端与端口已在 `ReleaseAll` 阶段释放，reaper 只做记账收尾，必然成功。
- 过期 `Ready`：锁外停止后端。停止失败时 `ReclaimErrs` 追加一条（时间+原因），
  事件 `reclaim-failed`，**状态保持不变、端口保留**，下一轮继续尝试；
  `ReclaimResult.Retryable=true`。
- 停止成功：转 `Reclaimed`、清零 PID/Endpoint/Port、归还端口、删 target 索引。
- 对已 `Reclaimed` 环境再 `Reclaim` → 事件 `reclaim-idempotent`，无副作用。
- 扫描与执行之间若环境被重新占用（竞态），停止前的状态分支返回 `InUse`，
  在结果中标记为非 retryable 的跳过，不会误杀在用实例。

## 4. 后端抽象（本地进程/容器模拟）

```go
type Provisioner interface {
    Start(ctx, name string, port int) (Instance, error)
    Stop(ctx, env *environment.Environment) error
    Health(ctx, env *environment.Environment) error
}
```

默认 `SimProvisioner` 是纯内存实现，零外部依赖：

- 记录"运行中"实例（合成 PID 与 `http://127.0.0.1:<port>` endpoint）；
- `FailStartNext(name)`：让下一次启动失败一次（验证回滚）；
- `FailStopTimes(name, n)`：让接下来 n 次停止失败（验证重试与幂等）；
- 对已不存在的实例 `Stop` 成功（控制器侧幂等的基础）。

后续接入真实容器/本地进程时只需实现该接口，编排逻辑不变。

## 5. HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/environments` | 申请/收敛，body：`target,name,holder,ttl,request_id` |
| GET  | `/v1/environments` | 列出全部（含事件与回收失败史） |
| GET  | `/v1/environments/{name}` | 查询单个 |
| POST | `/v1/environments/{name}/occupy` | body：`holder,request_id` |
| POST | `/v1/environments/{name}/release` | 释放单个 holder |
| POST | `/v1/environments/{name}/release-all` | 空闲回收至 Released，同步停后端还端口（占用则 409） |
| POST | `/v1/environments/{name}/reclaim` | 销毁（Released 收尾 / 过期 Ready；幂等） |
| POST | `/v1/reclaim-expired` | 立即执行一轮清扫 |

错误响应统一为：

```json
{"code":"InUse","message":"...","target":"pe-refs-pr-1","request_id":"demo-3"}
```

可用 `-request-id` 参数或 `X-Request-Id` 头贯穿一次申请的所有日志与事件。

## 6. 测试矩阵

| # | 场景 | 关键断言 |
| --- | --- | --- |
| 1 | 重复申请同一 target | 同名/同端口/`claim_version=1`，端口池占用 1 |
| 2 | 32 路并发申请 | 全部拿到同端口同名，后端实例数=1，记录数=1（含 `-race`） |
| 3 | 名称冲突 | `NameConflict`；小配额 → `QuotaExhausted`；单端口区间 → `PortsExhausted`，三者可区分 |
| 4 | 占用中释放 | `ReleaseAll` 与 `Reclaim` 均 `InUse`；holder 逐个释放至 Ready；未知 holder 释放幂等 |
| 5 | 过期回收 | 空闲+Released 被回收；占用环境过 TTL 仍 InUse、PID 存活；未过期 Ready 手动回收 `NotReclaimable` |
| 6 | 回收失败重试 | 过期 Ready 前 2 次停止 `ProvisionerFailure`：状态不变、端口不还、失败留痕；第 3 次成功；第 4 次幂等；reaper sweep 同样会重试（`TestReaperRetriesFailedExpiryReclaim`）；`ReleaseAll` 停止失败也不改状态（`TestReleaseAllStopFailureKeepsState`） |
| 7 | 回收后再申请 | `claim_version=2`、新端口/PID；紧接着的再申请收敛回代次 2 |
| 8 | 启动失败回滚 | 错误码 `ProvisionerFailure`，端口与配额立即可用，无残留记录 |
| 9 | HTTP 入口一致性 | 经 HTTP 重复申请收敛；占用中释放返回同一 `InUse` 码；404 结构化 |
| 10 | HTTP 并发 | 16 路 HTTP 并发申请只产生一个实例 |

查看带申请标识、状态迁移、判定依据的日志：

```bash
go test ./internal/controller/ -v -run 'TestReleaseInUse|TestReclaimFailure' 2>&1 | less
```

## 7. 本地端到端验证

```bash
# 终端 A
go run ./cmd/server -addr 127.0.0.1:8080 -ttl 2s -reap-every 1s

# 终端 B：观察 Ready ->（2s 后 reaper 自动）-> Reclaimed -> 再申请 claim=2
go run ./cmd/ctl apply -target refs/ttl/1
go run ./cmd/ctl get -name pe-refs-ttl-1
sleep 3
go run ./cmd/ctl get -name pe-refs-ttl-1     # Reclaimed, port 0
go run ./cmd/ctl apply -target refs/ttl/1    # Ready, claim_version 2
```

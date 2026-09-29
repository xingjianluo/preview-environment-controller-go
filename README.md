# Preview Environment Controller (Go)

一套**本地可跑**的预览环境编排器：用本地进程/容器的可插拔模拟后端
（`Provisioner` 接口 + 内存模拟实现）来代表"环境实例"，并在三个入口上
给出**一致且可解释**的申请 / 占用 / 释放 / 回收结果：

1. **库入口**：`internal/controller`（Go API）
2. **HTTP 入口**：`internal/httpapi`（REST，库入口的薄封装）
3. **CLI 入口**：`cmd/ctl`（走 HTTP，返回同样的结构化错误码）

三个入口共用同一个状态机、同一份错误码定义（`internal/environment`），
因此不存在"库能拒绝、HTTP 却静默成功"这类不一致。

## 能力一览

- **申请 / 就绪 / 释放全流程**
  - `Apply(target)` 幂等：同一 target 重复申请收敛到**同一个实例**
    （同名、同端口、相同 `claim_version`）。
  - 并发申请同一 target 通过 single-flight 合并为**一次**创建，
    32/16 并发的竞态测试保证不会产生两个实例。
  - 申请时即可携带 holder，环境从 `Ready` 直接进入 `InUse`。
- **隔离与配额**
  - 名称冲突（同名但不同 target）→ `NameConflict`（HTTP 409）。
  - 环境配额耗尽 → `QuotaExhausted`（HTTP 503）。
  - 主机端口池耗尽 → `PortsExhausted`（HTTP 503）。
    两类"耗尽"原因相互区分，先判配额再判端口，顺序确定。
  - 释放/回收**仍在占用**中的环境 → `InUse`（HTTP 409），错误信息中
    直接给出当前 holder 列表。
  - 后端启动失败会**完整回滚**端口与配额占用，不会留下半分配状态。
- **超时 / 异常中断后的回收**
  - `Ready` 环境超过 TTL 后由后台 reaper 回收；`InUse` 环境即使名义上
    过期也**不受影响**（持有优先）。
  - `ReleaseAll` 同步停止后端并归还端口；停止失败则状态与端口保留，可重试。
  - 过期回收（手动或 reaper）时若后端停止失败，状态不变、端口不释放，
    失败被追加到 `reclaim_errors`，reaper 下一 tick 自动重试；
    成功后的再次回收是**幂等 no-op**。
  - 回收后再申请：同名记录保留审计轨迹，资源代次 `claim_version` 递增，
    状态变化稳定可解释。
- **可解释性**
  - 每个环境带有序的 `events` 迁移日志（from/to/reason/detail/request_id）。
  - 所有控制器日志带申请标识 `[req=...]`，`go test -v` 可直接看到
    申请标识、状态迁移与判定依据。

## 快速开始

```bash
# 终端 1：启动服务（模拟后端，无需 Docker）
go run ./cmd/server -addr 127.0.0.1:8080 \
  -max-envs 8 -port-low 19000 -port-high 19100 \
  -ttl 60s -reap-every 5s

# 终端 2：用 CLI 走一遍生命周期（全局 flag 可在子命令前后任意位置）
go run ./cmd/ctl apply -target refs/pr/123 -request-id demo-1
go run ./cmd/ctl occupy  -name pe-refs-pr-123 -holder ci-job-1 -request-id demo-2
go run ./cmd/ctl release-all -name pe-refs-pr-123 -request-id demo-3   # 被拒绝: InUse
go run ./cmd/ctl release  -name pe-refs-pr-123 -holder ci-job-1 -request-id demo-4
go run ./cmd/ctl release-all -name pe-refs-pr-123 -request-id demo-5   # Ready -> Released
go run ./cmd/ctl reclaim   -name pe-refs-pr-123 -request-id demo-6    # Released -> Reclaimed
go run ./cmd/ctl list
```

也可以直接用 curl：

```bash
curl -s -XPOST localhost:8080/v1/environments \
  -d '{"target":"refs/pr/123","ttl":"30s","request_id":"curl-1"}'
curl -s -XPOST localhost:8080/v1/reclaim-expired -d '{}'
curl -s localhost:8080/v1/environments | jq .
```

## 本地验证（测试）

```bash
go test ./... -count=1                 # 全部单测
go test ./... -race -count=1           # 竞态检测
go test ./internal/controller/ -v      # 查看申请标识/状态迁移/判定依据日志
```

测试覆盖（详见 `docs/design.md` 的"测试矩阵"）：

| 场景 | 测试 |
| --- | --- |
| 重复申请收敛 | `TestApplyConvergesToSameInstance` |
| 并发申请单实例 | `TestConcurrentApplySingleInstance`（32 goroutine） |
| 名称冲突 vs 配额 vs 端口耗尽 | `TestNameConflictIsRejectedAndDistinguishable` |
| 占用中释放被拒、holder 级释放 | `TestReleaseInUseRejectedAndHolderRelease` |
| 过期回收但保留在用环境 | `TestReclaimExpiredKeepsInUse` |
| 回收失败重试 + 幂等 | `TestReclaimFailureRetryAndIdempotency` |
| 回收后再申请稳定 | `TestApplyAfterReclaimStable` |
| 启动失败完整回滚 | `TestProvisionFailureRollback` |
| HTTP/CLI 入口一致性 | `internal/httpapi/server_test.go` |

## 目录结构

```
internal/environment/  领域模型：状态、事件、结构化错误码、申请请求
internal/controller/   编排核心：状态机、single-flight、端口池、配额、reaper、Provisioner 接口与模拟实现
internal/httpapi/      HTTP Handler 与 Client（三个入口共享同一套语义）
cmd/server/            HTTP 服务入口
cmd/ctl/               命令行入口
docs/design.md         生命周期状态机、隔离/配额规则、HTTP API、测试矩阵
```

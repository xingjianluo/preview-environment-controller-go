# preview-environment-controller-go

本地可跑的预览环境编排能力：用本地进程模拟环境实例，统一管理环境的
**申请、占用、释放、回收** 全生命周期，保证各入口下结果一致且可解释。

## 架构

```
cmd/previewctl        本地验证入口（CLI 演示完整生命周期）
internal/preview
  ├── types.go        状态机、环境对象、可区分错误码
  ├── backend.go      Backend 接口 + 本地进程后端（sleep 进程模拟实例）
  └── controller.go   编排核心：申请/释放/销毁/回收，单互斥锁保证并发一致
```

## 生命周期状态机

```
(无) --Acquire--> Provisioning --后端启动成功--> Ready
Ready --Acquire(同名)--> Ready            （复用，追加占用者，不重复创建）
Ready --Release(占用者离开)--> Ready      （环境保留，等待过期）
Ready --Destroy(无占用)--> Reclaiming --停止成功--> Reclaimed
Ready(过期且空闲) --Reclaim--> Reclaiming --停止成功--> Reclaimed
Reclaiming --停止失败--> Ready            （回滚，记录 ReclaimErr，可重试）
Reclaimed --Acquire(同名)--> Provisioning （替换为新实例，端口可复用）
```

关键不变式：

- **并发收敛**：所有入口共用一把互斥锁，并发申请同名环境只会创建一个
  实例，其余申请者收敛到同一环境（`AcquireResult.Created=false`）。
- **幂等**：重复申请返回同一环境；重复回收是空操作；后端 `Stop` 幂等。
- **不留半成品**：新建时任一步骤（配额/端口/后端启动）失败都会回滚
  已分配资源；回收失败回滚状态并记录原因，可安全重试。

## 隔离与配额规则

| 规则 | 实现 | 失败原因码 |
|---|---|---|
| 命名隔离 | 环境以名称作为唯一键；同名申请收敛复用 | — |
| 命名冲突 | 同名环境处于 `Reclaiming` 时拒绝新申请 | `NAME_CONFLICT` |
| 端口隔离 | 端口从 `[PortBase, PortEnd]` 池独占分配，回收后归还 | `PORT_EXHAUSTED` |
| 配额控制 | 未回收环境数不得超过 `MaxEnvs` | `QUOTA_EXCEEDED` |
| 占用保护 | 仍有占用者的环境禁止销毁/回收 | `ENV_IN_USE` |
| 释放校验 | 释放者必须确实占用该环境 | `NOT_HELD` / `NOT_FOUND` |
| 后端故障 | 进程启动/停止失败 | `BACKEND_ERROR`（可重试） |

所有错误都是带 `Code` 的 `*preview.Error`，调用方可用 `errors.As`
区分失败原因；每次判定都会打印日志，包含申请标识（req）、状态迁移
和判定依据，不存在静默失败或半完成状态。

## 回收语义

- `Reclaim` 只回收**已过期且空闲**的环境；已过期但仍在使用的环境会被
  跳过（日志注明依据），不受影响。
- 回收失败：状态回滚为 `Ready`，失败原因记入 `Environment.ReclaimErr`，
  下一次 `Reclaim` 可重试；重试成功后原因清空。
- 回收后再申请同名环境：稳定地得到新实例（`Created=true`），端口被复用。

## 本地验证

```bash
# 运行全部测试（含并发申请、释放中占用、过期回收、失败重试）
go test ./... -v

# 竞态检测
go test ./... -race

# 端到端演示：真实启动本地进程作为环境实例
go run ./cmd/previewctl -ttl 5s
```

测试与演示日志均打印申请标识（`req=...`）、状态迁移
（`state=Ready->Reclaiming`）与判定依据（新建/复用/拒绝原因码）。

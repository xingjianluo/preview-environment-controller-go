// Package preview 提供本地预览环境的编排能力：
// 申请、占用、释放、回收的全生命周期管理。
package preview

import (
	"fmt"
	"time"
)

// State 是环境生命周期状态机中的状态。
//
// 状态迁移图：
//
//	(无) --Acquire--> Provisioning --后端就绪--> Ready
//	Ready --Release(最后一个占用者离开)--> Ready(空闲, 等待过期)
//	Ready(已过期且无占用) --Reclaim--> Reclaiming --后端停止成功--> Reclaimed
//	Reclaiming --后端停止失败--> Ready (可重试, 幂等)
//	Ready(无占用) --Destroy--> Reclaiming --> Reclaimed
type State string

const (
	StateProvisioning State = "Provisioning"
	StateReady        State = "Ready"
	StateReclaiming   State = "Reclaiming"
	StateReclaimed    State = "Reclaimed"
)

// Environment 表示一个预览环境实例。
type Environment struct {
	Name       string               // 环境唯一标识（命名隔离的键）
	Port       int                  // 分配到的本地端口
	State      State                // 当前状态
	Holders    []string             // 当前占用者标识列表
	CreatedAt  time.Time            // 创建时间
	ExpiresAt  time.Time            // 环境初始有效期（创建时间 + TTL），续期不改变该值
	Leases     map[string]time.Time // 各占用者自己的租约到期时间（续期只影响对应键）
	ReleasedAt time.Time            // 最后一个占用者离开的时间；有人占用时为零值
	ReclaimErr string               // 最近一次回收失败原因（用于解释与重试）

	handle *Handle // 后端实例句柄（未导出，由控制器管理）
}

// HasHolder 报告 holder 是否占用了该环境。
func (e *Environment) HasHolder(holder string) bool {
	for _, h := range e.Holders {
		if h == holder {
			return true
		}
	}
	return false
}

// InUse 报告环境当前是否被占用。
func (e *Environment) InUse() bool { return len(e.Holders) > 0 }

// Expired 报告环境在 now 时刻是否已过期。
func (e *Environment) Expired(now time.Time) bool { return !now.Before(e.ExpiresAt) }

// ErrCode 是可区分的失败原因码。
type ErrCode string

const (
	ErrNameConflict  ErrCode = "NAME_CONFLICT"  // 命名冲突（同名环境处于不可复用状态）
	ErrPortExhausted ErrCode = "PORT_EXHAUSTED" // 端口池耗尽
	ErrQuotaExceeded ErrCode = "QUOTA_EXCEEDED" // 配额耗尽
	ErrEnvInUse      ErrCode = "ENV_IN_USE"     // 环境正在使用中，禁止释放/销毁
	ErrNotFound      ErrCode = "NOT_FOUND"      // 环境不存在
	ErrNotHeld       ErrCode = "NOT_HELD"       // 调用方并未占用该环境
	ErrBackend       ErrCode = "BACKEND_ERROR"  // 后端（进程）操作失败，可重试
	ErrLeaseExpired  ErrCode = "LEASE_EXPIRED"  // 已错过续期时间点（租约已到期，不能再续期）
	ErrShuttingDown  ErrCode = "SHUTTING_DOWN"  // 服务正在停机收尾，不再接受新申请
)

// Error 是带原因码的错误，调用方可区分失败原因。
type Error struct {
	Code ErrCode
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Msg) }

func newError(code ErrCode, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// AcquireResult 是申请结果，携带判定依据便于解释。
type AcquireResult struct {
	Env       *Environment
	Created   bool   // true 表示本次新建；false 表示收敛到已有环境
	Rationale string // 判定依据（新建/复用/并发收敛）
}

// RenewResult 是租约续期结果。
type RenewResult struct {
	Env        *Environment
	OldExpires time.Time // 续期前该占用者自己的租约到期时间
	NewExpires time.Time // 续期后该占用者自己的租约到期时间
	Rationale  string    // 判定依据
}

// ReclaimSkipped 记录一次回收扫描中被跳过的环境及原因。
type ReclaimSkipped struct {
	Name   string
	Reason string
}

// ReclaimFailure 记录一次回收扫描中回收失败的环境及原因（可安全重试）。
type ReclaimFailure struct {
	Name string
	Err  string
}

// ReclaimReport 是一次空闲回收扫描的完整结论。
// 回收失败体现在 Failures 中而不是静默丢弃，调用方可据此安全重试。
type ReclaimReport struct {
	Reclaimed []string         // 本次成功回收的环境
	Skipped   []ReclaimSkipped // 被跳过的环境（仍在使用/空闲不足阈值等）及依据
	Failures  []ReclaimFailure // 回收失败的环境（状态已回滚，可重试）
}

// Succeeded 报告本次扫描是否全部处理成功（没有回收失败）。
func (r *ReclaimReport) Succeeded() bool { return len(r.Failures) == 0 }

// HasFailures 报告本次扫描是否存在回收失败。
func (r *ReclaimReport) HasFailures() bool { return len(r.Failures) > 0 }

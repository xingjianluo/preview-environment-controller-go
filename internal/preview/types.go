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
	Name       string    // 环境唯一标识（命名隔离的键）
	Port       int       // 分配到的本地端口
	State      State     // 当前状态
	Holders    []string  // 当前占用者标识列表
	CreatedAt  time.Time // 创建时间
	ExpiresAt  time.Time // 环境整体到期时间（取各占用者租约到期时间的最大值）
	ReclaimErr string    // 最近一次回收失败原因（用于解释与重试）

	// LeaseExpiry 记录每个占用者各自的租约到期时间；续期只延长调用方自己
	// 那一份，不影响其他占用者。
	LeaseExpiry map[string]time.Time
	// IdleAt 是最后一个占用者离开的时刻；零值表示当前仍在使用。
	// 空闲回收从该时刻起算 IdleTTL。
	IdleAt time.Time

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

// Expired 报告环境在 now 时刻是否已超过整体到期时间。
// 注意：已过期但仍被占用的环境不会被回收，依旧照常可用。
func (e *Environment) Expired(now time.Time) bool { return !now.Before(e.ExpiresAt) }

// LeaseExpired 报告 holder 自己那份租约在 now 时刻是否已到期。
func (e *Environment) LeaseExpired(holder string, now time.Time) bool {
	return !now.Before(e.LeaseExpiry[holder])
}

// Idle 报告环境当前是否空闲（没有任何占用者）。
func (e *Environment) Idle() bool { return len(e.Holders) == 0 }

// ErrCode 是可区分的失败原因码。
type ErrCode string

const (
	ErrNameConflict    ErrCode = "NAME_CONFLICT"    // 命名冲突（同名环境处于不可复用状态）
	ErrPortExhausted   ErrCode = "PORT_EXHAUSTED"   // 端口池耗尽
	ErrQuotaExceeded   ErrCode = "QUOTA_EXCEEDED"   // 配额耗尽
	ErrEnvInUse        ErrCode = "ENV_IN_USE"       // 环境正在使用中，禁止释放/销毁
	ErrNotFound        ErrCode = "NOT_FOUND"        // 环境不存在
	ErrNotHeld         ErrCode = "NOT_HELD"         // 释放者并未占用该环境
	ErrBackend         ErrCode = "BACKEND_ERROR"    // 后端（进程）操作失败，可重试
	ErrLeaseExpired    ErrCode = "LEASE_EXPIRED"    // 租约已到期，错过可续期的时间点
	ErrServiceDraining ErrCode = "SERVICE_DRAINING" // 服务正在停机收尾，不再接受该类请求
	ErrBadRequest      ErrCode = "BAD_REQUEST"      // 入口参数非法
)

// 回收判定依据（写入 ReclaimItem.Why，用于解释“为什么回收/跳过”）。
const (
	WhyIdleExpired   = "idle-expired"         // 空闲时间超过阈值，回收
	WhyBusy          = "busy"                 // 仍有占用者，跳过
	WhyIdleNotEnough = "idle-not-long-enough" // 已空闲但未满空闲阈值，跳过
	WhyNotExpired    = "not-idle"             // 保留口径：尚未进入空闲
)

// ReclaimItem 描述单个环境的一次回收判定结果。
type ReclaimItem struct {
	Name      string `json:"name"`          // 环境名
	Reclaimed bool   `json:"reclaimed"`     // 本次是否真正完成回收
	Failed    bool   `json:"failed"`        // 尝试回收但失败（可安全重试）
	Err       string `json:"err,omitempty"` // 失败原因
	Why       string `json:"why"`           // 判定依据（回收/跳过原因）
}

// ReclaimReport 是一次回收扫描的完整报告：
// 成功列表、失败列表（对外可见、可重试）与每个环境的判定依据。
type ReclaimReport struct {
	Reclaimed []string      // 成功回收的环境名
	Failures  []ReclaimItem // 回收失败的环境（不报成功，可重试）
	Skipped   []ReclaimItem // 被跳过的环境及原因
}

// HasFailure 报告本次扫描是否存在回收失败的环境。
func (r *ReclaimReport) HasFailure() bool { return r != nil && len(r.Failures) > 0 }

// AllItems 按环境名汇总全部判定项，便于入口逐个解释。
func (r *ReclaimReport) AllItems() []ReclaimItem {
	items := make([]ReclaimItem, 0, len(r.Reclaimed)+len(r.Failures)+len(r.Skipped))
	for _, n := range r.Reclaimed {
		items = append(items, ReclaimItem{Name: n, Reclaimed: true, Why: WhyIdleExpired})
	}
	items = append(items, r.Failures...)
	items = append(items, r.Skipped...)
	return items
}

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

package preview

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Config 是控制器配置。
type Config struct {
	MaxEnvs  int           // 配额：最大并存（未回收）环境数
	PortBase int           // 端口池起始（含）
	PortEnd  int           // 端口池结束（含）
	TTL      time.Duration // 单个占用者租约的默认有效期；申请时据此发放租约
	IdleTTL  time.Duration // 空闲回收阈值：最后一个占用者离开后多久可被回收；为 0 时取 TTL
}

// Controller 编排预览环境的申请、释放、续期与回收。
// 所有入口共用同一把互斥锁，因此并发申请同一环境只会创建一个实例，
// 各入口观察到一致的状态与失败原因。
type Controller struct {
	mu       sync.Mutex
	cfg      Config
	backend  Backend
	logger   *log.Logger
	draining bool // 停机收尾标志：置位后拒绝一切新申请

	envs    map[string]*Environment // 按名称隔离
	ports   map[int]string          // 端口 -> 环境名
	handles map[string]*Handle      // 环境名 -> 后端句柄
	now     func() time.Time        // 可注入时钟，便于测试
}

// New 创建控制器。
func New(cfg Config, b Backend, logger *log.Logger) *Controller {
	if logger == nil {
		logger = log.Default()
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = cfg.TTL
	}
	return &Controller{
		cfg:     cfg,
		backend: b,
		logger:  logger,
		envs:    map[string]*Environment{},
		ports:   map[int]string{},
		handles: map[string]*Handle{},
		now:     time.Now,
	}
}

// SetClock 注入时钟（仅测试用）。
func (c *Controller) SetClock(now func() time.Time) { c.now = now }

func (c *Controller) logf(requestID, format string, args ...any) {
	c.logger.Printf("[req=%s] "+format, append([]any{requestID}, args...)...)
}

// Acquire 申请名为 name 的环境并登记占用者 holder。
//
// 幂等与并发收敛：同名且未回收的环境直接复用，不会重复创建；
// 已回收的环境会被替换为新实例。失败原因以 *Error 的 Code 区分。
func (c *Controller) Acquire(requestID, name, holder string) (*AcquireResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.draining {
		c.logf(requestID, "acquire name=%s holder=%s -> 拒绝 code=%s 依据: 服务停机收尾中，不接受新申请",
			name, holder, ErrShuttingDown)
		return nil, newError(ErrShuttingDown, "服务正在停机收尾，不再接受新的环境申请")
	}

	if env, ok := c.envs[name]; ok && env.State != StateReclaimed {
		if env.State == StateReclaiming {
			c.logf(requestID, "acquire name=%s holder=%s -> 拒绝 code=%s 依据: 同名环境正在回收中",
				name, holder, ErrNameConflict)
			return nil, newError(ErrNameConflict, "环境 %q 正在回收中，禁止同名申请", name)
		}
		if !env.HasHolder(holder) {
			env.Holders = append(env.Holders, holder)
		}
		// 无论新增占用者还是旧占用者再次申请，都（重新）发放一份完整租约：
		// 错过续期窗口的占用者可通过重新申请拿回使用权。
		env.Leases[holder] = c.now().Add(c.cfg.TTL)
		env.ReleasedAt = time.Time{}
		rationale := "复用已有环境（同名申请收敛，未重复创建；已为该占用者发放/续发租约）"
		c.logf(requestID, "acquire name=%s holder=%s -> 复用 state=%s port=%d holders=%v lease_until=%s 依据: %s",
			name, holder, env.State, env.Port, env.Holders, env.Leases[holder].Format(time.RFC3339), rationale)
		return &AcquireResult{Env: cloneEnv(env), Created: false, Rationale: rationale}, nil
	}

	// 新建路径：先查配额，再分配端口，最后启动后端；任一步失败都不留半成品。
	if n := c.activeCountLocked(); n >= c.cfg.MaxEnvs {
		c.logf(requestID, "acquire name=%s holder=%s -> 拒绝 code=%s 当前活跃=%d 配额=%d",
			name, holder, ErrQuotaExceeded, n, c.cfg.MaxEnvs)
		return nil, newError(ErrQuotaExceeded, "活跃环境数 %d 已达配额上限 %d", n, c.cfg.MaxEnvs)
	}
	port, ok := c.allocPortLocked()
	if !ok {
		c.logf(requestID, "acquire name=%s holder=%s -> 拒绝 code=%s 端口池=[%d,%d]",
			name, holder, ErrPortExhausted, c.cfg.PortBase, c.cfg.PortEnd)
		return nil, newError(ErrPortExhausted, "端口池 [%d,%d] 已耗尽", c.cfg.PortBase, c.cfg.PortEnd)
	}

	now := c.now()
	env := &Environment{
		Name:      name,
		Port:      port,
		State:     StateProvisioning,
		Holders:   []string{holder},
		CreatedAt: now,
		ExpiresAt: now.Add(c.cfg.TTL),
		Leases:    map[string]time.Time{holder: now.Add(c.cfg.TTL)},
	}
	c.logf(requestID, "acquire name=%s holder=%s -> 新建 state=%s port=%d lease_until=%s",
		name, holder, env.State, port, env.Leases[holder].Format(time.RFC3339))

	h, err := c.backend.Start(env)
	if err != nil {
		delete(c.ports, port) // 回滚端口，不留半创建状态
		c.logf(requestID, "acquire name=%s -> 失败 code=%s err=%v（端口已回滚）", name, ErrBackend, err)
		return nil, newError(ErrBackend, "启动环境实例失败: %v", err)
	}

	env.State = StateReady
	c.envs[name] = env
	c.handles[name] = h
	rationale := "新建环境（首次申请或前序环境已回收）"
	c.logf(requestID, "acquire name=%s holder=%s -> 就绪 state=%s->%s port=%d pid=%d 依据: %s",
		name, holder, StateProvisioning, StateReady, port, h.PID, rationale)
	return &AcquireResult{Env: cloneEnv(env), Created: true, Rationale: rationale}, nil
}

// Release 解除 holder 对环境的占用。环境本身保留，等待过期回收。
func (c *Controller) Release(requestID, name, holder string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	env, ok := c.envs[name]
	if !ok || env.State == StateReclaimed {
		c.logf(requestID, "release name=%s holder=%s -> 拒绝 code=%s", name, holder, ErrNotFound)
		return newError(ErrNotFound, "环境 %q 不存在或已回收", name)
	}
	if !env.HasHolder(holder) {
		c.logf(requestID, "release name=%s holder=%s -> 拒绝 code=%s holders=%v",
			name, holder, ErrNotHeld, env.Holders)
		return newError(ErrNotHeld, "holder %q 并未占用环境 %q", holder, name)
	}
	next := env.Holders[:0]
	for _, h := range env.Holders {
		if h != holder {
			next = append(next, h)
		}
	}
	env.Holders = next
	delete(env.Leases, holder)
	if !env.InUse() {
		// 最后一个占用者离开：空闲计时从这一刻开始，而不是从环境创建/租约到期开始。
		env.ReleasedAt = c.now()
	}
	c.logf(requestID, "release name=%s holder=%s -> 成功 state=%s 剩余占用=%v idle_since=%s",
		name, holder, env.State, env.Holders, formatIdleSince(env.ReleasedAt))
	return nil
}

// Renew 为 holder 自己的租约续期 extend 时长。
//
// 续期只延长该占用者在 Leases 中对应的到期时间，不影响环境上的其他占用者，
// 也不改变环境的初始有效期（ExpiresAt）与空闲计时。
//
// 三类可互相区分的拒绝原因：
//   - ErrNotFound：环境不存在或已回收（续期不可能生效）；
//   - ErrNotHeld：该调用方并未占用该环境；
//   - ErrLeaseExpired：已错过续期时间点（租约到期时刻起不再接受续期）。
//
// 任一拒绝都不会修改任何状态（先判定、后写入，原子完成，不会改到一半）。
// extend <= 0 时按配置的默认租约时长续期。
func (c *Controller) Renew(requestID, name, holder string, extend time.Duration) (*RenewResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	env, ok := c.envs[name]
	if !ok || env.State == StateReclaimed {
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s", name, holder, ErrNotFound)
		return nil, newError(ErrNotFound, "环境 %q 不存在或已回收，无法续期", name)
	}
	if env.State == StateReclaiming {
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s 依据: 环境正在回收中", name, holder, ErrNotFound)
		return nil, newError(ErrNotFound, "环境 %q 正在回收中，无法续期", name)
	}
	old, held := env.Leases[holder]
	if !held || !env.HasHolder(holder) {
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s holders=%v", name, holder, ErrNotHeld, env.Holders)
		return nil, newError(ErrNotHeld, "调用方 %q 并未占用环境 %q，不能续期", holder, name)
	}
	now := c.now()
	if !now.Before(old) { // 到期时刻即为临界点：到期(含)之后一律拒绝续期
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s now=%s lease_until=%s 依据: 已错过续期时间点",
			name, holder, ErrLeaseExpired, now.Format(time.RFC3339), old.Format(time.RFC3339))
		return nil, newError(ErrLeaseExpired, "环境 %q 的占用方 %q 租约已于 %s 到期，错过续期窗口（可重新申请）",
			name, holder, old.Format(time.RFC3339))
	}
	if extend <= 0 {
		extend = c.cfg.TTL
	}
	newExpires := old.Add(extend)
	env.Leases[holder] = newExpires // 只改自己的键，其他占用者不受影响
	c.logf(requestID, "renew name=%s holder=%s -> 成功 lease=%s->%s extend=%s holders=%v",
		name, holder, old.Format(time.RFC3339), newExpires.Format(time.RFC3339), extend, env.Holders)
	return &RenewResult{
		Env:        cloneEnv(env),
		OldExpires: old,
		NewExpires: newExpires,
		Rationale:  fmt.Sprintf("仅延长占用方 %q 自身租约 %s，其他占用方不受影响", holder, extend),
	}, nil
}

// Destroy 强制销毁空闲环境。正在使用中的环境拒绝销毁（ErrEnvInUse）。
func (c *Controller) Destroy(requestID, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	env, ok := c.envs[name]
	if !ok || env.State == StateReclaimed {
		c.logf(requestID, "destroy name=%s -> 拒绝 code=%s", name, ErrNotFound)
		return newError(ErrNotFound, "环境 %q 不存在或已回收", name)
	}
	if env.InUse() {
		c.logf(requestID, "destroy name=%s -> 拒绝 code=%s holders=%v", name, ErrEnvInUse, env.Holders)
		return newError(ErrEnvInUse, "环境 %q 仍被 %v 占用，禁止销毁", name, env.Holders)
	}
	if err := c.reclaimLocked(requestID, env, "destroy"); err != nil {
		return err
	}
	return nil
}

// Reclaim 回收所有空闲超过阈值的环境；仍在使用的环境跳过。
// 回收失败会记录在环境的 ReclaimErr 上并保持原状态，可安全重试；
// 对已回收环境重复回收是幂等空操作。
//
// 兼容说明：该签名保持不变，只返回成功回收的环境名；失败明细请用
// ReclaimIdle 获取（失败不会让本方法整体返回错误，也不会被伪装成成功）。
func (c *Controller) Reclaim(requestID string) ([]string, error) {
	report := c.ReclaimIdle(requestID)
	return report.Reclaimed, nil
}

// ReclaimIdle 执行一次基于空闲时间的回收扫描并返回完整结论：
//
//   - 空闲从最后一个占用者离开（ReleasedAt）起算，空闲超过 IdleTTL 才可回收；
//   - 仍在被使用的环境（即使早过了初始有效期）一律跳过并给出依据；
//   - 回收失败计入 report.Failures，状态回滚、原因落 ReclaimErr，可安全重试，
//     绝不会对外表现为“回收成功”。
func (c *Controller) ReclaimIdle(requestID string) *ReclaimReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepLocked(requestID, false)
}

// sweepLocked 执行回收扫描。ignoreIdle=true（停机路径）表示忽略空闲阈值，
// 立即回收所有空闲环境；正在使用的仍然跳过。
func (c *Controller) sweepLocked(requestID string, ignoreIdle bool) *ReclaimReport {
	now := c.now()
	report := &ReclaimReport{}
	for _, env := range c.envs {
		if env.State == StateReclaimed {
			continue // 幂等：已回收的直接跳过
		}
		if env.State == StateReclaiming {
			report.Skipped = append(report.Skipped, ReclaimSkipped{
				Name: env.Name, Reason: "环境处于回收中状态（上一轮回收尚未收敛），本次跳过",
			})
			continue
		}
		if env.InUse() {
			report.Skipped = append(report.Skipped, ReclaimSkipped{
				Name:   env.Name,
				Reason: fmt.Sprintf("仍在使用中，holders=%v（即使超过初始有效期也跳过）", env.Holders),
			})
			c.logf(requestID, "reclaim name=%s -> 跳过 依据: 仍在使用 holders=%v", env.Name, env.Holders)
			continue
		}
		idleSince := env.ReleasedAt
		if idleSince.IsZero() {
			idleSince = env.CreatedAt // 防御：理论上空闲环境一定有离开时间
		}
		idleFor := now.Sub(idleSince)
		if !ignoreIdle && idleFor < c.cfg.IdleTTL {
			reason := fmt.Sprintf("空闲时长 %s 未达阈值 %s", idleFor.Round(time.Millisecond), c.cfg.IdleTTL)
			report.Skipped = append(report.Skipped, ReclaimSkipped{Name: env.Name, Reason: reason})
			c.logf(requestID, "reclaim name=%s -> 跳过 依据: %s idle_since=%s",
				env.Name, reason, idleSince.Format(time.RFC3339))
			continue
		}
		if err := c.reclaimLocked(requestID, env, "idle-timeout"); err != nil {
			c.logf(requestID, "reclaim name=%s -> 失败(可重试) err=%v", env.Name, err)
			report.Failures = append(report.Failures, ReclaimFailure{Name: env.Name, Err: err.Error()})
			continue
		}
		report.Reclaimed = append(report.Reclaimed, env.Name)
	}
	return report
}

// reclaimLocked 执行状态迁移 Ready->Reclaiming->Reclaimed 并释放端口。
// 失败时回滚到 Ready 并记录原因，保证可重试且不留半回收状态。
func (c *Controller) reclaimLocked(requestID string, env *Environment, reason string) error {
	prev := env.State
	env.State = StateReclaiming
	c.logf(requestID, "reclaim name=%s -> state=%s->%s 依据: %s", env.Name, prev, StateReclaiming, reason)

	if err := c.backend.Stop(c.handles[env.Name]); err != nil {
		env.State = prev
		env.ReclaimErr = err.Error()
		c.logf(requestID, "reclaim name=%s -> 回滚 state=%s->%s code=%s err=%v",
			env.Name, StateReclaiming, prev, ErrBackend, err)
		return newError(ErrBackend, "回收环境 %q 失败（可重试）: %v", env.Name, err)
	}

	env.State = StateReclaimed
	env.ReclaimErr = ""
	delete(c.ports, env.Port)
	delete(c.handles, env.Name)
	c.logf(requestID, "reclaim name=%s -> state=%s->%s port=%d 已释放", env.Name, StateReclaiming, StateReclaimed, env.Port)
	return nil
}

// Get 查询环境快照。
func (c *Controller) Get(name string) (*Environment, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	env, ok := c.envs[name]
	if !ok {
		return nil, false
	}
	return cloneEnv(env), true
}

// activeCountLocked 统计未回收的环境数（配额口径）。
func (c *Controller) activeCountLocked() int {
	n := 0
	for _, e := range c.envs {
		if e.State != StateReclaimed {
			n++
		}
	}
	return n
}

// allocPortLocked 从端口池分配空闲端口。
func (c *Controller) allocPortLocked() (int, bool) {
	for p := c.cfg.PortBase; p <= c.cfg.PortEnd; p++ {
		if _, used := c.ports[p]; !used {
			c.ports[p] = ""
			return p, true
		}
	}
	return 0, false
}

func cloneEnv(e *Environment) *Environment {
	cp := *e
	cp.Holders = append([]string(nil), e.Holders...)
	if e.Leases != nil {
		cp.Leases = make(map[string]time.Time, len(e.Leases))
		for h, t := range e.Leases {
			cp.Leases[h] = t
		}
	}
	cp.handle = nil
	return &cp
}

func formatIdleSince(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}

// BeginDrain 进入停机收尾：此后所有新申请都会被稳定地拒绝（ErrShuttingDown），
// 重复触发是幂等的。进行中的请求不受影响，仍拿到一致结论。
func (c *Controller) BeginDrain(requestID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.draining {
		c.draining = true
		c.logf(requestID, "shutdown -> 开始收尾：停止接受新申请，等待进行中请求与占用者离开")
	} else {
		c.logf(requestID, "shutdown -> 重复触发，收尾仍在进行（幂等空操作）")
	}
}

// IsDraining 报告是否已进入停机收尾。
func (c *Controller) IsDraining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.draining
}

// Drain 执行停机收尾：不再接受新申请（BeginDrain 的效果），
// 等待占用者释放并持续回收空闲环境；只要 ctx 未到期，环境仍被使用就继续等待。
//
// ctx 到期后进入强制阶段：停止全部仍残留的后端实例，保证退出后不留后台进程；
// 强制阶段仍失败的环境会体现在返回的报告/错误中（绝不谎报成功）。
// 重复调用安全：第二次起若已无活跃环境则立即返回空报告。
func (c *Controller) Drain(ctx context.Context, requestID string) (*ReclaimReport, error) {
	c.BeginDrain(requestID)
	report := &ReclaimReport{}

	// 阶段一：优雅等待。占用者离开后立即回收（忽略空闲阈值）；仍在使用则继续等。
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		r := c.sweepIdleNowLocked(requestID)
		report = mergeReports(report, r)
		if c.activeHandleCountLocked() == 0 {
			c.logf(requestID, "shutdown -> 收尾完成：全部自管实例已停止 reclaimed=%v failures=%d",
				report.Reclaimed, len(report.Failures))
			return report, nil
		}
		select {
		case <-ctx.Done():
			c.logf(requestID, "shutdown -> 等待超时（仍有占用/残留），进入强制停止阶段")
			return c.forceStopAllLocked(ctx, requestID, report)
		case <-poll.C:
		}
	}
}

// sweepIdleNowLocked 忽略空闲阈值回收所有当前空闲的环境。
func (c *Controller) sweepIdleNowLocked(requestID string) *ReclaimReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sweepLocked(requestID, true)
}

// forceStopAllLocked 强制停止所有仍存活的后端实例（包括仍被占用的环境），
// 保证退出后不留后台进程。
func (c *Controller) forceStopAllLocked(ctx context.Context, requestID string, accumulated *ReclaimReport) (*ReclaimReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var firstErr error
	for _, env := range c.envs {
		if env.State == StateReclaimed {
			continue
		}
		if err := c.reclaimLocked(requestID, env, "shutdown-force"); err != nil {
			c.logf(requestID, "shutdown -> 强制停止失败 name=%s err=%v", env.Name, err)
			accumulated.Failures = append(accumulated.Failures, ReclaimFailure{Name: env.Name, Err: err.Error()})
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		accumulated.Reclaimed = append(accumulated.Reclaimed, env.Name)
	}
	_ = ctx
	if firstErr != nil {
		return accumulated, fmt.Errorf("停机强制停止阶段有实例未能停止（仍可能残留后台进程）: %w", firstErr)
	}
	c.logf(requestID, "shutdown -> 强制停止完成：残留实例全部停止 reclaimed=%v", accumulated.Reclaimed)
	return accumulated, nil
}

// activeHandleCountLocked 返回仍持有后端句柄（实例在跑）的环境数。
func (c *Controller) activeHandleCountLocked() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.handles)
}

func mergeReports(a, b *ReclaimReport) *ReclaimReport {
	a.Reclaimed = append(a.Reclaimed, b.Reclaimed...)
	a.Skipped = append(a.Skipped, b.Skipped...)
	a.Failures = append(a.Failures, b.Failures...)
	return a
}

// List 返回全部环境快照（含已回收记录），供常驻服务的查询入口使用。
func (c *Controller) List() []*Environment {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*Environment, 0, len(c.envs))
	for _, env := range c.envs {
		out = append(out, cloneEnv(env))
	}
	return out
}

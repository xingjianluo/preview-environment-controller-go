package preview

import (
	"log"
	"sync"
	"time"
)

// Config 是控制器配置。
type Config struct {
	MaxEnvs  int           // 配额：最大并存（未回收）环境数
	PortBase int           // 端口池起始（含）
	PortEnd  int           // 端口池结束（含）
	TTL      time.Duration // 环境自创建起的过期时长
}

// Controller 编排预览环境的申请、释放与回收。
// 所有入口共用同一把互斥锁，因此并发申请同一环境只会创建一个实例，
// 各入口观察到一致的状态。
type Controller struct {
	mu      sync.Mutex
	cfg     Config
	backend Backend
	logger  *log.Logger

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

	if env, ok := c.envs[name]; ok && env.State != StateReclaimed {
		if env.State == StateReclaiming {
			c.logf(requestID, "acquire name=%s holder=%s -> 拒绝 code=%s 依据: 同名环境正在回收中",
				name, holder, ErrNameConflict)
			return nil, newError(ErrNameConflict, "环境 %q 正在回收中，禁止同名申请", name)
		}
		if !env.HasHolder(holder) {
			env.Holders = append(env.Holders, holder)
		}
		rationale := "复用已有环境（同名申请收敛，未重复创建）"
		c.logf(requestID, "acquire name=%s holder=%s -> 复用 state=%s port=%d holders=%v 依据: %s",
			name, holder, env.State, env.Port, env.Holders, rationale)
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
	}
	c.logf(requestID, "acquire name=%s holder=%s -> 新建 state=%s port=%d", name, holder, env.State, port)

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
	c.logf(requestID, "release name=%s holder=%s -> 成功 state=%s 剩余占用=%v", name, holder, env.State, env.Holders)
	return nil
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

// Reclaim 回收所有已过期且空闲的环境；仍在使用的过期环境跳过。
// 回收失败会记录在环境的 ReclaimErr 上并保持原状态，可安全重试；
// 对已回收环境重复回收是幂等空操作。
func (c *Controller) Reclaim(requestID string) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	var reclaimed []string
	for _, env := range c.envs {
		if env.State == StateReclaimed {
			continue // 幂等：已回收的直接跳过
		}
		if !env.Expired(now) {
			continue
		}
		if env.InUse() {
			c.logf(requestID, "reclaim name=%s -> 跳过 依据: 已过期但仍在使用 holders=%v", env.Name, env.Holders)
			continue
		}
		if err := c.reclaimLocked(requestID, env, "ttl-expired"); err != nil {
			c.logf(requestID, "reclaim name=%s -> 失败(可重试) err=%v", env.Name, err)
			continue
		}
		reclaimed = append(reclaimed, env.Name)
	}
	return reclaimed, nil
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
	cp.handle = nil
	return &cp
}

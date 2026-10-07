package preview

import (
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

// fakeBackend 记录启动次数，并可注入停止失败以模拟回收失败。
type fakeBackend struct {
	mu        sync.Mutex
	starts    int
	stops     int
	stopErr   error
	stopCalls int
}

func (f *fakeBackend) Start(env *Environment) (*Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	return &Handle{PID: 1000 + f.starts}, nil
}

func (f *fakeBackend) Stop(h *Handle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopCalls++
	if f.stopErr != nil {
		return f.stopErr
	}
	f.stops++
	return nil
}

func (f *fakeBackend) setStopErr(err error) { f.mu.Lock(); f.stopErr = err; f.mu.Unlock() }
func (f *fakeBackend) startCount() int      { f.mu.Lock(); defer f.mu.Unlock(); return f.starts }

func newTestController(t *testing.T, cfg Config) (*Controller, *fakeBackend) {
	t.Helper()
	logger := log.New(os.Stderr, "[preview-test] ", log.Lmicroseconds)
	fb := &fakeBackend{}
	return New(cfg, fb, logger), fb
}

func testCfg() Config {
	return Config{MaxEnvs: 4, PortBase: 18000, PortEnd: 18003, TTL: time.Minute, IdleTTL: 30 * time.Second}
}

func mustCode(t *testing.T, err error, want ErrCode) {
	t.Helper()
	var pe *Error
	if !errors.As(err, &pe) {
		t.Fatalf("期望 *Error，实际 %v", err)
	}
	if pe.Code != want {
		t.Fatalf("期望错误码 %s，实际 %s (%v)", want, pe.Code, err)
	}
	t.Logf("判定依据: 错误码=%s 消息=%v", pe.Code, err)
}

// 并发申请同一环境：只允许创建一个实例，全部收敛到同一端口。
func TestConcurrentAcquireConverges(t *testing.T) {
	c, fb := newTestController(t, testCfg())
	const n = 16
	var wg sync.WaitGroup
	results := make([]*AcquireResult, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reqID := "conc-" + string(rune('a'+i))
			r, err := c.Acquire(reqID, "env-a", "holder-"+string(rune('a'+i)))
			results[i], errs[i] = r, err
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("第 %d 个并发申请失败: %v", i, err)
		}
		if results[i].Env.Name != "env-a" || results[i].Env.Port != 18000 {
			t.Fatalf("第 %d 个申请未收敛到同一实例: %+v", i, results[i].Env)
		}
	}
	if got := fb.startCount(); got != 1 {
		t.Fatalf("并发申请创建了 %d 个实例，期望 1", got)
	}
	env, _ := c.Get("env-a")
	if len(env.Holders) != n {
		t.Fatalf("占用者数量=%d，期望 %d", len(env.Holders), n)
	}
	t.Logf("判定依据: 后端启动次数=%d，全部申请收敛到 port=%d", fb.startCount(), results[0].Env.Port)
}

// 重复申请幂等：第二次 Created=false。
func TestAcquireIdempotent(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	r1, err := c.Acquire("idem-1", "env-1", "h1")
	if err != nil || !r1.Created {
		t.Fatalf("首次申请应新建: created=%v err=%v", r1.Created, err)
	}
	r2, err := c.Acquire("idem-2", "env-1", "h1")
	if err != nil || r2.Created {
		t.Fatalf("重复申请应复用: created=%v err=%v", r2.Created, err)
	}
	if r1.Env.Port != r2.Env.Port {
		t.Fatalf("重复申请端口不一致: %d vs %d", r1.Env.Port, r2.Env.Port)
	}
}

// 配额与端口耗尽：错误码必须可区分。
func TestQuotaAndPortExhaustion(t *testing.T) {
	c, _ := newTestController(t, Config{MaxEnvs: 1, PortBase: 18000, PortEnd: 18001, TTL: time.Minute})
	if _, err := c.Acquire("q-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	_, err := c.Acquire("q-2", "env-2", "h2")
	mustCode(t, err, ErrQuotaExceeded)

	// 放大配额但不放大端口池 -> 端口耗尽
	c2, _ := newTestController(t, Config{MaxEnvs: 10, PortBase: 18000, PortEnd: 18000, TTL: time.Minute})
	if _, err := c2.Acquire("p-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	_, err = c2.Acquire("p-2", "env-2", "h2")
	mustCode(t, err, ErrPortExhausted)
}

// 释放中的占用规则：销毁使用中环境被拒；未占用者释放被拒。
func TestReleaseAndDestroyWhileInUse(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	if _, err := c.Acquire("r-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("r-2", "env-1", "h2"); err != nil {
		t.Fatal(err)
	}

	mustCode(t, c.Destroy("r-3", "env-1"), ErrEnvInUse)
	mustCode(t, c.Release("r-4", "env-1", "stranger"), ErrNotHeld)
	mustCode(t, c.Release("r-5", "no-such", "h1"), ErrNotFound)

	if err := c.Release("r-6", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	mustCode(t, c.Destroy("r-7", "env-1"), ErrEnvInUse) // h2 仍占用
	if err := c.Release("r-8", "env-1", "h2"); err != nil {
		t.Fatal(err)
	}
	if err := c.Destroy("r-9", "env-1"); err != nil {
		t.Fatalf("空闲后销毁应成功: %v", err)
	}
	env, _ := c.Get("env-1")
	if env.State != StateReclaimed {
		t.Fatalf("销毁后状态=%s，期望 %s", env.State, StateReclaimed)
	}
	t.Logf("判定依据: 全部占用者释放后销毁成功，状态迁移至 %s", env.State)
}

// 过期回收：空闲过期环境被回收，使用中的过期环境不受影响；回收后可再申请。
func TestReclaimExpired(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("rc-1", "idle-env", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("rc-2", "busy-env", "h2"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("rc-3", "idle-env", "h1"); err != nil {
		t.Fatal(err)
	}

	now = base.Add(2 * time.Minute) // 全部过期
	report, err := c.Reclaim("rc-4")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Reclaimed) != 1 || report.Reclaimed[0] != "idle-env" {
		t.Fatalf("回收列表=%v，期望仅 [idle-env]", report.Reclaimed)
	}
	busyItem := report.Skipped[0]
	idle, _ := c.Get("idle-env")
	busy, _ := c.Get("busy-env")
	if idle.State != StateReclaimed {
		t.Fatalf("idle-env 状态=%s，期望 Reclaimed", idle.State)
	}
	if busy.State != StateReady || !busy.InUse() {
		t.Fatalf("busy-env 不应被回收: state=%s holders=%v", busy.State, busy.Holders)
	}
	if busyItem.Name != "busy-env" || busyItem.Why != WhyBusy {
		t.Fatalf("busy-env 跳过依据=%+v，期望 why=%s", busyItem, WhyBusy)
	}
	t.Logf("判定依据: idle-env 已过期且空闲被回收；busy-env 已过期但使用中被跳过(why=%s)", busyItem.Why)

	// 回收后再申请：得到新实例，端口被复用
	r, err := c.Acquire("rc-5", "idle-env", "h3")
	if err != nil || !r.Created {
		t.Fatalf("回收后再申请应新建: created=%v err=%v", r.Created, err)
	}
	if r.Env.Port != 18000 {
		t.Fatalf("回收后端口应被复用，实际 port=%d", r.Env.Port)
	}
}

// 回收失败可重试且幂等：失败后状态回滚、记录原因；重试成功；重复回收是空操作。
func TestReclaimRetryAndIdempotent(t *testing.T) {
	c, fb := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("rt-1", "env-x", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("rt-2", "env-x", "h1"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(2 * time.Minute)

	fb.setStopErr(errors.New("模拟后端故障"))
	report, err := c.Reclaim("rt-3")
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Reclaimed) != 0 {
		t.Fatalf("故障时不应回收成功: %v", report.Reclaimed)
	}
	if len(report.Failures) != 1 {
		t.Fatalf("回收失败应在 Failures 对外可见，实际 %+v", report.Failures)
	}
	env, _ := c.Get("env-x")
	if env.State != StateReady {
		t.Fatalf("回收失败后状态应回滚为 Ready，实际 %s", env.State)
	}
	if env.ReclaimErr == "" {
		t.Fatal("回收失败后应记录 ReclaimErr")
	}
	t.Logf("判定依据: 回收失败回滚 state=Ready，ReclaimErr=%q", env.ReclaimErr)

	fb.setStopErr(nil)
	report, err = c.Reclaim("rt-4")
	if err != nil || len(report.Reclaimed) != 1 {
		t.Fatalf("重试回收应成功: reclaimed=%v err=%v", report.Reclaimed, err)
	}
	env, _ = c.Get("env-x")
	if env.State != StateReclaimed || env.ReclaimErr != "" {
		t.Fatalf("重试后状态=%s ReclaimErr=%q", env.State, env.ReclaimErr)
	}

	// 幂等：再次回收是空操作
	report, err = c.Reclaim("rt-5")
	if err != nil || len(report.Reclaimed) != 0 || len(report.Failures) != 0 {
		t.Fatalf("重复回收应为幂等空操作: report=%+v err=%v", report, err)
	}
	t.Log("判定依据: 重复 Reclaim 对已回收环境为空操作")
}

// 命名冲突：同名环境处于回收中状态时拒绝申请。
func TestNameConflictWhileReclaiming(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	if _, err := c.Acquire("nc-1", "env-c", "h1"); err != nil {
		t.Fatal(err)
	}
	// 同包测试：直接构造回收中状态
	c.mu.Lock()
	c.envs["env-c"].State = StateReclaiming
	c.mu.Unlock()
	_, err := c.Acquire("nc-2", "env-c", "h2")
	mustCode(t, err, ErrNameConflict)
}

// 本地进程后端集成：真实启动/停止 sleep 进程，Stop 幂等。
func TestProcessBackendLifecycle(t *testing.T) {
	b := NewProcessBackend()
	h, err := b.Start(&Environment{Name: "proc-env"})
	if err != nil {
		t.Skipf("无法启动本地进程: %v", err)
	}
	if h.PID <= 0 {
		t.Fatalf("非法 PID: %d", h.PID)
	}
	if err := b.Stop(h); err != nil {
		t.Fatalf("停止进程失败: %v", err)
	}
	if err := b.Stop(h); err != nil {
		t.Fatalf("重复停止应幂等: %v", err)
	}
	t.Logf("判定依据: 进程 pid=%d 启动/停止成功，重复 Stop 幂等", h.PID)
}

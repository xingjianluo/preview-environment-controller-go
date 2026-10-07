package preview

import (
	"testing"
	"time"
)

// 空闲未满阈值：不回收；一旦超过阈值立即回收。
func TestIdleThreshold(t *testing.T) {
	c, _ := newTestController(t, testCfg()) // IdleTTL=30s
	base := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("it-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("it-2", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}

	now = base.Add(29 * time.Second)
	rep, err := c.Reclaim("it-3")
	if err != nil || len(rep.Reclaimed) != 0 || len(rep.Skipped) != 1 {
		t.Fatalf("空闲未满阈值不应回收: %+v err=%v", rep, err)
	}
	if rep.Skipped[0].Why != WhyIdleNotEnough {
		t.Fatalf("跳过依据=%s，期望 %s", rep.Skipped[0].Why, WhyIdleNotEnough)
	}
	t.Logf("判定依据: idle=29s<30s 跳过 why=%s", rep.Skipped[0].Why)

	// 临界：恰好 IdleAt+IdleTTL -> 到期可回收
	now = base.Add(30 * time.Second)
	rep, err = c.Reclaim("it-4")
	if err != nil || len(rep.Reclaimed) != 1 {
		t.Fatalf("到达空闲阈值应回收: %+v err=%v", rep, err)
	}
}

// 超过整体有效期但仍在使用：绝不回收，环境照常可用（可复用、可续期）。
func TestBusyEnvPastExpiryStaysUsable(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("bp-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(10 * time.Minute) // 远超 TTL(1m)，但 h1 仍占用

	rep, err := c.Reclaim("bp-2")
	if err != nil || len(rep.Reclaimed) != 0 || len(rep.Failures) != 0 {
		t.Fatalf("在用环境即使过期也不应回收: %+v err=%v", rep, err)
	}
	if len(rep.Skipped) != 1 || rep.Skipped[0].Why != WhyBusy {
		t.Fatalf("应按 busy 跳过: %+v", rep.Skipped)
	}
	env, _ := c.Get("env-1")
	if env.State != StateReady || !env.InUse() {
		t.Fatalf("在用过期环境应保持 Ready 且可用: state=%s holders=%v", env.State, env.Holders)
	}
	// 个人租约虽已到期，不能续期（LEASE_EXPIRED），但同名再申请可复用、不新建。
	mustCode(t, c.RenewErr("bp-3", "env-1", "h1", time.Second), ErrLeaseExpired)
	r, err := c.Acquire("bp-4", "env-1", "h2")
	if err != nil || r.Created {
		t.Fatalf("过期但在用的环境应可复用: created=%v err=%v", r != nil && r.Created, err)
	}
	t.Logf("判定依据: 过期但 holders=%v 跳过(why=busy)，环境 state=Ready 仍可复用", env.Holders)
}

// 全部离开后又有人回来：空闲计时清零；再次全部离开后从新的离开点起算。
func TestIdleRestartedByRejoin(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("ij-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("ij-2", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	firstIdleAt := c.envs["env-1"].IdleAt

	now = base.Add(20 * time.Second)                            // 空闲 20s（未满 30s）
	if _, err := c.Acquire("ij-3", "env-1", "h9"); err != nil { // h9 回来
		t.Fatal(err)
	}
	if !c.envs["env-1"].IdleAt.IsZero() {
		t.Fatal("重新被占用后 IdleAt 应清零")
	}
	now = base.Add(25 * time.Second)
	if err := c.Release("ij-4", "env-1", "h9"); err != nil {
		t.Fatal(err)
	}
	secondIdleAt := c.envs["env-1"].IdleAt
	if !secondIdleAt.After(firstIdleAt) {
		t.Fatalf("二次空闲起点应晚于首次: first=%s second=%s", firstIdleAt, secondIdleAt)
	}

	// 从首次离开算已过 25s(未满30s)，但从二次离开点算 0s -> 不应回收
	rep, err := c.Reclaim("ij-5")
	if err != nil || len(rep.Reclaimed) != 0 {
		t.Fatalf("重新占用后空闲应从二次离开点起算: %+v err=%v", rep, err)
	}
	now = secondIdleAt.Add(30 * time.Second)
	rep, err = c.Reclaim("ij-6")
	if err != nil || len(rep.Reclaimed) != 1 {
		t.Fatalf("二次空闲满阈值后应回收: %+v err=%v", rep, err)
	}
	t.Logf("判定依据: idleAt %s -> 重新占用清零 -> %s，二次起算", firstIdleAt.Format(time.RFC3339), secondIdleAt.Format(time.RFC3339))
}

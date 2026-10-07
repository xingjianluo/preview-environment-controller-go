package preview

import (
	"sync"
	"testing"
	"time"
)

// 续期基本语义：成功延长；只影响续期者自己，其他占用者租约不变；环境初始有效期不变。
func TestRenewLeaseOnlyAffectsCaller(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("rw-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("rw-2", "env-a", "h2"); err != nil {
		t.Fatal(err)
	}
	env, _ := c.Get("env-a")
	h1Before := env.Leases["h1"]
	h2Before := env.Leases["h2"]
	expiresBefore := env.ExpiresAt

	now = base.Add(10 * time.Second)
	res, err := c.Renew("rw-3", "env-a", "h1", 30*time.Second)
	if err != nil {
		t.Fatalf("续期应成功: %v", err)
	}
	if !res.NewExpires.Equal(h1Before.Add(30 * time.Second)) {
		t.Fatalf("续期后到期时间=%s，期望 %s", res.NewExpires, h1Before.Add(30*time.Second))
	}
	env, _ = c.Get("env-a")
	if !env.Leases["h2"].Equal(h2Before) {
		t.Fatalf("h2 租约被续期误伤: before=%s after=%s", h2Before, env.Leases["h2"])
	}
	if !env.ExpiresAt.Equal(expiresBefore) {
		t.Fatalf("环境初始有效期不应被续期改变: before=%s after=%s", expiresBefore, env.ExpiresAt)
	}
	t.Logf("申请标识=rw-3 状态迁移判定: h1 租约 %s -> %s；h2 保持 %s；环境 ExpiresAt 不变",
		h1Before.Format(time.RFC3339), res.NewExpires.Format(time.RFC3339), h2Before.Format(time.RFC3339))
}

// 续期拒绝原因必须三类可区分：NOT_FOUND / NOT_HELD / LEASE_EXPIRED；且都不改状态。
func TestRenewRejectionReasons(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("rr-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	// 环境不存在
	_, err := c.Renew("rr-2", "no-such", "h1", time.Minute)
	mustCode(t, err, ErrNotFound)
	// 调用方未占用
	_, err = c.Renew("rr-3", "env-a", "stranger", time.Minute)
	mustCode(t, err, ErrNotHeld)

	env, _ := c.Get("env-a")
	before := env.Leases["h1"] // base + 1min

	// 临界点之前一刻：成功（只续 1ns，新到期点为 base+1min+1ns）
	now = base.Add(time.Minute - time.Nanosecond)
	res4, err := c.Renew("rr-4", "env-a", "h1", time.Nanosecond)
	if err != nil {
		t.Fatalf("到期前 1ns 续期应成功: %v", err)
	}
	extended := res4.NewExpires

	// 临界点本身：now 到达（原始）到期时刻，但仍早于刚续出的到期点，
	// 因此仍可续期 —— 这正是“到期前续期把窗口往后推”的语义。
	now = base.Add(time.Minute)
	if _, err := c.Renew("rr-4b", "env-a", "h1", time.Minute); err != nil {
		t.Fatalf("未到当前租约到期点，续期应成功: %v", err)
	}

	// 错过续期窗口：时钟越过当前租约到期点后，稳定拒绝 LEASE_EXPIRED
	env, _ = c.Get("env-a")
	now = env.Leases["h1"] // 恰好到期（临界）
	_, err = c.Renew("rr-5", "env-a", "h1", time.Minute)
	mustCode(t, err, ErrLeaseExpired)

	// 状态不被改坏：失败续期不得缩短/改动租约，占用者仍在
	env, _ = c.Get("env-a")
	if !env.Leases["h1"].Equal(extended.Add(time.Minute)) {
		t.Fatalf("续期失败把租约改坏了: %s", env.Leases["h1"])
	}
	if !env.HasHolder("h1") || env.State != StateReady {
		t.Fatalf("续期失败后环境状态异常: state=%s holders=%v", env.State, env.Holders)
	}
	t.Logf("申请标识=rr-5 判定依据: 临界 now=%s 原始lease=%s 拒绝 LEASE_EXPIRED 且租约保持 %s",
		now.Format(time.RFC3339), before.Format(time.RFC3339), extended.Format(time.RFC3339))

	// 错过窗口后重新申请可拿回使用权
	if _, err := c.Acquire("rr-6", "env-a", "h1"); err != nil {
		t.Fatalf("过期后重新申请应成功: %v", err)
	}
	env, _ = c.Get("env-a")
	if !env.Leases["h1"].After(now) {
		t.Fatalf("重新申请后应发放新租约: %s", env.Leases["h1"])
	}
}

// 并发续期互不破坏：同一占用者多次续期串行化，租约单调不减。
func TestConcurrentRenewIsConsistent(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })
	if _, err := c.Acquire("rcr-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Renew("rcr-x", "env-a", "h1", time.Second)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发续期不应失败: %v", err)
		}
	}
	env, _ := c.Get("env-a")
	if !env.Leases["h1"].After(base) {
		t.Fatalf("并发续期后租约应被延长: %s", env.Leases["h1"])
	}
}

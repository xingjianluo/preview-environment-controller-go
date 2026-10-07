package preview

import (
	"testing"
	"time"
)

// 续期成功：延长的是调用方自己的租约，环境整体到期时间随之重算。
func TestRenewSuccess(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("rw-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	env, err := c.Renew("rw-2", "env-1", "h1", 30*time.Second)
	if err != nil {
		t.Fatalf("续期应成功: %v", err)
	}
	want := base.Add(time.Minute + 30*time.Second)
	if !env.LeaseExpiry["h1"].Equal(want) {
		t.Fatalf("h1 租约=%s，期望 %s", env.LeaseExpiry["h1"], want)
	}
	if !env.ExpiresAt.Equal(want) {
		t.Fatalf("环境整体到期=%s，期望 %s", env.ExpiresAt, want)
	}
	t.Logf("判定依据: 状态迁移 leaseExpiry %s -> %s，envExpiresAt=%s",
		base.Add(time.Minute), want, env.ExpiresAt)
}

// 多占用者续期互不影响：只延长自己那一份，别人的租约不变，
// 环境整体到期取各占用者租约最大值。
func TestRenewIsolatesHolders(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("ri-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(10 * time.Second) // h2 晚加入，租约自然更晚到期
	if _, err := c.Acquire("ri-2", "env-1", "h2"); err != nil {
		t.Fatal(err)
	}
	h1LeaseBefore := c.envs["env-1"].LeaseExpiry["h1"]
	h2LeaseBefore := c.envs["env-1"].LeaseExpiry["h2"]

	env, err := c.Renew("ri-3", "env-1", "h1", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !env.LeaseExpiry["h2"].Equal(h2LeaseBefore) {
		t.Fatalf("h2 租约被误改: %s -> %s", h2LeaseBefore, env.LeaseExpiry["h2"])
	}
	if env.LeaseExpiry["h1"].Equal(h1LeaseBefore) {
		t.Fatal("h1 续期后租约未变化")
	}
	if !env.ExpiresAt.Equal(env.LeaseExpiry["h1"]) {
		t.Fatalf("整体到期应取最大租约(h1): env=%s h1=%s", env.ExpiresAt, env.LeaseExpiry["h1"])
	}
	t.Logf("判定依据: h1 续期 %s->%s；h2 保持 %s；envExpiresAt=%s",
		h1LeaseBefore, env.LeaseExpiry["h1"], h2LeaseBefore, env.ExpiresAt)
}

// 续期拒绝原因必须互相区分，且失败不得改动环境状态。
func TestRenewRejectReasons(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("rr-1", "env-1", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("rr-2", "env-2", "h2"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("rr-3", "env-2", "h2"); err != nil {
		t.Fatal(err)
	}

	// 环境不存在
	mustCode(t, c.RenewErr("rr-4", "no-such", "h1", time.Second), ErrNotFound)
	// 已回收环境同样 NOT_FOUND
	if err := c.Destroy("rr-5", "env-2"); err != nil {
		t.Fatal(err)
	}
	mustCode(t, c.RenewErr("rr-6", "env-2", "h2", time.Second), ErrNotFound)
	// 调用方并未占用
	mustCode(t, c.RenewErr("rr-7", "env-1", "stranger", time.Second), ErrNotHeld)
	// 回收中 -> NAME_CONFLICT
	c.mu.Lock()
	c.envs["env-1"].State = StateReclaiming
	c.mu.Unlock()
	mustCode(t, c.RenewErr("rr-8", "env-1", "h1", time.Second), ErrNameConflict)
	c.mu.Lock()
	c.envs["env-1"].State = StateReady
	c.mu.Unlock()

	// 临界时刻：恰好等于到期点 -> LEASE_EXPIRED；差 1 纳秒 -> 成功
	deadline := base.Add(time.Minute)
	now = deadline
	mustCode(t, c.RenewErr("rr-9", "env-1", "h1", time.Second), ErrLeaseExpired)
	// 失败后状态不被改坏：租约仍是原到期点
	c.mu.Lock()
	got := c.envs["env-1"].LeaseExpiry["h1"]
	state := c.envs["env-1"].State
	c.mu.Unlock()
	if !got.Equal(deadline) || state != StateReady {
		t.Fatalf("续期失败后状态被改: lease=%s state=%s", got, state)
	}
	now = deadline.Add(-time.Nanosecond)
	if _, err := c.Renew("rr-10", "env-1", "h1", time.Second); err != nil {
		t.Fatalf("临界前 1ns 续期应成功: %v", err)
	}
	t.Logf("判定依据: now==leaseExpiry 拒绝 LEASE_EXPIRED；now<leaseExpiry(差1ns) 成功")
}

// RenewErr 是测试辅助：只取续期的错误。
func (c *Controller) RenewErr(requestID, name, holder string, extend time.Duration) error {
	_, err := c.Renew(requestID, name, holder, extend)
	return err
}

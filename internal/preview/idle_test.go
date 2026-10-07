package preview

import (
	"errors"
	"testing"
	"time"
)

func idleTestCfg() Config {
	return Config{MaxEnvs: 8, PortBase: 18000, PortEnd: 18010, TTL: time.Minute, IdleTTL: 30 * time.Second}
}

// 空闲与在用混在一起：只有“最后一个占用者离开后空闲超过阈值”的环境被回收；
// 仍在使用的环境即使早过初始有效期也跳过，且跳过原因可见。
func TestIdleReclaimMixed(t *testing.T) {
	c, _ := newTestController(t, idleTestCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	// env-idle：占用者在 base+5s 离开
	if _, err := c.Acquire("ix-1", "env-idle", "h1"); err != nil {
		t.Fatal(err)
	}
	// env-busy：一直被 h2 占用
	if _, err := c.Acquire("ix-2", "env-busy", "h2"); err != nil {
		t.Fatal(err)
	}
	// env-fresh：占用者在 base+40s 才离开（空闲不足阈值）
	if _, err := c.Acquire("ix-3", "env-fresh", "h3"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(5 * time.Second)
	if err := c.Release("ix-4", "env-idle", "h1"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(100 * time.Second) // env-fresh 在 base+100s 才空闲
	if err := c.Release("ix-5", "env-fresh", "h3"); err != nil {
		t.Fatal(err)
	}

	// 时钟推进到 base+2min：env-busy 的初始有效期(1min)早过，但仍被占用
	now = base.Add(2 * time.Minute)
	report := c.ReclaimIdle("ix-6")

	if len(report.Reclaimed) != 1 || report.Reclaimed[0] != "env-idle" {
		t.Fatalf("回收列表=%v，期望仅 [env-idle]", report.Reclaimed)
	}
	if !report.Succeeded() {
		t.Fatalf("本轮不应有失败: %+v", report.Failures)
	}
	skipNames := map[string]string{}
	for _, sk := range report.Skipped {
		skipNames[sk.Name] = sk.Reason
	}
	if r, ok := skipNames["env-busy"]; !ok {
		t.Fatalf("env-busy 应被跳过并给出依据，实际 skip=%+v", report.Skipped)
	} else {
		t.Logf("申请标识=ix-6 判定依据: env-busy 跳过原因=%q", r)
	}
	if _, ok := skipNames["env-fresh"]; !ok {
		t.Fatalf("env-fresh 空闲不足阈值应被跳过: %+v", report.Skipped)
	}
	idle, _ := c.Get("env-idle")
	busy, _ := c.Get("env-busy")
	fresh, _ := c.Get("env-fresh")
	if idle.State != StateReclaimed {
		t.Fatalf("env-idle 状态=%s，期望 Reclaimed", idle.State)
	}
	if busy.State != StateReady || !busy.InUse() {
		t.Fatalf("env-busy 必须保持可用: state=%s holders=%v", busy.State, busy.Holders)
	}
	if fresh.State != StateReady {
		t.Fatalf("env-fresh 空闲不足，应保持 Ready: %s", fresh.State)
	}

	// env-fresh 再等过阈值后可回收（离开于 100s，需到 130s 后）
	now = base.Add(131 * time.Second)
	report2 := c.ReclaimIdle("ix-7")
	if len(report2.Reclaimed) != 1 || report2.Reclaimed[0] != "env-fresh" {
		t.Fatalf("env-fresh 空闲满阈值后应被回收: %v", report2.Reclaimed)
	}
	t.Log("申请标识=ix-7 状态迁移: env-fresh Ready->Reclaiming->Reclaimed")
}

// 回收失败必须从入口可见且可安全重试：报告 Failures 非空、状态回滚、对外不报成功。
func TestIdleReclaimFailureVisibleAndRetryable(t *testing.T) {
	c, fb := newTestController(t, idleTestCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("if-1", "env-x", "h1"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(time.Second)
	if err := c.Release("if-2", "env-x", "h1"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(time.Minute) // 空闲超过 30s

	fb.setStopErr(errors.New("模拟后端故障"))
	report := c.ReclaimIdle("if-3")
	if report.Succeeded() || len(report.Failures) != 1 {
		t.Fatalf("回收失败必须体现在 Failures: %+v", report)
	}
	if report.Failures[0].Name != "env-x" {
		t.Fatalf("失败环境名不对: %+v", report.Failures)
	}
	if len(report.Reclaimed) != 0 {
		t.Fatalf("失败不能计入成功回收: %v", report.Reclaimed)
	}
	env, _ := c.Get("env-x")
	if env.State != StateReady || env.ReclaimErr == "" {
		t.Fatalf("失败后应回滚 Ready 并记录原因: state=%s err=%q", env.State, env.ReclaimErr)
	}
	t.Logf("申请标识=if-3 判定依据: 回收失败可见 failure=%q，状态回滚 Ready（不报成功）", report.Failures[0].Err)

	// 旧入口 Reclaim 同样不会把失败伪装成成功：返回列表为空、整体 err 为 nil（保持兼容）
	reclaimed, err := c.Reclaim("if-3b")
	if err != nil || len(reclaimed) != 0 {
		t.Fatalf("旧入口语义: reclaimed=%v err=%v", reclaimed, err)
	}

	// 故障恢复后重试成功
	fb.setStopErr(nil)
	report2 := c.ReclaimIdle("if-4")
	if !report2.Succeeded() || len(report2.Reclaimed) != 1 || report2.Reclaimed[0] != "env-x" {
		t.Fatalf("重试应成功: %+v", report2)
	}
	env, _ = c.Get("env-x")
	if env.State != StateReclaimed || env.ReclaimErr != "" {
		t.Fatalf("重试后状态=%s ReclaimErr=%q", env.State, env.ReclaimErr)
	}
}

// 空闲从“最后一个占用者”离开起算：先离开的人不启动空闲计时。
func TestIdleStartsAfterLastHolder(t *testing.T) {
	c, _ := newTestController(t, idleTestCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("il-1", "env", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("il-2", "env", "h2"); err != nil {
		t.Fatal(err)
	}
	now = base.Add(10 * time.Second)
	if err := c.Release("il-3", "env", "h1"); err != nil {
		t.Fatal(err)
	}
	// 此时 h2 仍占用，空闲计时未开始；推进很久也不能回收
	now = base.Add(10 * time.Minute)
	env, _ := c.Get("env")
	if env.ReleasedAt != (time.Time{}) {
		t.Fatalf("仍有占用者时 ReleasedAt 应为零值，实际 %s", env.ReleasedAt)
	}
	report := c.ReclaimIdle("il-4")
	for _, sk := range report.Skipped {
		if sk.Name == "env" {
			t.Logf("申请标识=il-4 判定依据: %s", sk.Reason)
		}
	}
	if len(report.Reclaimed) != 0 {
		t.Fatalf("仍有占用者，不应回收: %v", report.Reclaimed)
	}
	now = base.Add(10*time.Minute + 1*time.Second)
	if err := c.Release("il-5", "env", "h2"); err != nil {
		t.Fatal(err)
	}
	env, _ = c.Get("env")
	if !env.ReleasedAt.Equal(now) {
		t.Fatalf("最后一个占用者离开时间应=%s，实际 %s", now, env.ReleasedAt)
	}
	now = now.Add(31 * time.Second)
	report2 := c.ReclaimIdle("il-6")
	if len(report2.Reclaimed) != 1 {
		t.Fatalf("从最后一个占用者离开起算空闲，满阈值后应回收: %+v", report2)
	}
}

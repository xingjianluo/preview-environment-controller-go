package preview

import (
	"context"
	"testing"
	"time"
)

// 优雅停机：先停止接受新申请；占用者释放后自动回收其拉起过的实例，无残留。
func TestDrainGraceful(t *testing.T) {
	c, fb := newTestController(t, idleTestCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	if _, err := c.Acquire("dn-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("dn-2", "env-b", "h2"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("dn-3", "env-a", "h1"); err != nil { // env-a 空闲
		t.Fatal(err)
	}

	c.BeginDrain("dn-trigger")

	// 停机期间新申请被稳定拒绝
	_, err := c.Acquire("dn-4", "env-c", "h9")
	mustCode(t, err, ErrShuttingDown)
	// 进行中语义的其他操作仍有明确响应
	if err := c.Release("dn-5", "no-such", "h1"); err == nil {
		t.Fatal("停机期间对不存在环境释放仍应返回明确错误，而不是静默")
	}

	// h2 在 50ms 后离开
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = c.Release("dn-6", "env-b", "h2")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	report, err := c.Drain(ctx, "dn-drain")
	if err != nil {
		t.Fatalf("优雅停机不应失败: %v", err)
	}
	if got := len(report.Reclaimed); got != 2 {
		t.Fatalf("应收尾回收 2 个实例，实际 %d (%v)", got, report.Reclaimed)
	}
	if c.activeHandleCountLocked() != 0 {
		t.Fatalf("停机后仍有后台实例句柄残留: %d", c.activeHandleCountLocked())
	}
	if fb.runningCount() != 0 {
		t.Fatalf("后端仍有 %d 个运行中实例（进程泄漏）", fb.runningCount())
	}
	t.Logf("申请标识=dn-drain 状态迁移: 两个实例均 Ready->Reclaimed，reclaimed=%v", report.Reclaimed)

	// 停机后依旧拒绝新申请，结论稳定
	_, err = c.Acquire("dn-7", "env-a", "h1")
	mustCode(t, err, ErrShuttingDown)
}

// 超时强制停止：占用者不释放时，ctx 到期后强制停止残留实例，不留后台进程。
func TestDrainForceAfterTimeout(t *testing.T) {
	c, fb := newTestController(t, idleTestCfg())
	if _, err := c.Acquire("df-1", "env-a", "sticky"); err != nil {
		t.Fatal(err)
	}
	c.BeginDrain("df-trigger")

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	report, err := c.Drain(ctx, "df-drain")
	if err != nil {
		t.Fatalf("强制停止成功时不应返回错误: %v", err)
	}
	if c.activeHandleCountLocked() != 0 || fb.runningCount() != 0 {
		t.Fatalf("强制停止后仍有残留实例: handles=%d running=%d", c.activeHandleCountLocked(), fb.runningCount())
	}
	env, _ := c.Get("env-a")
	if env.State != StateReclaimed {
		t.Fatalf("强制停止后环境应为 Reclaimed，实际 %s", env.State)
	}
	t.Logf("申请标识=df-drain 判定依据: 占用者未释放，超时后强制回收 reclaimed=%v", report.Reclaimed)
}

// 停机可重复触发、重复收尾不出错。
func TestDrainIdempotent(t *testing.T) {
	c, fb := newTestController(t, idleTestCfg())
	if _, err := c.Acquire("di-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Release("di-2", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	c.BeginDrain("di-1")
	c.BeginDrain("di-2") // 重复触发幂等

	ctx := context.Background()
	r1, err := c.Drain(ctx, "di-drain-1")
	if err != nil || len(r1.Reclaimed) != 1 {
		t.Fatalf("首次收尾: r=%v err=%v", r1.Reclaimed, err)
	}
	// 再次收尾：无活跃实例，立即空返回
	r2, err := c.Drain(ctx, "di-drain-2")
	if err != nil || len(r2.Reclaimed) != 0 {
		t.Fatalf("重复收尾应为空操作: r=%v err=%v", r2.Reclaimed, err)
	}
	if fb.runningCount() != 0 {
		t.Fatal("重复收尾后仍有运行实例")
	}
}

// 强制阶段后端仍失败：必须对外报错，不能谎报成功。
func TestDrainForceFailureReported(t *testing.T) {
	c, fb := newTestController(t, idleTestCfg())
	if _, err := c.Acquire("dx-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	c.BeginDrain("dx-trigger")
	fb.setStopErr(errFoo)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.Drain(ctx, "dx-drain")
	if err == nil {
		t.Fatal("强制停止失败必须返回错误，不能谎报停机成功")
	}
	fb.setStopErr(nil)
	t.Logf("申请标识=dx-drain 判定依据: 强制停止失败已对外暴露 err=%v", err)
}

package preview

import (
	"errors"
	"testing"
)

// 停机收尾：即使环境仍被占用，也要停止全部自拉起实例，且不残留句柄。
func TestStopAllStopsBusyInstances(t *testing.T) {
	c, fb := newTestController(t, testCfg())
	if _, err := c.Acquire("sa-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("sa-2", "env-b", "h2"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("sa-3", "env-c", "h3"); err != nil {
		t.Fatal(err)
	}
	// env-b 仍有占用者，env-a 已空闲，env-c 不释放：都必须被停止
	if err := c.Release("sa-4", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}

	rep := c.StopAll("sa-5")
	if rep.HasFailures() {
		t.Fatalf("StopAll 不应有失败: %+v", rep.Failures)
	}
	if len(rep.Stopped) != 3 {
		t.Fatalf("应停止 3 个实例，实际 %v", rep.Stopped)
	}
	if fb.stopCalls != 3 {
		t.Fatalf("后端 Stop 调用次数=%d，期望 3", fb.stopCalls)
	}
	c.mu.Lock()
	leaked := len(c.handles)
	c.mu.Unlock()
	if leaked != 0 {
		t.Fatalf("停机后残留 %d 个后端句柄", leaked)
	}
	for _, name := range []string{"env-a", "env-b", "env-c"} {
		env, _ := c.Get(name)
		if env.State != StateReclaimed {
			t.Fatalf("%s 状态=%s，期望 Reclaimed", name, env.State)
		}
	}
	t.Logf("判定依据: stopped=%v，含在用环境，handles 全部清理", rep.Stopped)
}

// StopAll 失败可安全重试：失败实例回滚、记录原因；重试成功；重复调用幂等。
func TestStopAllFailureRetryIdempotent(t *testing.T) {
	c, fb := newTestController(t, testCfg())
	if _, err := c.Acquire("sr-1", "env-a", "h1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Acquire("sr-2", "env-b", "h2"); err != nil {
		t.Fatal(err)
	}

	fb.setStopErr(errors.New("模拟 SIGTERM 失败"))
	rep := c.StopAll("sr-3")
	if !rep.HasFailures() || len(rep.Failures) != 2 {
		t.Fatalf("两个实例都应失败: %+v", rep)
	}
	c.mu.Lock()
	leaked := len(c.handles)
	c.mu.Unlock()
	if leaked != 2 {
		t.Fatalf("失败后句柄应保留以便重试，残留=%d", leaked)
	}
	for _, name := range []string{"env-a", "env-b"} {
		env, _ := c.Get(name)
		if env.State != StateReady || env.ReclaimErr == "" {
			t.Fatalf("%s 应回滚 Ready 并记录原因: state=%s err=%q", name, env.State, env.ReclaimErr)
		}
	}
	t.Logf("判定依据: 失败回滚 state=Ready 并保留句柄，Failures 对外可见")

	fb.setStopErr(nil)
	rep = c.StopAll("sr-4")
	if rep.HasFailures() || len(rep.Stopped) != 2 {
		t.Fatalf("重试应收尾全部实例: %+v", rep)
	}

	// 幂等：没有活动实例时重复 StopAll 是空操作。
	rep = c.StopAll("sr-5")
	if rep.HasFailures() || len(rep.Stopped) != 0 {
		t.Fatalf("重复 StopAll 应为空操作: %+v", rep)
	}
}

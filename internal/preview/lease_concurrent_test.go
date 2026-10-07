package preview

import (
	"sync"
	"testing"
	"time"
)

// 多个占用者并发对同一环境各自续期：全部成功，各自只延长自己的租约，
// 环境整体到期时间始终是所有租约的最大值（race 检测下也成立）。
func TestConcurrentRenewIndependentHolders(t *testing.T) {
	c, _ := newTestController(t, testCfg())
	base := time.Now()
	now := base
	c.SetClock(func() time.Time { return now })

	const n = 12
	for i := 0; i < n; i++ {
		h := "h" + string(rune('a'+i))
		if _, err := c.Acquire("cr-"+h, "env-1", h); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			h := "h" + string(rune('a'+i))
			_, errs[i] = c.Renew("cr-renew-"+h, "env-1", h, time.Duration(i+1)*time.Second)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("holder %d 并发续期失败: %v", i, err)
		}
	}

	c.mu.Lock()
	env := c.envs["env-1"]
	var max time.Time
	for i := 0; i < n; i++ {
		h := "h" + string(rune('a'+i))
		want := base.Add(time.Minute + time.Duration(i+1)*time.Second)
		if got := env.LeaseExpiry[h]; !got.Equal(want) {
			c.mu.Unlock()
			t.Fatalf("%s 租约=%s，期望 %s（续期串改了别人）", h, got, want)
		}
		if want.After(max) {
			max = want
		}
	}
	exp := env.ExpiresAt
	c.mu.Unlock()
	if !exp.Equal(max) {
		t.Fatalf("环境整体到期=%s，期望各租约最大值 %s", exp, max)
	}
	t.Logf("判定依据: %d 个占用者并发续期全部成功，个人租约互不串改，envExpiresAt=max=%s", n, max)
}

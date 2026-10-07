package service

import (
	"context"
	"testing"
	"time"
)

// 停机等待期内占用者仍可释放：最后一个人释放后立即干净收尾，无需等满宽限期。
func TestReleaseDuringDrainLeadsToCleanStop(t *testing.T) {
	srv, _, fb := newTestServer(t, 30*time.Second) // 宽限很长，但应提前结束
	base := "http://" + srv.Addr()
	if st, b := postJSON(t, base+"/v1/envs", map[string]string{"name": "e1", "holder": "u1"}, "dd-1"); st != 200 {
		t.Fatalf("申请失败: %d %v", st, b)
	}

	done := make(chan struct{})
	var rep struct {
		Stopped []string
		Err     error
	}
	go func() {
		r, err := srv.Shutdown(context.Background())
		rep.Stopped, rep.Err = r.Stopped, err
		close(done)
	}()

	// 等 draining 生效
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st, _ := postJSON(t, base+"/v1/envs", map[string]string{"name": "x", "holder": "y"}, "dd-probe")
		if st == 503 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 释放仍被允许（不是 503）；释放后 Shutdown 应很快完成
	st, b := doReq(t, "DELETE", base+"/v1/envs/e1?holder=u1", "dd-2")
	if st != 200 {
		t.Fatalf("停机期间释放应被允许: %d %v", st, b)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("占用者释放后停机收尾应提前完成，而不是等满宽限期")
	}
	if rep.Err != nil || len(rep.Stopped) != 1 || rep.Stopped[0] != "e1" {
		t.Fatalf("收尾结果异常: stopped=%v err=%v", rep.Stopped, rep.Err)
	}
	if fb.stopCount() != 1 {
		t.Fatalf("应停止恰好 1 个实例，实际 %d", fb.stopCount())
	}
	t.Logf("判定依据: draining 中释放=200，最后占用者离开后立即收尾 stopped=%v", rep.Stopped)
}

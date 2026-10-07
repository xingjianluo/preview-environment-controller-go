package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

// fakeBackend 与 preview 包测试用的桩一致：可注入停止失败。
type fakeBackend struct {
	mu      sync.Mutex
	starts  int
	stops   int
	stopErr error
}

func (f *fakeBackend) Start(env *preview.Environment) (*preview.Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	return &preview.Handle{PID: 2000 + f.starts}, nil
}

func (f *fakeBackend) fakeStop(err error) { f.mu.Lock(); f.stopErr = err; f.mu.Unlock() }

func (f *fakeBackend) Stop(h *preview.Handle) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopErr != nil {
		return f.stopErr
	}
	f.stops++
	return nil
}

func (f *fakeBackend) startCount() int { f.mu.Lock(); defer f.mu.Unlock(); return f.starts }
func (f *fakeBackend) stopCount() int  { f.mu.Lock(); defer f.mu.Unlock(); return f.stops }

func newTestServer(t *testing.T, grace time.Duration) (*Server, *preview.Controller, *fakeBackend) {
	t.Helper()
	logger := log.New(os.Stderr, "[service-test] ", log.Lmicroseconds)
	fb := &fakeBackend{}
	ctrl := preview.New(preview.Config{
		MaxEnvs: 8, PortBase: 28000, PortEnd: 28010,
		TTL: time.Minute, IdleTTL: 100 * time.Millisecond,
	}, fb, logger)
	srv := NewServer(ctrl, "127.0.0.1:0", grace, logger)
	if err := srv.Start(); err != nil {
		t.Fatalf("启动服务失败: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = srv.Shutdown(ctx)
	})
	return srv, ctrl, fb
}

func postJSON(t *testing.T, url string, body any, reqID string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if reqID != "" {
		req.Header.Set("X-Request-Id", reqID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func doReq(t *testing.T, method, url string, reqID string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reqID != "" {
		req.Header.Set("X-Request-Id", reqID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// 并发从 HTTP 入口申请同一个环境：只创建一个实例，全部得到同一端口与结论。
func TestHTTPConcurrentAcquireConverges(t *testing.T) {
	srv, _, fb := newTestServer(t, time.Second)
	url := "http://" + srv.Addr() + "/v1/envs"
	const n = 20
	var wg sync.WaitGroup
	statuses := make([]int, n)
	ports := make([]float64, n)
	errs := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status, body := postJSON(t, url,
				map[string]string{"name": "env-a", "holder": "h" + string(rune('a'+i))},
				"http-conc-"+string(rune('a'+i)))
			statuses[i] = status
			if status == 200 {
				ports[i] = body["env"].(map[string]any)["port"].(float64)
			} else {
				errs[i] = body["error"].(string)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if statuses[i] != 200 {
			t.Fatalf("第 %d 个申请 status=%d err=%s", i, statuses[i], errs[i])
		}
		if ports[i] != 28000 {
			t.Fatalf("第 %d 个申请端口=%v，期望全部收敛到 28000", i, ports[i])
		}
	}
	if got := fb.startCount(); got != 1 {
		t.Fatalf("并发申请创建实例数=%d，期望 1", got)
	}
	t.Logf("判定依据: %d 个 HTTP 并发申请收敛到同一实例 port=28000，后端启动 1 次", n)
}

// 全流程经 HTTP：申请 -> 查询 -> 续期(各种拒绝) -> 释放 -> 空闲回收。
func TestHTTPRenewReclaimFlow(t *testing.T) {
	srv, _, _ := newTestServer(t, time.Second)
	base := "http://" + srv.Addr()

	status, body := postJSON(t, base+"/v1/envs", map[string]string{"name": "e1", "holder": "alice"}, "flow-1")
	if status != 200 || body["created"] != true {
		t.Fatalf("首次申请: status=%d body=%v", status, body)
	}
	// 复用
	status, body = postJSON(t, base+"/v1/envs", map[string]string{"name": "e1", "holder": "bob"}, "flow-2")
	if status != 200 || body["created"] != false {
		t.Fatalf("二次申请应复用: status=%d", status)
	}

	// 查询
	status, body = doReq(t, http.MethodGet, base+"/v1/envs/e1", "flow-3")
	if status != 200 {
		t.Fatalf("查询失败: %d %v", status, body)
	}

	// 续期：未占用者 -> 403 NOT_HELD
	status, body = postJSON(t, base+"/v1/envs/e1/renew",
		map[string]any{"holder": "carol", "extend": "10s"}, "flow-4")
	if status != 403 || body["code"] != string(preview.ErrNotHeld) {
		t.Fatalf("未占用续期应 403 NOT_HELD: %d %v", status, body)
	}
	// 续期不存在环境 -> 404 NOT_FOUND
	status, body = postJSON(t, base+"/v1/envs/nope/renew",
		map[string]any{"holder": "alice", "extend": "10s"}, "flow-5")
	if status != 404 || body["code"] != string(preview.ErrNotFound) {
		t.Fatalf("不存在环境续期应 404: %d %v", status, body)
	}
	// 参数非法 -> 400 BAD_REQUEST
	status, body = postJSON(t, base+"/v1/envs/e1/renew",
		map[string]any{"holder": "alice", "extend": "0s"}, "flow-6")
	if status != 400 || body["code"] != string(preview.ErrBadRequest) {
		t.Fatalf("非法续期应 400: %d %v", status, body)
	}
	// 正常续期
	status, body = postJSON(t, base+"/v1/envs/e1/renew",
		map[string]any{"holder": "alice", "extend": "30s"}, "flow-7")
	if status != 200 {
		t.Fatalf("正常续期应 200: %d %v", status, body)
	}
	t.Logf("判定依据: 续期拒绝原因码可区分 NOT_HELD/NOT_FOUND/BAD_REQUEST，正常续期 200")

	// 释放所有占用者，然后空闲回收（IdleTTL=100ms）
	for _, h := range []string{"alice", "bob"} {
		if st, b := doReq(t, http.MethodDelete, base+"/v1/envs/e1?holder="+h, "flow-8"); st != 200 {
			t.Fatalf("释放 %s 失败: %d %v", h, st, b)
		}
	}
	// 仍在用的环境不会被回收：再造一个不释放的
	if st, b := postJSON(t, base+"/v1/envs", map[string]string{"name": "e2", "holder": "dan"}, "flow-9"); st != 200 {
		t.Fatalf("申请 e2 失败: %d %v", st, b)
	}
	time.Sleep(150 * time.Millisecond)
	status, body = postJSON(t, base+"/v1/reclaim", map[string]string{}, "flow-10")
	if status != 200 {
		t.Fatalf("回收应 200: %d %v", status, body)
	}
	reclaimed := body["reclaimed"].([]any)
	skipped := body["skipped"].([]any)
	if len(reclaimed) != 1 || reclaimed[0] != "e1" {
		t.Fatalf("应仅回收 e1: %v", reclaimed)
	}
	if len(skipped) != 1 || skipped[0].(map[string]any)["name"] != "e2" ||
		skipped[0].(map[string]any)["why"] != preview.WhyBusy {
		t.Fatalf("e2 应按 busy 跳过: %v", skipped)
	}
	t.Logf("判定依据: 空闲 e1 被回收；在用 e2 以 why=busy 跳过并照常可用")
}

// 停机期间：新申请/续期得到稳定 503 SERVICE_DRAINING，而不是静默断开；
// 收尾后不留任何后台实例；重复停机返回同一结论且不出错。
func TestHTTPDrainingRejectsAndStopsInstances(t *testing.T) {
	srv, _, fb := newTestServer(t, 1500*time.Millisecond)
	base := "http://" + srv.Addr()
	if st, b := postJSON(t, base+"/v1/envs", map[string]string{"name": "busy-1", "holder": "u1"}, "dr-1"); st != 200 {
		t.Fatalf("申请 busy-1 失败: %d %v", st, b)
	}
	if st, b := postJSON(t, base+"/v1/envs", map[string]string{"name": "idle-1", "holder": "u2"}, "dr-2"); st != 200 {
		t.Fatalf("申请 idle-1 失败: %d %v", st, b)
	}
	if st, b := doReq(t, http.MethodDelete, base+"/v1/envs/idle-1?holder=u2", "dr-3"); st != 200 {
		t.Fatalf("释放 idle-1 失败: %d %v", st, b)
	}

	// busy-1 仍被 u1 占用；先在另一个 goroutine 触发停机（宽限期内等待释放），
	// 期间并发打申请，应稳定收到 503。
	var shutWG sync.WaitGroup
	shutWG.Add(1)
	var rep *preview.StopReport
	var shutErr error
	go func() {
		defer shutWG.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, shutErr = srv.Shutdown(ctx)
	}()

	// 等待 draining 生效后狂打申请与续期
	deadline := time.Now().Add(2 * time.Second)
	drainingSeen := 0
	for time.Now().Before(deadline) {
		st, b := postJSON(t, base+"/v1/envs", map[string]string{"name": "x", "holder": "y"}, "dr-req-acq")
		if st == 503 && b["code"] == string(preview.ErrServiceDraining) {
			drainingSeen++
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if drainingSeen == 0 {
		t.Fatal("停机期间申请未得到稳定的 503 SERVICE_DRAINING")
	}
	st, b := postJSON(t, base+"/v1/envs/busy-1/renew",
		map[string]any{"holder": "u1", "extend": "1s"}, "dr-req-renew")
	if st != 503 || b["code"] != string(preview.ErrServiceDraining) {
		t.Fatalf("停机期间续期应 503 SERVICE_DRAINING: %d %v", st, b)
	}
	t.Logf("判定依据: 停机期间 acquire/renew 稳定返回 503 code=SERVICE_DRAINING（连接未静默断开）")

	// 宽限期结束仍未释放 -> 强制停止在用实例
	shutWG.Wait()
	if shutErr != nil {
		t.Fatalf("停机收尾应成功: %v", shutErr)
	}
	if len(rep.Stopped) != 2 {
		t.Fatalf("应收尾 2 个实例（含在用）: %+v", rep)
	}
	if fb.stopCount() != 2 {
		t.Fatalf("后端停止次数=%d，期望 2（不留后台实例）", fb.stopCount())
	}

	// 服务已关闭：再请求得到连接错误，而不是半开服务
	if _, err := postJSONRaw(base+"/v1/envs", map[string]string{"name": "z", "holder": "q"}); err == nil {
		t.Fatal("停机后不应再能建立有效请求")
	}

	// 重复停机不出错，返回同一结论
	rep2, err2 := srv.Shutdown(context.Background())
	if err2 != nil || rep2 != rep {
		t.Fatalf("重复停机应幂等返回首次结论: rep=%p rep2=%p err=%v", rep, rep2, err2)
	}
	t.Logf("判定依据: 停机后 0 个后台实例残留，重复 Shutdown 幂等")
}

// 回收失败从入口可见（207 + failures），修复后重试成功。
func TestHTTPReclaimFailureVisibleAndRetry(t *testing.T) {
	srv, _, fb := newTestServer(t, time.Second)
	base := "http://" + srv.Addr()
	if st, b := postJSON(t, base+"/v1/envs", map[string]string{"name": "e1", "holder": "u1"}, "rf-1"); st != 200 {
		t.Fatalf("申请失败: %d %v", st, b)
	}
	if st, b := doReq(t, http.MethodDelete, base+"/v1/envs/e1?holder=u1", "rf-2"); st != 200 {
		t.Fatalf("释放失败: %d %v", st, b)
	}
	time.Sleep(150 * time.Millisecond)
	fb.fakeStop(errors.New("注入停止故障"))
	st, body := postJSON(t, base+"/v1/reclaim", map[string]string{}, "rf-3")
	if st != 207 {
		t.Fatalf("回收有失败时应返回 207，实际 %d", st)
	}
	fails := body["failures"].([]any)
	if len(fails) != 1 || fails[0].(map[string]any)["name"] != "e1" {
		t.Fatalf("failures 应包含 e1: %v", fails)
	}
	t.Logf("判定依据: 回收失败入口可见 status=207 failures=%v，未对外报成功", fails)

	fb.fakeStop(nil)
	st, body = postJSON(t, base+"/v1/reclaim", map[string]string{}, "rf-4")
	if st != 200 || len(body["reclaimed"].([]any)) != 1 {
		t.Fatalf("重试回收应 200 且成功: %d %v", st, body)
	}
	t.Log("判定依据: 后端恢复后重试回收成功")
}

// postJSONRaw 只返回请求错误（用于断言服务关闭后无法再通信）。
func postJSONRaw(url string, body any) (int, error) {
	b, _ := json.Marshal(body)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

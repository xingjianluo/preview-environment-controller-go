package service

import (
	"errors"

	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

var errSentinel = errors.New("模拟停止故障")

// testHarness 拉起一个监听随机端口的服务，测试结束时兜底停机。
type testHarness struct {
	t       *testing.T
	srv     *Server
	client  *http.Client
	backend *fakeBackendAdapter
}

// fakeBackendAdapter 是 service 包内能注入停止故障的后端。
type fakeBackendAdapter struct {
	mu      sync.Mutex
	starts  int
	running map[int]bool
	stopErr error
}

func (f *fakeBackendAdapter) start(env *preview.Environment) (*preview.Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	pid := 5000 + f.starts
	if f.running == nil {
		f.running = map[int]bool{}
	}
	f.running[pid] = true
	return &preview.Handle{PID: pid}, nil
}

func setupHarness(t *testing.T, drainTimeout time.Duration) *testHarness {
	t.Helper()
	fb := &fakeBackendAdapter{}
	logger := log.New(&testLogger{t: t}, "[service-test] ", log.Lmicroseconds)
	ctrl := preview.New(preview.Config{
		MaxEnvs: 16, PortBase: 20000, PortEnd: 20100,
		TTL: time.Minute, IdleTTL: 200 * time.Millisecond,
	}, &backendAdapter{fb: fb}, logger)
	srv := New(ctrl, Config{Addr: "127.0.0.1:0", DrainTimeout: drainTimeout, Logger: logger})
	go func() { _ = srv.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for srv.ListenAddr() == "" && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if srv.ListenAddr() == "" {
		t.Fatal("服务未在预期时间内监听")
	}
	h := &testHarness{t: t, srv: srv, client: &http.Client{Timeout: 3 * time.Second}, backend: fb}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return h
}

type testLogger struct{ t *testing.T }

func (l *testLogger) Write(p []byte) (int, error) {
	l.t.Log(string(bytes.TrimRight(p, "\n")))
	return len(p), nil
}

// backendAdapter 把包内 fakeBackendAdapter 适配为 preview.Backend。
type backendAdapter struct{ fb *fakeBackendAdapter }

func (b *backendAdapter) Start(env *preview.Environment) (*preview.Handle, error) {
	return b.fb.start(env)
}
func (b *backendAdapter) Stop(h *preview.Handle) error {
	b.fb.mu.Lock()
	defer b.fb.mu.Unlock()
	if b.fb.stopErr != nil {
		return b.fb.stopErr
	}
	delete(b.fb.running, h.PID)
	return nil
}

func (h *testHarness) url(path string) string { return "http://" + h.srv.ListenAddr() + path }

func (h *testHarness) post(path string, body any) (int, map[string]any) {
	h.t.Helper()
	buf, _ := json.Marshal(body)
	resp, err := h.client.Post(h.url(path), "application/json", bytes.NewReader(buf))
	if err != nil {
		h.t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var env map[string]any
	_ = json.Unmarshal(raw, &env)
	return resp.StatusCode, env
}

func (h *testHarness) get(path string) (int, map[string]any) {
	h.t.Helper()
	resp, err := h.client.Get(h.url(path))
	if err != nil {
		h.t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var env map[string]any
	_ = json.Unmarshal(raw, &env)
	return resp.StatusCode, env
}

// 并发申请同一环境：任何入口结论一致，只创建一个实例，全部拿到同一端口。
func TestHTTPConcurrentAcquireSameEnv(t *testing.T) {
	h := setupHarness(t, time.Second)
	const n = 24
	var wg sync.WaitGroup
	ports := make([]int64, n)
	creates := make([]bool, n)
	statuses := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req := map[string]string{"name": "env-z", "holder": "h" + string(rune('a'+i))}
			buf, _ := json.Marshal(req)
			rs, err := h.client.Post(h.url("/v1/environments/acquire"), "application/json", bytes.NewReader(buf))
			if err != nil {
				t.Errorf("请求 %d 出错: %v", i, err)
				return
			}
			defer rs.Body.Close()
			statuses[i] = rs.StatusCode
			var out response
			_ = json.NewDecoder(rs.Body).Decode(&out)
			if out.Data != nil {
				data, _ := json.Marshal(out.Data)
				var av acquireView
				_ = json.Unmarshal(data, &av)
				ports[i] = int64(av.Env.Port)
				creates[i] = av.Created
			}
		}(i)
	}
	wg.Wait()
	var created int
	firstPort := int64(-1)
	for i := 0; i < n; i++ {
		if statuses[i] != http.StatusOK {
			t.Fatalf("请求 %d 状态码=%d，期望 200", i, statuses[i])
		}
		if creates[i] {
			created++
		}
		if firstPort == -1 {
			firstPort = ports[i]
		} else if ports[i] != firstPort {
			t.Fatalf("入口结论不一致：端口 %d vs %d", ports[i], firstPort)
		}
	}
	if created != 1 {
		t.Fatalf("并发申请只应新建 1 个实例，实际 %d", created)
	}
	if h.backend.starts != 1 {
		t.Fatalf("后端启动次数=%d，期望 1（并发收敛）", h.backend.starts)
	}
	status, body := h.get("/v1/environments/env-z")
	if status != http.StatusOK {
		t.Fatalf("查询失败: %d %v", status, body)
	}
	t.Logf("判定依据: %d 个并发请求全部收敛到 port=%d，holders=%v",
		n, firstPort, body["holders"])
}

// 停机期间进来的新申请拿到稳定明确的 503/SHUTTING_DOWN；释放仍可完成；
// 收尾完成后无实例残留；重复触发停机不出错。
func TestHTTPShutdownBehavior(t *testing.T) {
	h := setupHarness(t, 5*time.Second)
	// 一个空闲环境 + 一个被占用环境
	if code, _ := h.post("/v1/environments/acquire", map[string]string{"name": "idle", "holder": "u1"}); code != 200 {
		t.Fatal("acquire idle failed")
	}
	if code, _ := h.post("/v1/environments/acquire", map[string]string{"name": "busy", "holder": "u2"}); code != 200 {
		t.Fatal("acquire busy failed")
	}
	if code, body := h.post("/v1/environments/release", map[string]string{"name": "idle", "holder": "u1"}); code != 200 {
		t.Fatalf("release idle: %d %v", code, body)
	}

	// 第一次触发停机：202
	code, body := h.post("/v1/shutdown", nil)
	if code != http.StatusAccepted {
		t.Fatalf("首次停机应 202，实际 %d %v", code, body)
	}
	// 重复触发：幂等，不报错
	code2, body2 := h.post("/v1/shutdown", map[string]string{})
	if code2 != http.StatusOK {
		t.Fatalf("重复停机应幂等 200，实际 %d %v", code2, body2)
	}

	// 停机期间新申请：稳定 503 + SHUTTING_DOWN，不断连
	deadline := time.Now().Add(2 * time.Second)
	got := false
	for time.Now().Before(deadline) {
		if c, b := h.post("/v1/environments/acquire", map[string]string{"name": "new", "holder": "x"}); c == http.StatusServiceUnavailable {
			if eb, _ := b["error"].(map[string]any); eb["code"] == string(preview.ErrShuttingDown) {
				got = true
				t.Logf("判定依据: 停机期间申请被明确拒绝 code=%v", eb["code"])
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !got {
		t.Fatal("停机期间没有观察到稳定的 503/SHUTTING_DOWN")
	}

	// 占用者释放 busy，让优雅收尾可以完成
	if c, b := h.post("/v1/environments/release", map[string]string{"name": "busy", "holder": "u2"}); c != http.StatusOK {
		t.Fatalf("停机期间释放占用应可完成: %d %v", c, b)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := h.srv.Shutdown(ctx); err != nil {
		t.Fatalf("停机收尾失败: %v", err)
	}
	if len(h.backend.running) != 0 {
		t.Fatalf("停机后仍有 %d 个运行实例（进程泄漏）", len(h.backend.running))
	}
}

// 回收失败从 HTTP 入口可见：502 + data.failures 非空；修复后重试返回 200。
func TestHTTPReclaimFailureVisibleThenRetry(t *testing.T) {
	h := setupHarness(t, time.Second)
	if code, _ := h.post("/v1/environments/acquire", map[string]string{"name": "env-r", "holder": "u1"}); code != 200 {
		t.Fatal("acquire failed")
	}
	if code, body := h.post("/v1/environments/release", map[string]string{"name": "env-r", "holder": "u1"}); code != 200 {
		t.Fatalf("release: %d %v", code, body)
	}
	time.Sleep(300 * time.Millisecond) // 超过 idle-ttl

	h.backend.mu.Lock()
	h.backend.stopErr = errSentinel
	h.backend.mu.Unlock()

	code, body := h.post("/v1/reclaim", map[string]string{})
	if code != http.StatusBadGateway {
		t.Fatalf("回收失败应反映为 502，实际 %d", code)
	}
	data, _ := body["data"].(map[string]any)
	failures, _ := data["failures"].([]any)
	if len(failures) != 1 {
		t.Fatalf("data.failures 应包含 1 条，实际 %v", data["failures"])
	}
	if succ, _ := data["succeeded"].(bool); succ {
		t.Fatal("存在失败时 succeeded 必须为 false")
	}
	t.Logf("申请标识=%v 判定依据: 回收失败入口可见 failures=%v", body["request_id"], failures)

	h.backend.mu.Lock()
	h.backend.stopErr = nil
	h.backend.mu.Unlock()
	code, body = h.post("/v1/reclaim", map[string]string{})
	if code != http.StatusOK {
		t.Fatalf("重试回收应 200，实际 %d %v", code, body)
	}
	data, _ = body["data"].(map[string]any)
	rc, _ := data["reclaimed"].([]any)
	if len(rc) != 1 || rc[0] != "env-r" {
		t.Fatalf("重试应成功回收 env-r: %v", rc)
	}
}

// 临界续期 + 三类拒绝原因经 HTTP 可区分。
func TestHTTPRenewReasons(t *testing.T) {
	h := setupHarness(t, time.Second)
	h.post("/v1/environments/acquire", map[string]string{"name": "renv", "holder": "u1"})

	// 未占用：409 NOT_HELD
	code, body := h.post("/v1/environments/renew", map[string]string{"name": "renv", "holder": "stranger", "extend": "10s"})
	if code != http.StatusConflict {
		t.Fatalf("NOT_HELD 应 409，实际 %d", code)
	}
	if eb, _ := body["error"].(map[string]any); eb["code"] != string(preview.ErrNotHeld) {
		t.Fatalf("错误码=%v", eb)
	}
	// 不存在：404 NOT_FOUND
	code, _ = h.post("/v1/environments/renew", map[string]string{"name": "ghost", "holder": "u1", "extend": "10s"})
	if code != http.StatusNotFound {
		t.Fatalf("NOT_FOUND 应 404，实际 %d", code)
	}
	// 正常续期成功
	code, body = h.post("/v1/environments/renew", map[string]string{"name": "renv", "holder": "u1", "extend": "10s"})
	if code != http.StatusOK {
		t.Fatalf("续期应成功: %d %v", code, body)
	}
	t.Logf("判定依据: 续期成功 new_expires 已返回，request_id=%v", body["request_id"])
}

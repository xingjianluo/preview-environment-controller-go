package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/controller"
	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

func newTestServer(t *testing.T, maxEnvs, portLow, portHigh int) (*Client, *controller.Controller) {
	t.Helper()
	c := controller.New(controller.Config{
		MaxEnvironments: maxEnvs,
		PortLow:         portLow,
		PortHigh:        portHigh,
		DefaultTTL:      time.Minute,
		Provisioner:     controller.NewSimProvisioner(),
	})
	mux := http.NewServeMux()
	NewHandler(c).Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return NewClient(srv.URL), c
}

func TestHTTPEntrySameSemantics(t *testing.T) {
	client, _ := newTestServer(t, 10, 23000, 23010)
	ctx := context.Background()

	e, err := client.Apply(ctx, ApplyParams{
		Target: "refs/http/1", RequestID: "http-req-1", TTL: time.Minute,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	t.Logf("apply http-req-1: name=%s status=%s claim=%d endpoint=%s",
		e.Name, e.Status, e.ClaimVersion, e.Endpoint)
	for _, ev := range e.Events {
		t.Logf("    %s -> %s reason=%s req=%s %s", ev.From, ev.To, ev.Reason, ev.RequestID, ev.Detail)
	}

	// Repeat apply over HTTP converges to same port/claim.
	e2, err := client.Apply(ctx, ApplyParams{Target: "refs/http/1", RequestID: "http-req-2"})
	if err != nil || e2.Port != e.Port || e2.ClaimVersion != e.ClaimVersion {
		t.Fatalf("HTTP repeat apply must converge: err=%v port %d vs %d claim %d vs %d",
			err, e.Port, e2.Port, e.ClaimVersion, e2.ClaimVersion)
	}

	// Occupy, then release-all must be refused with InUse — the same code the
	// library entry returns, proving cross-entry consistency.
	if _, err := client.Occupy(ctx, e.Name, "tester", "http-req-3"); err != nil {
		t.Fatal(err)
	}
	_, err = client.ReleaseAll(ctx, e.Name, "http-req-4")
	if environment.CodeOf(err) != environment.ErrCodeInUse {
		t.Fatalf("want InUse over HTTP, got %v", err)
	}
	t.Logf("release-all correctly rejected over HTTP: %v", err)

	// Release holder, release-all, reclaim: happy path through HTTP.
	if _, err := client.Release(ctx, e.Name, "tester", "http-req-5"); err != nil {
		t.Fatal(err)
	}
	if e3, err := client.ReleaseAll(ctx, e.Name, "http-req-6"); err != nil || e3.Status != environment.StatusReleased {
		t.Fatalf("release-all: err=%v status=%s", err, e3.Status)
	}
	if e4, err := client.Reclaim(ctx, e.Name, "http-req-7"); err != nil || e4.Status != environment.StatusReclaimed {
		t.Fatalf("reclaim: err=%v status=%s", err, e4.Status)
	}
	// Idempotent reclaim over HTTP.
	e5, err := client.Reclaim(ctx, e.Name, "http-req-8")
	if err != nil || e5.Status != environment.StatusReclaimed {
		t.Fatalf("idempotent reclaim: %v %s", err, e5.Status)
	}

	// 404 code and message survive the HTTP boundary as structured JSON.
	_, err = client.Get(ctx, "does-not-exist")
	if environment.CodeOf(err) != environment.ErrCodeNotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestHTTPConsistentErrorCodes(t *testing.T) {
	client, _ := newTestServer(t, 1, 24000, 24099)
	ctx := context.Background()

	if _, err := client.Apply(ctx, ApplyParams{Name: "n1", Target: "t1", RequestID: "q1"}); err != nil {
		t.Fatal(err)
	}
	// Name conflict: same name, other target.
	_, err := client.Apply(ctx, ApplyParams{Name: "n1", Target: "t2", RequestID: "q2"})
	if environment.CodeOf(err) != environment.ErrCodeNameConflict {
		t.Fatalf("want NameConflict, got %v", err)
	}
	t.Logf("HTTP name conflict code=%s msg=%v", environment.CodeOf(err), err)

	// Quota exhausted: other name, other target.
	_, err = client.Apply(ctx, ApplyParams{Name: "n2", Target: "t2", RequestID: "q3"})
	if environment.CodeOf(err) != environment.ErrCodeQuotaExhausted {
		t.Fatalf("want QuotaExhausted, got %v", err)
	}
	t.Logf("HTTP quota exhausted code=%s msg=%v", environment.CodeOf(err), err)
}

// Concurrent applies across the HTTP entry still coalesce server-side.
func TestHTTPConcurrentApply(t *testing.T) {
	client, c := newTestServer(t, 10, 25000, 25010)
	ctx := context.Background()

	const n = 16
	var wg sync.WaitGroup
	ports := make([]int, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			e, err := client.Apply(ctx, ApplyParams{
				Target:    "refs/concurrent/http",
				RequestID: "http-conc-" + string(rune('a'+i)),
			})
			if err == nil {
				ports[i] = e.Port
			}
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent http apply %d: %v", i, err)
		}
		if ports[i] != ports[0] {
			t.Fatalf("converged port mismatch at %d: %d vs %d", i, ports[i], ports[0])
		}
	}
	if len(c.Snapshot()) != 1 {
		t.Fatalf("want single instance record, got %d", len(c.Snapshot()))
	}
}

func TestHTTPReclaimExpired(t *testing.T) {
	client, _ := newTestServer(t, 10, 26000, 26010)
	ctx := context.Background()
	if _, err := client.Apply(ctx, ApplyParams{Target: "t", RequestID: "r1", Holder: "h"}); err != nil {
		t.Fatal(err)
	}
	// In-use envs are not reaped; sweep is a successful no-op.
	if err := client.ReclaimExpired(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	e, _ := client.Get(ctx, "pe-t")
	if e.Status != environment.StatusInUse {
		t.Fatalf("want InUse preserved, got %s", e.Status)
	}
}

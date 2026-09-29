package controller

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

// ---- test helpers --------------------------------------------------------

type fakeClock struct{ t time.Time }

func (f *fakeClock) Now() time.Time { return f.t }
func (f *fakeClock) Advance(d time.Duration) time.Time {
	f.t = f.t.Add(d)
	return f.t
}

// testLogger routes controller logs through the testing framework, so the
// unit-test output (-v) contains the application identifier, the state
// transition and the decision basis for every operation.
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) {
	l.t.Logf("  [controller-log] "+format, args...)
}

func newTestController(t *testing.T, cfg Config) (*Controller, *fakeClock, *SimProvisioner) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)}
	sim := NewSimProvisioner()
	cfg.Clock = clk
	cfg.Provisioner = sim
	cfg.Logger = testLogger{t: t}
	if cfg.MaxEnvironments == 0 {
		cfg.MaxEnvironments = 4
	}
	if cfg.PortLow == 0 {
		cfg.PortLow, cfg.PortHigh = 19000, 19010
	}
	if cfg.DefaultTTL == 0 {
		cfg.DefaultTTL = time.Minute
	}
	return New(cfg), clk, sim
}

func logTransitions(t *testing.T, label, requestID string, e *environment.Environment) {
	t.Helper()
	t.Logf("%s request=%s name=%s status=%s claim=%d holders=%v",
		label, requestID, e.Name, e.Status, e.ClaimVersion, e.Holders)
	for _, ev := range e.Events {
		t.Logf("    transition %s -> %s | reason=%-18s | req=%s | %s",
			ev.From, ev.To, ev.Reason, ev.RequestID, ev.Detail)
	}
}

func mustApply(t *testing.T, c *Controller, name, target, holder, reqID string, ttl time.Duration) *environment.Environment {
	t.Helper()
	e, err := c.Apply(context.Background(), environment.ApplyRequest{
		Name: name, Target: target, Holder: holder, TTL: ttl, RequestID: reqID,
	})
	if err != nil {
		t.Fatalf("Apply(%s) unexpected error: %v", reqID, err)
	}
	logTransitions(t, "apply-result", reqID, e)
	return e
}

func assertCode(t *testing.T, err error, want environment.ErrorCode) {
	t.Helper()
	if got := environment.CodeOf(err); got != want {
		t.Fatalf("error code mismatch: got=%q want=%q err=%v", got, want, err)
	}
}

// ---- tests ---------------------------------------------------------------

// Repeat application for the same target must converge to one instance.
func TestApplyConvergesToSameInstance(t *testing.T) {
	c, _, _ := newTestController(t, Config{})

	first := mustApply(t, c, "", "refs/pr/42", "", "req-1", 30*time.Second)
	second, err := c.Apply(context.Background(), environment.ApplyRequest{
		Target: "refs/pr/42", TTL: 30 * time.Second, RequestID: "req-2",
	})
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	logTransitions(t, "repeat-apply-result", "req-2", second)

	if first.Name != second.Name || first.Port != second.Port {
		t.Fatalf("repeat apply created a new instance: %+v vs %+v", first, second)
	}
	if second.ClaimVersion != 1 {
		t.Fatalf("want claim version 1 (no new resources), got %d", second.ClaimVersion)
	}
	if got := c.ports.InUse(); got != 1 {
		t.Fatalf("want exactly 1 port reserved, got %d", got)
	}
}

// Concurrent applications for the same target coalesce into one creation.
func TestConcurrentApplySingleInstance(t *testing.T) {
	c, _, sim := newTestController(t, Config{})

	const n = 32
	var wg sync.WaitGroup
	results := make([]*environment.Environment, n)
	errs := make([]error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			reqID := fmt.Sprintf("concurrent-%02d", i)
			e, err := c.Apply(context.Background(), environment.ApplyRequest{
				Target: "refs/pr/77", Holder: "", RequestID: reqID,
			})
			results[i], errs[i] = e, err
		}(i)
	}
	close(start)
	wg.Wait()

	port := 0
	name := ""
	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent apply %d failed: %v", i, err)
		}
		if port == 0 {
			port, name = results[i].Port, results[i].Name
		}
		if results[i].Port != port || results[i].Name != name {
			t.Fatalf("concurrent apply %d diverged: port=%d name=%s", i, results[i].Port, results[i].Name)
		}
		if results[i].ClaimVersion != 1 {
			t.Fatalf("concurrent apply %d created extra resources, claim=%d", i, results[i].ClaimVersion)
		}
	}
	if sim.RunningCount() != 1 {
		t.Fatalf("want exactly 1 running backend, got %d", sim.RunningCount())
	}
	if c.ports.InUse() != 1 {
		t.Fatalf("want 1 reserved port, got %d", c.ports.InUse())
	}
	if all := c.Snapshot(); len(all) != 1 {
		t.Fatalf("want 1 environment record, got %d", len(all))
	}
	t.Logf("decision basis: %d concurrent requests all observed single-flight on port %d", n, port)
}

// Two different targets requesting the same name get a NameConflict that is
// distinguishable from quota / port exhaustion.
func TestNameConflictIsRejectedAndDistinguishable(t *testing.T) {
	c, _, _ := newTestController(t, Config{MaxEnvironments: 10})

	mustApply(t, c, "shared-name", "refs/pr/1", "", "req-a", time.Minute)
	_, err := c.Apply(context.Background(), environment.ApplyRequest{
		Name: "shared-name", Target: "refs/pr/2", RequestID: "req-b",
	})
	assertCode(t, err, environment.ErrCodeNameConflict)

	// Different name, same quota pressure: quota failure has another code.
	c2, _, _ := newTestController(t, Config{MaxEnvironments: 1, PortLow: 20000, PortHigh: 20099})
	mustApply(t, c2, "one", "refs/pr/1", "", "req-c", time.Minute)
	_, err = c2.Apply(context.Background(), environment.ApplyRequest{
		Name: "two", Target: "refs/pr/2", RequestID: "req-d",
	})
	assertCode(t, err, environment.ErrCodeQuotaExhausted)

	// Wide quota but a single port: port failure has its own code.
	c3, _, _ := newTestController(t, Config{MaxEnvironments: 100, PortLow: 21000, PortHigh: 21000})
	mustApply(t, c3, "p1", "refs/pr/1", "", "req-e", time.Minute)
	_, err = c3.Apply(context.Background(), environment.ApplyRequest{
		Name: "p2", Target: "refs/pr/2", RequestID: "req-f",
	})
	assertCode(t, err, environment.ErrCodePortsExhausted)
}

// Releasing (destroying) an environment while holders exist must be refused;
// per-holder release works and the environment returns to Ready at zero.
func TestReleaseInUseRejectedAndHolderRelease(t *testing.T) {
	c, _, _ := newTestController(t, Config{})
	e := mustApply(t, c, "svc", "refs/pr/5", "", "req-apply", time.Minute)
	if e.Status != environment.StatusReady {
		t.Fatalf("want Ready, got %s", e.Status)
	}

	if _, err := c.Occupy(context.Background(), "svc", "alice", "req-occupy-1"); err != nil {
		t.Fatal(err)
	}
	e2, err := c.Occupy(context.Background(), "svc", "bob", "req-occupy-2")
	if err != nil {
		t.Fatal(err)
	}
	if e2.Status != environment.StatusInUse || len(e2.Holders) != 2 {
		t.Fatalf("want InUse with 2 holders, got %s %v", e2.Status, e2.Holders)
	}

	// Decision basis for rejection: holder list is present in the message.
	_, err = c.ReleaseAll(context.Background(), "svc", "req-destroy-1")
	assertCode(t, err, environment.ErrCodeInUse)
	t.Logf("release-all rejected while in use: %v", err)

	// Direct reclaim of an in-use env is rejected with the same InUse code.
	_, err = c.Reclaim(context.Background(), "svc", "req-reclaim-1")
	assertCode(t, err, environment.ErrCodeInUse)

	// One holder leaves: still in use, still cannot be released-all.
	e3, err := c.Release(context.Background(), "svc", "alice", "req-release-1")
	if err != nil {
		t.Fatal(err)
	}
	if e3.Status != environment.StatusInUse || len(e3.Holders) != 1 || e3.Holders[0] != "bob" {
		t.Fatalf("want InUse with [bob], got %s %v", e3.Status, e3.Holders)
	}
	_, err = c.ReleaseAll(context.Background(), "svc", "req-destroy-2")
	assertCode(t, err, environment.ErrCodeInUse)

	// Last holder leaves: Ready; repeated release by an unknown holder is
	// idempotent and must not error.
	e4, err := c.Release(context.Background(), "svc", "bob", "req-release-2")
	if err != nil {
		t.Fatal(err)
	}
	if e4.Status != environment.StatusReady || len(e4.Holders) != 0 {
		t.Fatalf("want Ready with 0 holders, got %s %v", e4.Status, e4.Holders)
	}
	e5, err := c.Release(context.Background(), "svc", "ghost", "req-release-3")
	if err != nil || e5.Status != environment.StatusReady {
		t.Fatalf("idempotent release of absent holder failed: %v status=%s", err, e5.Status)
	}

	// Now idle release-all is accepted.
	e6, err := c.ReleaseAll(context.Background(), "svc", "req-destroy-3")
	if err != nil || e6.Status != environment.StatusReleased {
		t.Fatalf("want Released, got %v %s", err, e6.Status)
	}
	// Idempotent second call.
	e7, err := c.ReleaseAll(context.Background(), "svc", "req-destroy-4")
	if err != nil || e7.Status != environment.StatusReleased {
		t.Fatalf("idempotent release-all failed: %v", err)
	}
}

// Expired Ready environments are reaped while InUse environments — even past
// their nominal TTL — keep running. A previously Released env is also swept.
func TestReclaimExpiredKeepsInUse(t *testing.T) {
	c, clk, _ := newTestController(t, Config{})

	mustApply(t, c, "idle", "refs/pr/10", "", "req-idle", 10*time.Second)
	busy := mustApply(t, c, "busy", "refs/pr/11", "", "req-busy-apply", 10*time.Second)
	if _, err := c.Occupy(context.Background(), "busy", "worker-1", "req-busy-occupy"); err != nil {
		t.Fatal(err)
	}
	mustApply(t, c, "done", "refs/pr/12", "", "req-done-apply", 10*time.Second)
	if _, err := c.ReleaseAll(context.Background(), "done", "req-done-release"); err != nil {
		t.Fatal(err)
	}

	clk.Advance(30 * time.Second) // idle TTL elapsed; busy also nominally expired

	results := c.ReclaimExpired(context.Background())
	t.Logf("sweep results: %+v", results)
	reclaimed := map[string]bool{}
	for _, r := range results {
		if !r.Reclaimed {
			t.Fatalf("unexpected failed sweep entry: %+v", r)
		}
		reclaimed[r.Name] = true
	}
	if !reclaimed["idle"] || !reclaimed["done"] {
		t.Fatalf("want idle and done reclaimed, got %v", reclaimed)
	}

	got, _ := c.Get("idle")
	if got.Status != environment.StatusReclaimed || got.Port != 0 {
		t.Fatalf("idle: want Reclaimed with freed port, got %s port=%d", got.Status, got.Port)
	}
	got, _ = c.Get("busy")
	if got.Status != environment.StatusInUse {
		t.Fatalf("decision basis violated: in-use env must survive expiry, got %s", got.Status)
	}
	if got.PID == 0 {
		t.Fatalf("in-use env backend must stay alive, pid=%d", got.PID)
	}
	t.Logf("busy env preserved past TTL because it is held by %v (port %d)", got.Holders, got.Port)

	// Manual reclaim of a still-valid Ready env is rejected distinctly.
	mustApply(t, c, "fresh", "refs/pr/13", "", "req-fresh", time.Hour)
	_, err := c.Reclaim(context.Background(), "fresh", "req-fresh-reclaim")
	assertCode(t, err, environment.ErrCodeNotReclaimable)
	_ = busy
}

// A failing reclaim leaves everything unchanged, can be retried, and the
// eventual success plus a further call are idempotent.
func TestReclaimFailureRetryAndIdempotency(t *testing.T) {
	c, clk, sim := newTestController(t, Config{})
	mustApply(t, c, "flaky", "refs/pr/20", "", "req-apply", time.Minute)
	// Expiry-driven reclaim: Ready env passes TTL, reaper attempts to stop.
	clk.Advance(2 * time.Minute)
	sim.FailStopTimes("flaky", 2)

	// Attempt 1: backend failure, state untouched, error recorded.
	_, err := c.Reclaim(context.Background(), "flaky", "req-reclaim-1")
	assertCode(t, err, environment.ErrCodeProvisioner)
	got, _ := c.Get("flaky")
	if got.Status != environment.StatusReady || len(got.ReclaimErrs) != 1 {
		t.Fatalf("after failed reclaim want Ready + 1 recorded error, got %s errs=%d",
			got.Status, len(got.ReclaimErrs))
	}
	if c.ports.InUse() != 1 {
		t.Fatalf("failed reclaim must not free the port, inUse=%d", c.ports.InUse())
	}

	// Attempt 2: still scripted to fail; retry is genuinely re-attempted.
	_, err = c.Reclaim(context.Background(), "flaky", "req-reclaim-2")
	assertCode(t, err, environment.ErrCodeProvisioner)
	got, _ = c.Get("flaky")
	if len(got.ReclaimErrs) != 2 {
		t.Fatalf("want 2 recorded failures, got %d", len(got.ReclaimErrs))
	}

	// Attempt 3: backend now succeeds; resources released; audit kept.
	e3, err := c.Reclaim(context.Background(), "flaky", "req-reclaim-3")
	if err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if e3.Status != environment.StatusReclaimed || e3.Port != 0 {
		t.Fatalf("want Reclaimed with freed resources, got %s port=%d", e3.Status, e3.Port)
	}
	if len(e3.ReclaimErrs) != 2 {
		t.Fatalf("successful retry must keep the failure audit trail, got %d", len(e3.ReclaimErrs))
	}
	if c.ports.InUse() != 0 {
		t.Fatalf("want port returned to pool, got %d", c.ports.InUse())
	}

	// Attempt 4: idempotent no-op carrying the same terminal state.
	e4, err := c.Reclaim(context.Background(), "flaky", "req-reclaim-4")
	if err != nil || e4.Status != environment.StatusReclaimed {
		t.Fatalf("idempotent reclaim failed: %v %s", err, e4.Status)
	}
	last := e4.Events[len(e4.Events)-1]
	if last.Reason != "reclaim-idempotent" {
		t.Fatalf("want idempotent marker event, got %s", last.Reason)
	}
}

// ReleaseAll stops the backend and frees the port; a stop failure leaves the
// Ready record and reservation untouched and is retryable.
func TestReleaseAllStopFailureKeepsState(t *testing.T) {
	c, _, sim := newTestController(t, Config{})
	mustApply(t, c, "flaky", "refs/pr/22", "", "req-apply", time.Minute)
	sim.FailStopTimes("flaky", 1)

	_, err := c.ReleaseAll(context.Background(), "flaky", "req-release-bad")
	assertCode(t, err, environment.ErrCodeProvisioner)
	got, _ := c.Get("flaky")
	if got.Status != environment.StatusReady || got.Port == 0 || got.PID == 0 {
		t.Fatalf("failed release-all must keep Ready + resources, got %s port=%d pid=%d",
			got.Status, got.Port, got.PID)
	}
	if c.ports.InUse() != 1 {
		t.Fatalf("failed release-all must keep port reserved, inUse=%d", c.ports.InUse())
	}
	// Retry succeeds; port freed; a later Reclaim just finalizes bookkeeping.
	e, err := c.ReleaseAll(context.Background(), "flaky", "req-release-ok")
	if err != nil || e.Status != environment.StatusReleased || e.Port != 0 {
		t.Fatalf("retry release-all: err=%v status=%s port=%d", err, e.Status, e.Port)
	}
	if c.ports.InUse() != 0 {
		t.Fatalf("want port freed, got %d", c.ports.InUse())
	}
	r, err := c.Reclaim(context.Background(), "flaky", "req-reclaim")
	if err != nil || r.Status != environment.StatusReclaimed {
		t.Fatalf("reclaim after release-all: err=%v status=%s", err, r.Status)
	}
}

// The background-style sweep also retries failed expiry reclaims.
func TestReaperRetriesFailedExpiryReclaim(t *testing.T) {
	c, clk, sim := newTestController(t, Config{})
	mustApply(t, c, "flaky2", "refs/pr/21", "", "req-apply2", time.Second)
	clk.Advance(2 * time.Second)
	sim.FailStopTimes("flaky2", 1)
	r1 := c.ReclaimExpired(context.Background())
	if len(r1) != 1 || r1[0].Retryable != true {
		t.Fatalf("want one retryable sweep result, got %+v", r1)
	}
	r2 := c.ReclaimExpired(context.Background())
	if len(r2) != 1 || r2[0].Reclaimed != true {
		t.Fatalf("want retry sweep to succeed, got %+v", r2)
	}
}

// Re-applying after reclaim starts a stable, explainable new generation.
func TestApplyAfterReclaimStable(t *testing.T) {
	c, clk, _ := newTestController(t, Config{})
	first := mustApply(t, c, "pe-refs-pr-30", "refs/pr/30", "", "req-apply-1", time.Minute)
	if first.ClaimVersion != 1 {
		t.Fatalf("want first generation claim=1, got %d", first.ClaimVersion)
	}
	if _, err := c.ReleaseAll(context.Background(), "pe-refs-pr-30", "req-release"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reclaim(context.Background(), "pe-refs-pr-30", "req-reclaim"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)

	again, err := c.Apply(context.Background(), environment.ApplyRequest{
		Target: "refs/pr/30", RequestID: "req-apply-2",
	})
	if err != nil {
		t.Fatalf("re-apply after reclaim: %v", err)
	}
	logTransitions(t, "reapply-result", "req-apply-2", again)
	if again.Status != environment.StatusReady || again.ClaimVersion != 2 {
		t.Fatalf("want Ready new generation claim=2, got %s claim=%d", again.Status, again.ClaimVersion)
	}
	if again.Port == 0 || again.PID == 0 {
		t.Fatalf("new generation must own fresh resources, port=%d pid=%d", again.Port, again.PID)
	}
	// A third application while active converges to generation 2.
	third, err := c.Apply(context.Background(), environment.ApplyRequest{
		Target: "refs/pr/30", RequestID: "req-apply-3",
	})
	if err != nil || third.ClaimVersion != 2 {
		t.Fatalf("want convergence to claim=2, got %v claim=%d", err, third.ClaimVersion)
	}
}

// A provisioning failure must roll back the reservation completely; the next
// application succeeds and no quota/port is leaked.
func TestProvisionFailureRollback(t *testing.T) {
	c, _, sim := newTestController(t, Config{MaxEnvironments: 1, PortLow: 22000, PortHigh: 22000})
	sim.FailStartNext("pe-refs-pr-40")

	_, err := c.Apply(context.Background(), environment.ApplyRequest{
		Target: "refs/pr/40", RequestID: "req-bad-apply",
	})
	assertCode(t, err, environment.ErrCodeProvisioner)
	if c.ports.Available() != 1 {
		t.Fatalf("want rolled-back port available again, got %d", c.ports.Available())
	}
	if len(c.Snapshot()) != 0 {
		t.Fatalf("want no lingering record after rollback, got %d", len(c.Snapshot()))
	}

	e := mustApply(t, c, "", "refs/pr/40", "", "req-good-apply", time.Minute)
	if e.Status != environment.StatusReady || e.Port != 22000 {
		t.Fatalf("retry after rollback should succeed, got %s port=%d", e.Status, e.Port)
	}
}

// Holder supplied at apply time must leave the environment directly InUse.
func TestApplyWithHolderIsInUse(t *testing.T) {
	c, _, _ := newTestController(t, Config{})
	e := mustApply(t, c, "", "refs/pr/50", "ci-job-9", "req-apply-held", time.Minute)
	if e.Status != environment.StatusInUse || len(e.Holders) != 1 {
		t.Fatalf("want InUse with one holder, got %s %v", e.Status, e.Holders)
	}
}

// A failed revive of a Released environment must restore the record to
// Released (not a dangling Pending) and return its port to the pool.
func TestReviveFailureRestoresReleasedRecord(t *testing.T) {
	c, clk, sim := newTestController(t, Config{MaxEnvironments: 1, PortLow: 27000, PortHigh: 27000})
	mustApply(t, c, "pe-r", "refs/pr/60", "", "req-apply", time.Minute)
	if _, err := c.ReleaseAll(context.Background(), "pe-r", "req-release"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Reclaim(context.Background(), "pe-r", "req-reclaim"); err != nil {
		t.Fatal(err)
	}
	clk.Advance(time.Second)

	// Reclaim freed the single port; revive once successfully.
	e := mustApply(t, c, "pe-r", "refs/pr/60", "", "req-revive-ok", time.Minute)
	if e.ClaimVersion != 2 || e.Status != environment.StatusReady {
		t.Fatalf("want revived claim=2 Ready, got claim=%d %s", e.ClaimVersion, e.Status)
	}
	if _, err := c.ReleaseAll(context.Background(), "pe-r", "req-release-2"); err != nil {
		t.Fatal(err)
	}
	sim.FailStartNext("pe-r")

	_, err := c.Apply(context.Background(), environment.ApplyRequest{
		Name: "pe-r", Target: "refs/pr/60", RequestID: "req-revive-bad",
	})
	assertCode(t, err, environment.ErrCodeProvisioner)
	got, _ := c.Get("pe-r")
	if got.Status != environment.StatusReleased {
		t.Fatalf("want record restored to Released after failed revive, got %s", got.Status)
	}
	if got.Port != 0 || got.PID != 0 || got.Endpoint != "" {
		t.Fatalf("released record must keep cleared resources, got port=%d pid=%d ep=%q",
			got.Port, got.PID, got.Endpoint)
	}
	if c.ports.Available() != 1 {
		t.Fatalf("want port returned after failed revive, available=%d", c.ports.Available())
	}
	// A subsequent successful revive still works and is generation 3.
	again := mustApply(t, c, "pe-r", "refs/pr/60", "", "req-revive-retry", time.Minute)
	if again.ClaimVersion != 3 || again.Status != environment.StatusReady {
		t.Fatalf("want claim=3 Ready after failed-then-successful revive, got %d %s",
			again.ClaimVersion, again.Status)
	}
}

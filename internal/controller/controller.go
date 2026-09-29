package controller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

// Clock is injectable so tests can drive expiry deterministically.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Logger is the minimal logging surface used by the controller.
type Logger interface {
	Printf(format string, args ...any)
}

type stdLogger struct{ l *log.Logger }

func (s stdLogger) Printf(format string, args ...any) { s.l.Printf(format, args...) }

// Config configures a Controller.
type Config struct {
	MaxEnvironments int
	PortLow         int
	PortHigh        int
	DefaultTTL      time.Duration
	Provisioner     Provisioner
	Clock           Clock
	Logger          Logger
}

// applyCall is the single-flight coordination record for one target.
type applyCall struct {
	done chan struct{}
	env  *environment.Environment
	err  error
}

// Controller is the orchestration core. It is safe for concurrent use.
type Controller struct {
	cfg Config

	mu     sync.Mutex
	envs   map[string]*environment.Environment
	target map[string]string // target -> active env name (pending/ready/in-use)
	ports  *PortPool

	inflight sync.Map // target -> *applyCall

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// New constructs a controller from cfg, applying defaults.
func New(cfg Config) *Controller {
	if cfg.MaxEnvironments <= 0 {
		cfg.MaxEnvironments = 8
	}
	if cfg.PortLow == 0 && cfg.PortHigh == 0 {
		cfg.PortLow, cfg.PortHigh = 19000, 19100
	}
	if cfg.DefaultTTL <= 0 {
		cfg.DefaultTTL = 60 * time.Second
	}
	if cfg.Provisioner == nil {
		cfg.Provisioner = NewSimProvisioner()
	}
	if cfg.Clock == nil {
		cfg.Clock = realClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = stdLogger{l: log.New(os.Stderr, "[controller] ", log.LstdFlags)}
	}
	return &Controller{
		cfg:    cfg,
		envs:   make(map[string]*environment.Environment),
		target: make(map[string]string),
		ports:  NewPortPool(cfg.PortLow, cfg.PortHigh),
	}
}

// Snapshot returns a deep-copied list of all known environment records,
// ordered by creation time.
func (c *Controller) Snapshot() []*environment.Environment {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*environment.Environment, 0, len(c.envs))
	for _, e := range c.envs {
		out = append(out, e.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out
}

// Get returns a deep copy of one environment by name.
func (c *Controller) Get(name string) (*environment.Environment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.envs[name]
	if e == nil {
		return nil, environment.NewError(environment.ErrCodeNotFound,
			fmt.Sprintf("environment %q not found", name))
	}
	return e.Clone(), nil
}

// Apply creates an environment for req.Target and waits until it is ready.
// Convergence guarantees:
//   - a repeat application for an active target returns the SAME instance
//     (same port, same claim version) — no duplicate is created;
//   - concurrent applications for the same target are coalesced into one
//     single-flight creation;
//   - a released record for the same target is revived with a fresh backend;
//   - a reclaimed (terminal) record is kept for audit and a new generation
//     is created under the same name (claim version is bumped).
func (c *Controller) Apply(ctx context.Context, req environment.ApplyRequest) (*environment.Environment, error) {
	if req.Target == "" {
		return nil, environment.NewError(environment.ErrCodeInvalidRequest, "target is required")
	}
	if req.TTL == 0 {
		req.TTL = c.cfg.DefaultTTL
	}
	if req.Name == "" {
		req.Name = nameForTarget(req.Target)
	}

	// Coalesce concurrent applications for the same target.
	call := &applyCall{done: make(chan struct{})}
	actual, loaded := c.inflight.LoadOrStore(req.Target, call)
	if loaded {
		leader := actual.(*applyCall)
		c.logf(req.RequestID, "apply target=%s name=%s -> joined in-flight creation", req.Target, req.Name)
		select {
		case <-leader.done:
			if leader.err != nil {
				return nil, withCtx(leader.err, req.Target, req.RequestID)
			}
			return c.convergedAfterWait(req, leader.env.Name)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	defer c.inflight.Delete(req.Target)

	env, err := c.createOrRevive(req)
	call.env, call.err = env, err
	close(call.done)
	if err != nil {
		return nil, err
	}
	return env.Clone(), nil
}

// createOrRevive validates isolation/quota, provisions the backend and moves
// Pending -> Ready. Any backend failure triggers a full rollback, so a failed
// application never consumes a port or quota slot.
func (c *Controller) createOrRevive(req environment.ApplyRequest) (*environment.Environment, error) {
	c.mu.Lock()
	existing := c.envs[req.Name]
	if existing != nil {
		switch {
		case existing.Status.Active():
			c.mu.Unlock()
			if existing.Target != req.Target {
				return nil, environment.NewError(environment.ErrCodeNameConflict,
					fmt.Sprintf("name %q is already used by target %q", req.Name, existing.Target))
			}
			return c.convergedAfterWait(req, req.Name)
		case existing.Status == environment.StatusReleased:
			if existing.Target != req.Target {
				c.mu.Unlock()
				return nil, environment.NewError(environment.ErrCodeNameConflict,
					fmt.Sprintf("name %q is reserved for target %q", req.Name, existing.Target))
			}
			// revive: fall through to re-provision below, reusing the record.
		case existing.Status == environment.StatusReclaimed:
			// terminal record kept for audit; a new generation reuses the name.
			c.appendEventLocked(existing, existing.Status, existing.Status,
				"superseded", "a new generation reuses this name after reclaim", req.RequestID)
		}
	}

	// Quota is counted first so quota exhaustion and port exhaustion produce
	// distinct, deterministic errors. A Released record being revived already
	// holds its quota slot, so its resurrection adds nothing; a brand-new
	// record (or a Reclaimed name starting a new generation) adds one.
	quotaIncrement := 1
	if existing != nil && existing.Status == environment.StatusReleased {
		quotaIncrement = 0
	}
	if c.liveCountLocked()+quotaIncrement > c.cfg.MaxEnvironments {
		c.mu.Unlock()
		return nil, environment.NewError(environment.ErrCodeQuotaExhausted,
			fmt.Sprintf("quota of %d environments reached", c.cfg.MaxEnvironments))
	}
	port, err := c.ports.Allocate()
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}

	now := c.cfg.Clock.Now()
	env := existing // may be a released/terminal record being revived
	isNew := env == nil
	if isNew {
		env = &environment.Environment{
			Name:      req.Name,
			Target:    req.Target,
			CreatedAt: now,
		}
		c.envs[req.Name] = env
	}
	from := env.Status
	// Snapshot the pre-Pending fields so a backend failure can restore a
	// revived Released/Reclaimed record byte-for-byte instead of leaving it
	// pointing at a rolled-back port.
	prev := *env
	prevEventsLen := len(env.Events)
	env.Status = environment.StatusPending
	env.Target = req.Target
	env.Port = port
	env.PID = 0
	env.Endpoint = ""
	env.Holders = nil
	env.TTL = req.TTL
	env.ExpiresAt = now.Add(req.TTL)
	env.UpdatedAt = now
	env.ClaimVersion++
	env.ReadyAt = time.Time{}
	c.target[req.Target] = req.Name
	c.appendEventLocked(env, from, environment.StatusPending, "apply",
		fmt.Sprintf("allocated port=%d quota_used=%d", port, c.liveCountLocked()), req.RequestID)
	c.mu.Unlock()

	c.logf(req.RequestID, "apply target=%s name=%s status=%q -> provisioning on port %d",
		req.Target, req.Name, string(from), port)

	inst, perr := c.cfg.Provisioner.Start(context.Background(), req.Name, port)
	if perr != nil {
		c.rollbackFailedStart(req, port, perr, isNew, prev, prevEventsLen)
		return nil, withCtx(perr, req.Target, req.RequestID)
	}

	c.mu.Lock()
	now = c.cfg.Clock.Now()
	env.Status = environment.StatusReady
	env.PID = inst.PID
	env.Endpoint = inst.Endpoint
	env.ReadyAt = now
	env.UpdatedAt = now
	c.appendEventLocked(env, environment.StatusPending, environment.StatusReady, "ready",
		fmt.Sprintf("endpoint=%s pid=%d", inst.Endpoint, inst.PID), req.RequestID)
	if req.Holder != "" {
		env.Holders = []string{req.Holder}
		env.Status = environment.StatusInUse
		c.appendEventLocked(env, environment.StatusReady, environment.StatusInUse, "occupied",
			"initial holder="+req.Holder, req.RequestID)
	}
	env.ExpiresAt = now.Add(env.TTL)
	env = env.Clone()
	c.mu.Unlock()

	c.logf(req.RequestID, "apply target=%s name=%s status=Ready endpoint=%s holders=%d claim=%d",
		req.Target, req.Name, inst.Endpoint, len(env.Holders), env.ClaimVersion)
	return env, nil
}

// rollbackFailedStart releases the reserved port, removes the pending mapping
// and records the failure, guaranteeing no half-applied allocation remains.
// A brand-new record is deleted; a revived Released/Reclaimed record is
// restored to its exact pre-attempt snapshot.
func (c *Controller) rollbackFailedStart(req environment.ApplyRequest, port int, perr error, isNew bool, prev environment.Environment, prevEventsLen int) {
	c.ports.Release(port)
	c.mu.Lock()
	defer c.mu.Unlock()
	env := c.envs[req.Name]
	if env == nil || env.Status != environment.StatusPending {
		return
	}
	if isNew {
		delete(c.envs, req.Name)
		if name, ok := c.target[req.Target]; ok && name == req.Name {
			delete(c.target, req.Target)
		}
	} else {
		// Restore the record as it was before the attempt, keeping only the
		// pre-existing events, then append the failure audit entry.
		restored := prev
		restored.Events = append([]environment.Event(nil), prev.Events[:prevEventsLen]...)
		*env = restored
		if name, ok := c.target[req.Target]; ok && name == req.Name {
			delete(c.target, req.Target)
		}
		c.appendEventLocked(env, environment.StatusPending, env.Status, "provision-failed",
			"revive rolled back port and quota: "+perr.Error(), req.RequestID)
	}
	c.logf(req.RequestID, "apply target=%s FAILED rollback port=%d reason=%s",
		req.Target, port, environment.CodeOf(perr))
}

// convergedAfterWait handles a duplicate/concurrent application that found an
// already active instance: it attaches a holder (if given), refreshes TTL and
// returns the SAME instance — no new port or backend is created.
func (c *Controller) convergedAfterWait(req environment.ApplyRequest, name string) (*environment.Environment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	env := c.envs[name]
	if env == nil || !env.Status.Active() {
		// Leader was rolled back or the instance vanished; caller must retry.
		return nil, environment.NewError(environment.ErrCodeNotFound,
			"the in-flight instance is no longer active")
	}
	if req.Holder != "" && !env.HasHolder(req.Holder) {
		prev := env.Status
		env.Holders = append(env.Holders, req.Holder)
		env.Status = environment.StatusInUse
		c.appendEventLocked(env, prev, environment.StatusInUse, "occupied",
			"converged duplicate holder="+req.Holder, req.RequestID)
	}
	env.TTL = req.TTL
	env.ExpiresAt = c.cfg.Clock.Now().Add(req.TTL)
	env.UpdatedAt = c.cfg.Clock.Now()
	c.appendEventLocked(env, env.Status, env.Status, "apply-converged",
		fmt.Sprintf("returning existing instance claim=%d", env.ClaimVersion), req.RequestID)
	c.logf(req.RequestID, "apply target=%s CONVERGED name=%s claim=%d",
		req.Target, name, env.ClaimVersion)
	return env.Clone(), nil
}

// Occupy adds a holder to an already Ready/InUse environment and refreshes its
// TTL. Re-occupying with the same holder is idempotent.
func (c *Controller) Occupy(ctx context.Context, name, holder, requestID string) (*environment.Environment, error) {
	if holder == "" {
		return nil, environment.NewError(environment.ErrCodeInvalidRequest, "holder is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	env, err := c.requireActiveLocked(name)
	if err != nil {
		return nil, withCtx(err, name, requestID)
	}
	prev := env.Status
	if !env.HasHolder(holder) {
		env.Holders = append(env.Holders, holder)
	}
	env.Status = environment.StatusInUse
	env.ExpiresAt = c.cfg.Clock.Now().Add(env.TTL)
	env.UpdatedAt = c.cfg.Clock.Now()
	c.appendEventLocked(env, prev, environment.StatusInUse, "occupied",
		"holder="+holder, requestID)
	c.logf(requestID, "occupy name=%s holder=%s holders=%d", name, holder, len(env.Holders))
	return env.Clone(), nil
}

// Release removes one holder. When the last holder leaves, the environment
// returns to Ready (resources kept, TTL refreshed). It never destroys
// resources and never fails for an unknown holder (idempotent release).
func (c *Controller) Release(ctx context.Context, name, holder, requestID string) (*environment.Environment, error) {
	if holder == "" {
		return nil, environment.NewError(environment.ErrCodeInvalidRequest, "holder is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	env, err := c.requireActiveLocked(name)
	if err != nil {
		return nil, withCtx(err, name, requestID)
	}
	prev := env.Status
	kept := env.Holders[:0]
	present := false
	for _, h := range env.Holders {
		if h == holder {
			present = true
			continue
		}
		kept = append(kept, h)
	}
	env.Holders = kept
	switch {
	case len(env.Holders) > 0:
		env.Status = environment.StatusInUse
	case prev == environment.StatusInUse:
		env.Status = environment.StatusReady
		env.ExpiresAt = c.cfg.Clock.Now().Add(env.TTL)
	default:
		env.Status = environment.StatusReady
	}
	detail := "holder=" + holder
	if !present {
		detail += " (already absent; idempotent)"
	}
	c.appendEventLocked(env, prev, env.Status, "released", detail, requestID)
	c.logf(requestID, "release name=%s holder=%s remaining=%d status=%s",
		name, holder, len(env.Holders), env.Status)
	return env.Clone(), nil
}

// ReleaseAll releases an IDLE environment back to the pool: Ready ->
// Released. The backend is stopped and the host port is freed synchronously,
// so a Released environment no longer holds a port; it keeps counting against
// quota only until Reclaim finishes the bookkeeping. Environments that still
// have holders are rejected with ErrCodeInUse; the explicit error (and the
// holder list in its detail) is what prevents a release-while-in-use from
// silently half-succeeding.
func (c *Controller) ReleaseAll(ctx context.Context, name, requestID string) (*environment.Environment, error) {
	c.mu.Lock()
	env := c.envs[name]
	if env == nil {
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeNotFound,
			fmt.Sprintf("environment %q not found", name)), name, requestID)
	}
	if len(env.Holders) > 0 || env.Status == environment.StatusInUse {
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeInUse,
			fmt.Sprintf("environment %q is in use by %d holder(s) %v", name, len(env.Holders), env.Holders)),
			name, requestID)
	}
	switch env.Status {
	case environment.StatusReady:
		// allowed
	case environment.StatusPending:
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeNotReady,
			fmt.Sprintf("environment %q is still provisioning", name)), name, requestID)
	case environment.StatusReleased:
		c.mu.Unlock()
		return c.Get(name) // idempotent
	default:
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeNotReclaimable,
			fmt.Sprintf("environment %q is %s and cannot be released", name, env.Status)),
			name, requestID)
	}
	prev := env.Status
	port := env.Port
	record := env.Clone()
	c.mu.Unlock()

	// Stop the backend outside the lock. On failure the Ready state and port
	// reservation stay untouched — nothing is changed by half.
	if err := c.cfg.Provisioner.Stop(context.Background(), record); err != nil {
		c.mu.Lock()
		c.appendEventLocked(env, prev, prev, "release-failed",
			"backend stop failed; state unchanged: "+err.Error(), requestID)
		c.mu.Unlock()
		c.logf(requestID, "release-all name=%s FAILED reason=%s (state unchanged)",
			name, environment.CodeOf(err))
		return nil, withCtx(err, name, requestID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	env.Status = environment.StatusReleased
	env.PID = 0
	env.Endpoint = ""
	env.Port = 0
	env.UpdatedAt = c.cfg.Clock.Now()
	delete(c.target, env.Target)
	c.ports.Release(port)
	c.appendEventLocked(env, prev, environment.StatusReleased, "release-all",
		fmt.Sprintf("idle environment returned to pool; backend stopped, port=%d freed; awaiting reclaim", port),
		requestID)
	c.logf(requestID, "release-all name=%s status=Released port=%d freed", name, port)
	return env.Clone(), nil
}

// Reclaim destroys a Released environment, or a Ready one whose TTL has
// elapsed. It is idempotent: reclaiming an already Reclaimed environment
// returns it unchanged. In-use and unexpired ready environments are rejected
// with distinct codes (InUse / NotReclaimable).
func (c *Controller) Reclaim(ctx context.Context, name, requestID string) (*environment.Environment, error) {
	return c.reclaimOne(name, requestID)
}

// reclaimOne implements both manual Reclaim and the reaper sweep: the
// eligibility rules are identical (Released always; Ready once TTL elapsed),
// so expiry can be reclaimed through either entry point.
func (c *Controller) reclaimOne(name, requestID string) (*environment.Environment, error) {
	c.mu.Lock()
	env := c.envs[name]
	if env == nil {
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeNotFound,
			fmt.Sprintf("environment %q not found", name)), name, requestID)
	}
	switch env.Status {
	case environment.StatusReclaimed:
		c.appendEventLocked(env, environment.StatusReclaimed, environment.StatusReclaimed,
			"reclaim-idempotent", "already reclaimed; no-op", requestID)
		c.mu.Unlock()
		c.logf(requestID, "reclaim name=%s IDEMPOTENT already=Reclaimed", name)
		return env.Clone(), nil
	case environment.StatusInUse:
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeInUse,
			fmt.Sprintf("environment %q is in use and cannot be reclaimed", name)), name, requestID)
	case environment.StatusPending:
		c.mu.Unlock()
		return nil, withCtx(environment.NewError(environment.ErrCodeNotReady,
			fmt.Sprintf("environment %q is still provisioning", name)), name, requestID)
	case environment.StatusReady:
		// A Ready environment is reclaimable once its TTL has elapsed,
		// whether the caller is the reaper or a manual request.
		if env.ExpiresAt.After(c.cfg.Clock.Now()) {
			c.mu.Unlock()
			return nil, withCtx(environment.NewError(environment.ErrCodeNotReclaimable,
				fmt.Sprintf("environment %q has not expired yet", name)), name, requestID)
		}
	case environment.StatusReleased:
		// ReleaseAll already stopped the backend and freed the port; reclaim
		// only finalizes the bookkeeping, so it cannot fail and is immediate.
		prev := env.Status
		env.Status = environment.StatusReclaimed
		env.UpdatedAt = c.cfg.Clock.Now()
		if current, ok := c.target[env.Target]; ok && current == name {
			delete(c.target, env.Target)
		}
		c.appendEventLocked(env, prev, environment.StatusReclaimed, "reclaimed",
			"finalized after release-all (backend and port already released)", requestID)
		c.mu.Unlock()
		c.logf(requestID, "reclaim name=%s status=Reclaimed (finalize after release-all)", name)
		return c.Get(name)
	}

	port := env.Port
	prev := env.Status
	record := env.Clone()
	c.mu.Unlock()

	c.logf(requestID, "reclaim name=%s stopping backend pid=%d port=%d", name, record.PID, port)
	if err := c.cfg.Provisioner.Stop(context.Background(), record); err != nil {
		c.mu.Lock()
		env.ReclaimErrs = append(env.ReclaimErrs, environment.ReclaimFailure{
			Time:   c.cfg.Clock.Now(),
			Reason: err.Error(),
		})
		env.UpdatedAt = c.cfg.Clock.Now()
		c.appendEventLocked(env, env.Status, env.Status, "reclaim-failed",
			"backend stop failed; will retry: "+err.Error(), requestID)
		c.mu.Unlock()
		c.logf(requestID, "reclaim name=%s FAILED reason=%s (state unchanged, retryable)",
			name, environment.CodeOf(err))
		return nil, withCtx(err, name, requestID)
	}

	c.mu.Lock()
	env.Status = environment.StatusReclaimed
	env.PID = 0
	env.Endpoint = ""
	env.Port = 0
	env.Holders = nil
	env.UpdatedAt = c.cfg.Clock.Now()
	if current, ok := c.target[env.Target]; ok && current == name {
		delete(c.target, env.Target)
	}
	c.appendEventLocked(env, prev, environment.StatusReclaimed, "reclaimed",
		fmt.Sprintf("backend stopped; freed port=%d", port), requestID)
	c.ports.Release(port)
	c.mu.Unlock()

	c.logf(requestID, "reclaim name=%s status=Reclaimed port=%d freed (idempotent thereafter)",
		name, port)
	return c.Get(name)
}

// ReclaimResult summarizes one outcome of ReclaimExpired.
type ReclaimResult struct {
	Name      string                `json:"name"`
	Reclaimed bool                  `json:"reclaimed"`
	Retryable bool                  `json:"retryable"`
	Code      environment.ErrorCode `json:"code,omitempty"`
	Message   string                `json:"message,omitempty"`
}

// ReclaimExpired reclaims every environment eligible for expiry. Only
// Ready environments past their ExpiresAt are eligible: InUse environments
// keep running untouched even if their nominal TTL elapsed, and Released
// environments are also destroyed. Failed attempts leave the state unchanged
// and are retried on the next invocation; success is idempotent.
func (c *Controller) ReclaimExpired(ctx context.Context) []ReclaimResult {
	now := c.cfg.Clock.Now()
	c.mu.Lock()
	var names []string
	for _, e := range c.envs {
		if e.Status == environment.StatusReleased {
			names = append(names, e.Name)
			continue
		}
		if e.Status == environment.StatusReady && !e.ExpiresAt.After(now) {
			names = append(names, e.Name)
		}
	}
	c.mu.Unlock()

	results := make([]ReclaimResult, 0, len(names))
	for _, name := range names {
		_, err := c.reclaimOne(name, "reaper")
		if err == nil {
			results = append(results, ReclaimResult{Name: name, Reclaimed: true})
			continue
		}
		code := environment.CodeOf(err)
		// Skips are expected races (e.g. re-occupied between scan and reclaim).
		results = append(results, ReclaimResult{
			Name: name, Reclaimed: false,
			Retryable: code == environment.ErrCodeProvisioner,
			Code:      code, Message: err.Error(),
		})
	}
	return results
}

// StartReaper launches a background ticker driving ReclaimExpired until
// Close is called.
func (c *Controller) StartReaper(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.ReclaimExpired(ctx)
			}
		}
	}()
}

// Close stops the background reaper and waits for it to finish.
func (c *Controller) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// ---- internals ---------------------------------------------------------

func (c *Controller) requireActiveLocked(name string) (*environment.Environment, error) {
	env := c.envs[name]
	if env == nil {
		return nil, environment.NewError(environment.ErrCodeNotFound,
			fmt.Sprintf("environment %q not found", name))
	}
	if !env.Status.Active() {
		return nil, environment.NewError(environment.ErrCodeNotReady,
			fmt.Sprintf("environment %q is %s, not active", name, env.Status))
	}
	return env, nil
}

// liveCountLocked counts records that currently consume quota: active
// (Pending/Ready/InUse) plus Released (resources are gone but the name is
// reserved until reclaim). Reclaimed records are free.
func (c *Controller) liveCountLocked() int {
	n := 0
	for _, e := range c.envs {
		if e.Status.Active() || e.Status == environment.StatusReleased {
			n++
		}
	}
	return n
}

func (c *Controller) appendEventLocked(e *environment.Environment, from, to environment.Status, reason, detail, requestID string) {
	e.Events = append(e.Events, environment.Event{
		Time:      c.cfg.Clock.Now(),
		From:      from,
		To:        to,
		Reason:    reason,
		Detail:    detail,
		RequestID: requestID,
	})
}

func (c *Controller) logf(requestID, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if requestID == "" {
		c.cfg.Logger.Printf("%s", msg)
	} else {
		c.cfg.Logger.Printf("[req=%s] %s", requestID, msg)
	}
}

func (c *Controller) logTransition(e *environment.Environment, from, to environment.Status, reqID, reason, detail string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appendEventLocked(e, from, to, reason, detail, reqID)
}

func withCtx(err error, target, requestID string) error {
	if err == nil {
		return nil
	}
	if errors.Is(context.Canceled, err) || errors.Is(context.DeadlineExceeded, err) {
		return err
	}
	return environment.WithContext(err, target, requestID)
}

// nameForTarget derives a stable, filesystem-friendly name from a target
// identifier so that repeat applications of the same target converge.
func nameForTarget(target string) string {
	b := make([]byte, 0, len(target))
	for _, r := range target {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-':
			b = append(b, byte(r))
		default:
			b = append(b, '-')
		}
	}
	name := string(b)
	for len(name) > 0 && name[0] == '-' {
		name = name[1:]
	}
	if name == "" {
		name = "env"
	}
	return "pe-" + name
}

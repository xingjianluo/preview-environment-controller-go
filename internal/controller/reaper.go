package controller

import (
	"context"
	"sync"
	"time"
)

// Reaper periodically drives Controller.ReclaimExpired and thereby retries
// any reclaim that previously failed. It is a thin lifecycle wrapper around
// the controller's own loop, provided so callers can construct, start and
// stop it explicitly.
type Reaper struct {
	c        *Controller
	interval time.Duration

	mu      sync.Mutex
	started bool
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewReaper creates a Reaper running every interval.
func NewReaper(c *Controller, interval time.Duration) *Reaper {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &Reaper{c: c, interval: interval}
}

// Start launches the background loop. Calling Start twice is a no-op.
func (r *Reaper) Start() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	r.started = true

	go func() {
		defer close(r.done)
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Failures are recorded inside the controller and retried on
				// the following tick; nothing to do with the results here.
				r.c.ReclaimExpired(ctx)
			}
		}
	}()
}

// Stop terminates the loop and waits for it to exit.
func (r *Reaper) Stop() {
	r.mu.Lock()
	if !r.started {
		r.mu.Unlock()
		return
	}
	cancel := r.cancel
	done := r.done
	r.started = false
	r.mu.Unlock()
	cancel()
	<-done
}

// TickOnce performs one synchronous sweep (useful for tests and diagnostics).
func (r *Reaper) TickOnce(ctx context.Context) []ReclaimResult {
	return r.c.ReclaimExpired(ctx)
}

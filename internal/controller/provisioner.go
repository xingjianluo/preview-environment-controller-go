package controller

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

// Instance is the backend handle of a provisioned environment.
type Instance struct {
	Endpoint string
	PID      int
}

// Provisioner abstracts local process / container backends. The controller
// talks to it only through this interface, so the simulation used here can
// later be swapped for real containers without touching orchestration logic.
type Provisioner interface {
	Start(ctx context.Context, name string, port int) (Instance, error)
	Stop(ctx context.Context, env *environment.Environment) error
	Health(ctx context.Context, env *environment.Environment) error
}

type runningInstance struct {
	instance Instance
	port     int
}

// SimProvisioner is an in-memory backend used by tests and local runs.
// It can be scripted to fail provisioning or stopping specific names a fixed
// number of times, which is what makes reclaim-retry scenarios reproducible
// without any real OS process.
type SimProvisioner struct {
	mu        sync.Mutex
	running   map[string]runningInstance
	failStart map[string]int
	failStop  map[string]int
	pidSeq    atomic.Int64
}

// NewSimProvisioner creates an empty simulated backend.
func NewSimProvisioner() *SimProvisioner {
	return &SimProvisioner{
		running:   make(map[string]runningInstance),
		failStart: make(map[string]int),
		failStop:  make(map[string]int),
	}
}

// FailStartNext makes the next Start attempt for name fail once.
func (p *SimProvisioner) FailStartNext(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failStart[name]++
}

// FailStopTimes makes the next n Stop attempts for name fail before
// succeeding. Failures are consumed one per attempt, so callers must retry.
func (p *SimProvisioner) FailStopTimes(name string, n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.failStop[name] += n
}

// Start simulates launching a local process / container on port.
func (p *SimProvisioner) Start(ctx context.Context, name string, port int) (Instance, error) {
	if err := ctx.Err(); err != nil {
		return Instance{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.running[name]; ok {
		return Instance{}, environment.NewError(environment.ErrCodeProvisioner,
			fmt.Sprintf("backend for %q already running", name))
	}
	if p.failStart[name] > 0 {
		p.failStart[name]--
		return Instance{}, environment.NewError(environment.ErrCodeProvisioner,
			fmt.Sprintf("simulated start failure for %q", name))
	}
	pid := int(p.pidSeq.Add(1)) + 20000
	inst := Instance{Endpoint: fmt.Sprintf("http://127.0.0.1:%d", port), PID: pid}
	p.running[name] = runningInstance{instance: inst, port: port}
	return inst, nil
}

// Stop simulates terminating the backend. Stopping an unknown/already-stopped
// instance succeeds, which is what makes controller-side reclaim idempotent.
func (p *SimProvisioner) Stop(ctx context.Context, env *environment.Environment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.running[env.Name]; !ok {
		return nil
	}
	if p.failStop[env.Name] > 0 {
		p.failStop[env.Name]--
		return environment.NewError(environment.ErrCodeProvisioner,
			fmt.Sprintf("simulated stop failure for %q", env.Name))
	}
	delete(p.running, env.Name)
	return nil
}

// Health reports whether the backend instance is still alive.
func (p *SimProvisioner) Health(ctx context.Context, env *environment.Environment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.running[env.Name]; !ok {
		return environment.NewError(environment.ErrCodeProvisioner,
			fmt.Sprintf("backend for %q is not running", env.Name))
	}
	return nil
}

// RunningCount reports how many simulated backends are alive.
func (p *SimProvisioner) RunningCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.running)
}

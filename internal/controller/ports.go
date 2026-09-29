package controller

import (
	"sync"

	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

// PortPool allocates a unique host port per active environment. Allocation
// is a hard reservation independent of the OS: two environments can never
// receive the same port, and exhaustion is reported explicitly rather than
// as a bind error.
type PortPool struct {
	mu       sync.Mutex
	low      int
	high     int
	occupied map[int]bool
}

// NewPortPool creates a pool over the inclusive range [low, high].
func NewPortPool(low, high int) *PortPool {
	if low <= 0 || high < low {
		low, high = 19000, 19100
	}
	return &PortPool{low: low, high: high, occupied: make(map[int]bool)}
}

// Allocate reserves and returns the next free port.
func (p *PortPool) Allocate() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for port := p.low; port <= p.high; port++ {
		if !p.occupied[port] {
			p.occupied[port] = true
			return port, nil
		}
	}
	return 0, environment.NewError(environment.ErrCodePortsExhausted,
		"no free host port in range")
}

// Release returns a port to the pool. Releasing an unused port is a no-op,
// which keeps reclaim retries idempotent.
func (p *PortPool) Release(port int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.occupied, port)
}

// InUse returns the number of currently reserved ports.
func (p *PortPool) InUse() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.occupied)
}

// Available returns how many more ports can be allocated.
func (p *PortPool) Available() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.high - p.low + 1 - len(p.occupied)
}

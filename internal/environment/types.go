package environment

import "time"

// Status is the lifecycle state of a preview environment instance.
type Status string

const (
	// Pending means the instance has been allocated but the backend
	// (simulated process / container) has not reported ready yet.
	StatusPending Status = "Pending"
	// Ready means the instance is provisioned and not occupied.
	StatusReady Status = "Ready"
	// InUse means the instance is provisioned and held by >=1 holders.
	StatusInUse Status = "InUse"
	// Released means the instance was released (either voluntarily
	// released back to the pool or force-released while idle); it is no
	// longer active but the name is kept reserved and counts toward quota
	// until it is reclaimed.
	StatusReleased Status = "Released"
	// Reclaimed is terminal: resources have been destroyed and the name is
	// free for a brand new application.
	StatusReclaimed Status = "Reclaimed"
)

// IsTerminal reports whether the state cannot transition any further.
func (s Status) IsTerminal() bool {
	return s == StatusReclaimed
}

// Active reports whether the environment currently holds live resources
// (a backend instance + a host port) and counts against the quota.
func (s Status) Active() bool {
	return s == StatusPending || s == StatusReady || s == StatusInUse
}

// Event records a single state transition for explainability.
type Event struct {
	Time      time.Time `json:"time"`
	From      Status    `json:"from"`
	To        Status    `json:"to"`
	Reason    string    `json:"reason"`
	Detail    string    `json:"detail"`
	RequestID string    `json:"request_id"`
}

// Environment is a single preview-environment instance record.
type Environment struct {
	Name      string        `json:"name"`
	Target    string        `json:"target"`
	Status    Status        `json:"status"`
	Port      int           `json:"port"`
	PID       int           `json:"pid"`
	Holders   []string      `json:"holders"`
	Endpoint  string        `json:"endpoint"`
	TTL       time.Duration `json:"ttl"`
	ExpiresAt time.Time     `json:"expires_at"`
	ReadyAt   time.Time     `json:"ready_at"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`

	// ReclaimErrs is the append-only history of failed reclaim attempts.
	// A later successful retry does not erase it, so the audit trail of
	// "failed -> retried -> succeeded" stays complete and explainable.
	ReclaimErrs []ReclaimFailure `json:"reclaim_errors,omitempty"`

	// Events is the ordered transition log used by tests and operators.
	Events []Event `json:"events"`

	// ClaimVersion is bumped whenever the backing resources (port/backend)
	// are (re)created. Apply responses carrying the same ClaimVersion are
	// convergent duplicates rather than new instances.
	ClaimVersion int64 `json:"claim_version"`
}

// ReclaimFailure records one failed reclaim attempt.
type ReclaimFailure struct {
	Time   time.Time `json:"time"`
	Reason string    `json:"reason"`
}

// HasHolder reports whether holder currently holds the environment.
func (e *Environment) HasHolder(holder string) bool {
	for _, h := range e.Holders {
		if h == holder {
			return true
		}
	}
	return false
}

// Clone returns a deep copy of the environment record safe to hand to
// callers without holding the controller lock.
func (e *Environment) Clone() *Environment {
	if e == nil {
		return nil
	}
	cp := *e
	if e.Holders != nil {
		cp.Holders = append([]string(nil), e.Holders...)
	}
	if e.ReclaimErrs != nil {
		cp.ReclaimErrs = append([]ReclaimFailure(nil), e.ReclaimErrs...)
	}
	if e.Events != nil {
		cp.Events = append([]Event(nil), e.Events...)
	}
	return &cp
}

package environment

import "time"

// ApplyRequest describes one application for a preview environment.
type ApplyRequest struct {
	Name      string
	Target    string
	Holder    string
	TTL       time.Duration
	RequestID string
}

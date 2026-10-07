package service

import (
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

// 统一响应信封：成功 data 非空，失败 error 非空且带可区分原因码。
type response struct {
	RequestID string   `json:"request_id"`
	Data      any      `json:"data,omitempty"`
	Error     *errBody `json:"error,omitempty"`
}

type errBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type acquireRequest struct {
	Name   string `json:"name"`
	Holder string `json:"holder"`
}

type renewRequest struct {
	Name   string `json:"name"`
	Holder string `json:"holder"`
	Extend string `json:"extend"` // 如 "30s"；空/非法时按控制器默认租约时长
}

type releaseRequest struct {
	Name   string `json:"name"`
	Holder string `json:"holder"`
}

type envView struct {
	Name       string               `json:"name"`
	Port       int                  `json:"port"`
	State      string               `json:"state"`
	Holders    []string             `json:"holders"`
	CreatedAt  time.Time            `json:"created_at"`
	ExpiresAt  time.Time            `json:"expires_at"`
	Leases     map[string]time.Time `json:"leases"`
	ReleasedAt *time.Time           `json:"released_at,omitempty"`
	ReclaimErr string               `json:"reclaim_err,omitempty"`
}

func toEnvView(e *preview.Environment) envView {
	v := envView{
		Name:       e.Name,
		Port:       e.Port,
		State:      string(e.State),
		Holders:    append([]string(nil), e.Holders...),
		CreatedAt:  e.CreatedAt,
		ExpiresAt:  e.ExpiresAt,
		Leases:     map[string]time.Time{},
		ReclaimErr: e.ReclaimErr,
	}
	for h, t := range e.Leases {
		v.Leases[h] = t
	}
	if !e.ReleasedAt.IsZero() {
		ra := e.ReleasedAt
		v.ReleasedAt = &ra
	}
	return v
}

type acquireView struct {
	Env       envView `json:"env"`
	Created   bool    `json:"created"`
	Rationale string  `json:"rationale"`
}

type renewView struct {
	Env        envView   `json:"env"`
	OldExpires time.Time `json:"old_expires"`
	NewExpires time.Time `json:"new_expires"`
	Rationale  string    `json:"rationale"`
}

type reclaimView struct {
	Reclaimed []string  `json:"reclaimed"`
	Skipped   []skipped `json:"skipped"`
	Failures  []failure `json:"failures"`
	Succeeded bool      `json:"succeeded"`
}

type skipped struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type failure struct {
	Name string `json:"name"`
	Err  string `json:"err"`
}

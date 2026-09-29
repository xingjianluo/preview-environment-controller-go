package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/controller"
	"github.com/xingjianluo/preview-environment-controller-go/internal/environment"
)

// Handler exposes the controller over HTTP so that every entry point
// (library / HTTP / CLI) sees exactly the same semantics and error codes.
type Handler struct {
	c *controller.Controller
}

// NewHandler builds a Handler backed by c.
func NewHandler(c *controller.Controller) *Handler { return &Handler{c: c} }

// Register wires the routes onto mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/environments", h.apply)
	mux.HandleFunc("GET /v1/environments", h.list)
	mux.HandleFunc("GET /v1/environments/{name}", h.get)
	mux.HandleFunc("POST /v1/environments/{name}/occupy", h.occupy)
	mux.HandleFunc("POST /v1/environments/{name}/release", h.release)
	mux.HandleFunc("POST /v1/environments/{name}/release-all", h.releaseAll)
	mux.HandleFunc("POST /v1/environments/{name}/reclaim", h.reclaim)
	mux.HandleFunc("POST /v1/reclaim-expired", h.reclaimExpired)
}

type applyRequestDTO struct {
	Name      string `json:"name,omitempty"`
	Target    string `json:"target"`
	Holder    string `json:"holder,omitempty"`
	TTL       string `json:"ttl,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type holderRequestDTO struct {
	Holder    string `json:"holder"`
	RequestID string `json:"request_id,omitempty"`
}

type reclaimRequestDTO struct {
	RequestID string `json:"request_id,omitempty"`
}

func (h *Handler) apply(w http.ResponseWriter, r *http.Request) {
	var in applyRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, environment.NewError(environment.ErrCodeInvalidRequest,
			"invalid JSON body: "+err.Error()))
		return
	}
	req := environment.ApplyRequest{
		Name:      in.Name,
		Target:    in.Target,
		Holder:    in.Holder,
		RequestID: orID(in.RequestID, r),
	}
	if in.TTL != "" {
		ttl, err := time.ParseDuration(in.TTL)
		if err != nil {
			writeError(w, http.StatusBadRequest, environment.NewError(environment.ErrCodeInvalidRequest,
				"invalid ttl (e.g. 30s, 5m): "+err.Error()))
			return
		}
		req.TTL = ttl
	}
	env, err := h.c.Apply(r.Context(), req)
	if err != nil {
		writeControllerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envResponse{Environment: toDTO(env)})
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, listResponse{Environments: toDTOs(h.c.Snapshot())})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	env, err := h.c.Get(r.PathValue("name"))
	if err != nil {
		writeControllerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envResponse{Environment: toDTO(env)})
}

func (h *Handler) occupy(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeHolder(w, r)
	if !ok {
		return
	}
	env, err := h.c.Occupy(r.Context(), r.PathValue("name"), in.Holder, orID(in.RequestID, r))
	if err != nil {
		writeControllerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envResponse{Environment: toDTO(env)})
}

func (h *Handler) release(w http.ResponseWriter, r *http.Request) {
	in, ok := decodeHolder(w, r)
	if !ok {
		return
	}
	env, err := h.c.Release(r.Context(), r.PathValue("name"), in.Holder, orID(in.RequestID, r))
	if err != nil {
		writeControllerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envResponse{Environment: toDTO(env)})
}

func (h *Handler) releaseAll(w http.ResponseWriter, r *http.Request) {
	in, _ := decodeBody[reclaimRequestDTO](w, r)
	env, err := h.c.ReleaseAll(r.Context(), r.PathValue("name"), orID(in.RequestID, r))
	if err != nil {
		writeControllerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envResponse{Environment: toDTO(env)})
}

func (h *Handler) reclaim(w http.ResponseWriter, r *http.Request) {
	in, _ := decodeBody[reclaimRequestDTO](w, r)
	env, err := h.c.Reclaim(r.Context(), r.PathValue("name"), orID(in.RequestID, r))
	if err != nil {
		writeControllerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, envResponse{Environment: toDTO(env)})
}

func (h *Handler) reclaimExpired(w http.ResponseWriter, r *http.Request) {
	results := h.c.ReclaimExpired(r.Context())
	writeJSON(w, http.StatusOK, reclaimResponse{Results: results})
}

// ---- DTOs ---------------------------------------------------------------

type envDTO struct {
	Name         string                       `json:"name"`
	Target       string                       `json:"target"`
	Status       environment.Status           `json:"status"`
	Port         int                          `json:"port"`
	PID          int                          `json:"pid"`
	Holders      []string                     `json:"holders"`
	Endpoint     string                       `json:"endpoint"`
	TTL          string                       `json:"ttl"`
	ExpiresAt    time.Time                    `json:"expires_at"`
	ReadyAt      time.Time                    `json:"ready_at"`
	CreatedAt    time.Time                    `json:"created_at"`
	UpdatedAt    time.Time                    `json:"updated_at"`
	ClaimVersion int64                        `json:"claim_version"`
	ReclaimErrs  []environment.ReclaimFailure `json:"reclaim_errors,omitempty"`
	Events       []eventDTO                   `json:"events"`
}

type eventDTO struct {
	Time      time.Time          `json:"time"`
	From      environment.Status `json:"from"`
	To        environment.Status `json:"to"`
	Reason    string             `json:"reason"`
	Detail    string             `json:"detail"`
	RequestID string             `json:"request_id"`
}

type envResponse struct {
	Environment envDTO `json:"environment"`
}

type listResponse struct {
	Environments []envDTO `json:"environments"`
}

type reclaimResponse struct {
	Results []controller.ReclaimResult `json:"results"`
}

type errorDTO struct {
	Code      environment.ErrorCode `json:"code"`
	Message   string                `json:"message"`
	Target    string                `json:"target,omitempty"`
	RequestID string                `json:"request_id,omitempty"`
}

func toDTO(e *environment.Environment) envDTO {
	d := envDTO{
		Name:         e.Name,
		Target:       e.Target,
		Status:       e.Status,
		Port:         e.Port,
		PID:          e.PID,
		Holders:      append([]string(nil), e.Holders...),
		Endpoint:     e.Endpoint,
		TTL:          e.TTL.String(),
		ExpiresAt:    e.ExpiresAt,
		ReadyAt:      e.ReadyAt,
		CreatedAt:    e.CreatedAt,
		UpdatedAt:    e.UpdatedAt,
		ClaimVersion: e.ClaimVersion,
		ReclaimErrs:  append([]environment.ReclaimFailure(nil), e.ReclaimErrs...),
	}
	for _, ev := range e.Events {
		d.Events = append(d.Events, eventDTO{
			Time: ev.Time, From: ev.From, To: ev.To,
			Reason: ev.Reason, Detail: ev.Detail, RequestID: ev.RequestID,
		})
	}
	return d
}

func toDTOs(list []*environment.Environment) []envDTO {
	out := make([]envDTO, 0, len(list))
	for _, e := range list {
		out = append(out, toDTO(e))
	}
	return out
}

// ---- helpers ------------------------------------------------------------

func decodeBody[T any](w http.ResponseWriter, r *http.Request) (T, bool) {
	var in T
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeError(w, http.StatusBadRequest, environment.NewError(environment.ErrCodeInvalidRequest,
			"invalid JSON body: "+err.Error()))
		return in, false
	}
	return in, true
}

func decodeHolder(w http.ResponseWriter, r *http.Request) (holderRequestDTO, bool) {
	in, ok := decodeBody[holderRequestDTO](w, r)
	if ok && strings.TrimSpace(in.Holder) == "" {
		writeError(w, http.StatusBadRequest, environment.NewError(environment.ErrCodeInvalidRequest,
			"holder is required"))
		return in, false
	}
	return in, ok
}

func orID(id string, r *http.Request) string {
	if id != "" {
		return id
	}
	return r.Header.Get("X-Request-Id")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, e *environment.Error) {
	writeJSON(w, status, errorDTO{
		Code: e.Code, Message: e.Message, Target: e.Target, RequestID: e.RequestID,
	})
}

// statusFor maps controller error codes to HTTP status codes.
func statusFor(code environment.ErrorCode) int {
	switch code {
	case environment.ErrCodeInvalidRequest:
		return http.StatusBadRequest
	case environment.ErrCodeNotFound:
		return http.StatusNotFound
	case environment.ErrCodeNameConflict:
		return http.StatusConflict
	case environment.ErrCodeQuotaExhausted, environment.ErrCodePortsExhausted:
		return http.StatusServiceUnavailable
	case environment.ErrCodeInUse:
		return http.StatusConflict
	case environment.ErrCodeNotReady:
		return http.StatusConflict
	case environment.ErrCodeNotReclaimable:
		return http.StatusConflict
	case environment.ErrCodeProvisioner:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func writeControllerError(w http.ResponseWriter, err error) {
	var ce *environment.Error
	if errors.As(err, &ce) {
		writeError(w, statusFor(ce.Code), ce)
		return
	}
	writeError(w, http.StatusInternalServerError,
		environment.NewError(environment.ErrCodeProvisioner, err.Error()))
}

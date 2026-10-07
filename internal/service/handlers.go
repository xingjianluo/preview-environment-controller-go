package service

import (
	"net/http"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

// POST /v1/environments/acquire
func (s *Server) handleAcquire(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	var req acquireRequest
	if err := decodeBody(r, &req); err != nil {
		s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: err.Error()})
		return
	}
	if req.Name == "" || req.Holder == "" {
		s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: "name 与 holder 必填"})
		return
	}
	res, err := s.ctrl.Acquire(reqID, req.Name, req.Holder)
	if err != nil {
		s.writeError(w, reqID, err)
		return
	}
	s.writeJSON(w, http.StatusOK, reqID, acquireView{
		Env: toEnvView(res.Env), Created: res.Created, Rationale: res.Rationale,
	})
}

// POST /v1/environments/release
func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	var req releaseRequest
	if err := decodeBody(r, &req); err != nil {
		s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: err.Error()})
		return
	}
	if req.Name == "" || req.Holder == "" {
		s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: "name 与 holder 必填"})
		return
	}
	if err := s.ctrl.Release(reqID, req.Name, req.Holder); err != nil {
		s.writeError(w, reqID, err)
		return
	}
	s.writeJSON(w, http.StatusOK, reqID, map[string]string{
		"name": req.Name, "holder": req.Holder, "result": "released",
	})
}

// POST /v1/environments/renew
func (s *Server) handleRenew(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	var req renewRequest
	if err := decodeBody(r, &req); err != nil {
		s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: err.Error()})
		return
	}
	if req.Name == "" || req.Holder == "" {
		s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: "name 与 holder 必填"})
		return
	}
	var extend time.Duration
	if req.Extend != "" {
		d, err := time.ParseDuration(req.Extend)
		if err != nil || d < 0 {
			s.writeError(w, reqID, &preview.Error{Code: "BAD_REQUEST", Msg: "extend 必须是合法非负时长（如 30s）"})
			return
		}
		extend = d
	}
	res, err := s.ctrl.Renew(reqID, req.Name, req.Holder, extend)
	if err != nil {
		s.writeError(w, reqID, err)
		return
	}
	s.writeJSON(w, http.StatusOK, reqID, renewView{
		Env: toEnvView(res.Env), OldExpires: res.OldExpires,
		NewExpires: res.NewExpires, Rationale: res.Rationale,
	})
}

// POST /v1/reclaim —— 无论有无失败都返回 200，失败明细在 data.failures 中；
// 存在回收失败时 succeeded=false，调用方可安全重试。
func (s *Server) handleReclaim(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	report := s.ctrl.ReclaimIdle(reqID)
	view := reclaimView{Succeeded: report.Succeeded()}
	view.Reclaimed = append(view.Reclaimed, report.Reclaimed...)
	for _, sk := range report.Skipped {
		view.Skipped = append(view.Skipped, skipped{Name: sk.Name, Reason: sk.Reason})
	}
	for _, f := range report.Failures {
		view.Failures = append(view.Failures, failure{Name: f.Name, Err: f.Err})
	}
	status := http.StatusOK
	if report.HasFailures() {
		// 让入口（监控/CI）直接能从状态码看出“本轮没全成”，但仍给出可重试的明细。
		status = http.StatusBadGateway
	}
	s.writeJSON(w, status, reqID, view)
}

// GET /v1/environments/{name}
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	name := r.PathValue("name")
	env, ok := s.ctrl.Get(name)
	if !ok {
		s.writeError(w, reqID, &preview.Error{Code: preview.ErrNotFound, Msg: "环境不存在"})
		return
	}
	s.writeJSON(w, http.StatusOK, reqID, toEnvView(env))
}

// GET /v1/environments
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	envs := s.ctrl.List()
	views := make([]envView, 0, len(envs))
	for _, e := range envs {
		views = append(views, toEnvView(e))
	}
	s.writeJSON(w, http.StatusOK, reqID, views)
}

// POST /v1/shutdown —— 幂等：首次返回 202 并在后台收尾；重复触发返回当前状态。
func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	reqID := requestID(r)
	if s.ShutdownTriggered() {
		s.writeJSON(w, http.StatusOK, reqID, map[string]any{
			"shutdown": "already-in-progress", "triggered": true,
		})
		return
	}
	go func() { _ = s.Shutdown(nil) }()
	s.writeJSON(w, http.StatusAccepted, reqID, map[string]any{
		"shutdown": "triggered", "triggered": true,
	})
}

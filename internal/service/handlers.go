package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

// envJSON 是环境快照的对外表示。
type envJSON struct {
	Name        string            `json:"name"`
	Port        int               `json:"port"`
	State       string            `json:"state"`
	Holders     []string          `json:"holders"`
	CreatedAt   time.Time         `json:"createdAt"`
	ExpiresAt   time.Time         `json:"expiresAt"`
	LeaseExpiry map[string]string `json:"leaseExpiry"`
	IdleAt      *time.Time        `json:"idleAt,omitempty"`
	InUse       bool              `json:"inUse"`
	ReclaimErr  string            `json:"reclaimErr,omitempty"`
}

func toEnvJSON(e *preview.Environment) envJSON {
	leases := map[string]string{}
	for h, t := range e.LeaseExpiry {
		leases[h] = t.UTC().Format(time.RFC3339Nano)
	}
	j := envJSON{
		Name:        e.Name,
		Port:        e.Port,
		State:       string(e.State),
		Holders:     append([]string(nil), e.Holders...),
		CreatedAt:   e.CreatedAt.UTC(),
		ExpiresAt:   e.ExpiresAt.UTC(),
		LeaseExpiry: leases,
		InUse:       e.InUse(),
		ReclaimErr:  e.ReclaimErr,
	}
	if !e.IdleAt.IsZero() {
		idle := e.IdleAt.UTC()
		j.IdleAt = &idle
	}
	return j
}

type acquireReq struct {
	Name   string `json:"name"`
	Holder string `json:"holder"`
}

type acquireResp struct {
	RequestID string  `json:"requestId"`
	Created   bool    `json:"created"`
	Rationale string  `json:"rationale"`
	Env       envJSON `json:"env"`
}

type simpleResp struct {
	RequestID string  `json:"requestId"`
	OK        bool    `json:"ok"`
	Env       envJSON `json:"env"`
}

// duration 支持 JSON 里写 "30s" 这样的时长字符串，也接受纳秒整数。
type duration struct {
	time.Duration
}

func (d *duration) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		parsed, err := time.ParseDuration(string(b[1 : len(b)-1]))
		if err != nil {
			return err
		}
		d.Duration = parsed
		return nil
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return err
	}
	d.Duration = time.Duration(n)
	return nil
}

type errorResp struct {
	RequestID string `json:"requestId"`
	Code      string `json:"code"`
	Error     string `json:"error"`
}

// registerRoutes 注册全部入口路由。
func (s *Server) registerRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/envs", s.handleEnvs)       // POST 申请 / GET 列表
	mux.HandleFunc("/v1/envs/", s.handleEnv)       // GET 查询 / POST 续期 / DELETE 释放或销毁
	mux.HandleFunc("/v1/reclaim", s.handleReclaim) // POST 空闲回收
	return mux
}

// POST /v1/envs {"name","holder"}  申请（自动收敛并发同名申请）
// GET  /v1/envs                    列出全部环境
func (s *Server) handleEnvs(w http.ResponseWriter, r *http.Request) {
	reqID := s.requestID(r)
	switch r.Method {
	case http.MethodPost:
		// 停机期间不再接受新申请，给出明确稳定的拒绝。
		if !s.gateNew(reqID, w, "acquire") {
			return
		}
		defer s.leaveMutating()
		var in acquireReq
		if !decodeBody(w, r, reqID, &in) {
			return
		}
		if in.Name == "" || in.Holder == "" {
			writePreviewError(w, reqID, http.StatusBadRequest,
				preview.ErrBadRequest, "name 与 holder 均不能为空")
			return
		}
		res, err := s.ctrl.Acquire(reqID, in.Name, in.Holder)
		if err != nil {
			writeCtrlError(w, reqID, err)
			return
		}
		writeJSON(w, http.StatusOK, acquireResp{
			RequestID: reqID, Created: res.Created, Rationale: res.Rationale, Env: toEnvJSON(res.Env),
		})
	case http.MethodGet:
		var list []envJSON
		for _, e := range s.ctrl.List() {
			list = append(list, toEnvJSON(e))
		}
		if list == nil {
			list = []envJSON{}
		}
		writeJSON(w, http.StatusOK, map[string]any{"requestId": reqID, "envs": list})
	default:
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// GET    /v1/envs/{name}          查询
// POST   /v1/envs/{name}/renew    续期  {"holder","extend"}
// DELETE /v1/envs/{name}?holder=x 释放（带 holder）；不带 holder 为空闲销毁
func (s *Server) handleEnv(w http.ResponseWriter, r *http.Request) {
	reqID := s.requestID(r)
	path := strings.TrimPrefix(r.URL.Path, "/v1/envs/")
	parts := strings.Split(path, "/")
	name := parts[0]
	if name == "" {
		http.Error(w, "env name required", http.StatusBadRequest)
		return
	}

	switch {
	case len(parts) == 2 && parts[1] == "renew" && r.Method == http.MethodPost:
		if !s.gateNew(reqID, w, "renew") {
			return
		}
		defer s.leaveMutating()
		var in struct {
			Holder string   `json:"holder"`
			Extend duration `json:"extend"`
		}
		if !decodeBody(w, r, reqID, &in) {
			return
		}
		if in.Holder == "" || in.Extend.Duration <= 0 {
			writePreviewError(w, reqID, http.StatusBadRequest,
				preview.ErrBadRequest, "holder 不能为空且 extend 必须为正")
			return
		}
		env, err := s.ctrl.Renew(reqID, name, in.Holder, in.Extend.Duration)
		if err != nil {
			writeCtrlError(w, reqID, err)
			return
		}
		writeJSON(w, http.StatusOK, simpleResp{RequestID: reqID, OK: true, Env: toEnvJSON(env)})

	case len(parts) == 1 && r.Method == http.MethodGet:
		env, ok := s.ctrl.Get(name)
		if !ok {
			writePreviewError(w, reqID, http.StatusNotFound, preview.ErrNotFound,
				fmt.Sprintf("环境 %q 不存在或已回收", name))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"requestId": reqID, "env": toEnvJSON(env)})

	case len(parts) == 1 && r.Method == http.MethodDelete:
		s.trackRelease()
		defer s.leaveMutating()
		holder := r.URL.Query().Get("holder")
		if holder != "" {
			if err := s.ctrl.Release(reqID, name, holder); err != nil {
				writeCtrlError(w, reqID, err)
				return
			}
		} else {
			if err := s.ctrl.Destroy(reqID, name); err != nil {
				writeCtrlError(w, reqID, err)
				return
			}
		}
		env, _ := s.ctrl.Get(name)
		resp := simpleResp{RequestID: reqID, OK: true}
		if env != nil {
			resp.Env = toEnvJSON(env)
		}
		writeJSON(w, http.StatusOK, resp)

	default:
		w.Header().Set("Allow", "GET, DELETE, POST(renew)")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

type reclaimResp struct {
	RequestID string                `json:"requestId"`
	Reclaimed []string              `json:"reclaimed"`
	Failures  []preview.ReclaimItem `json:"failures"`
	Skipped   []preview.ReclaimItem `json:"skipped"`
}

// POST /v1/reclaim 按空闲时间扫描回收。回收允许在停机期间调用（收尾用）。
func (s *Server) handleReclaim(w http.ResponseWriter, r *http.Request) {
	reqID := s.requestID(r)
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	rep, err := s.ctrl.Reclaim(reqID)
	if err != nil {
		writeCtrlError(w, reqID, err)
		return
	}
	out := reclaimResp{RequestID: reqID}
	out.Reclaimed = rep.Reclaimed
	if out.Reclaimed == nil {
		out.Reclaimed = []string{}
	}
	out.Failures = rep.Failures
	if out.Failures == nil {
		out.Failures = []preview.ReclaimItem{}
	}
	out.Skipped = rep.Skipped
	if out.Skipped == nil {
		out.Skipped = []preview.ReclaimItem{}
	}
	// 有回收失败时明确给出 207，绝不对外报成功。
	status := http.StatusOK
	if rep.HasFailure() {
		status = http.StatusMultiStatus
	}
	writeJSON(w, status, out)
}

// writeCtrlError 把带原因码的控制器错误映射为稳定的 HTTP 状态码。
func writeCtrlError(w http.ResponseWriter, reqID string, err error) {
	var pe *preview.Error
	if errors.As(err, &pe) {
		status := http.StatusInternalServerError
		switch pe.Code {
		case preview.ErrNotFound:
			status = http.StatusNotFound
		case preview.ErrNameConflict:
			status = http.StatusConflict
		case preview.ErrEnvInUse:
			status = http.StatusConflict
		case preview.ErrNotHeld, preview.ErrLeaseExpired:
			status = http.StatusForbidden
		case preview.ErrQuotaExceeded, preview.ErrPortExhausted:
			status = http.StatusServiceUnavailable
		case preview.ErrBackend:
			status = http.StatusBadGateway
		case preview.ErrServiceDraining:
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, errorResp{RequestID: reqID, Code: string(pe.Code), Error: pe.Msg})
		return
	}
	writeJSON(w, http.StatusInternalServerError, errorResp{RequestID: reqID, Code: "INTERNAL", Error: err.Error()})
}

func writePreviewError(w http.ResponseWriter, reqID string, status int, code preview.ErrCode, msg string) {
	writeJSON(w, status, errorResp{RequestID: reqID, Code: string(code), Error: msg})
}

func decodeBody(w http.ResponseWriter, r *http.Request, reqID string, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResp{RequestID: reqID, Code: "BAD_REQUEST", Error: err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

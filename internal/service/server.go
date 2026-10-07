// Package service 把预览环境编排器包装成可长期运行的本地 HTTP 服务，
// 对外提供申请、释放、续期、回收与查询能力，并支持优雅停机收尾。
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

// Server 是常驻服务入口。所有 HTTP 入口最终都落到同一个 *preview.Controller，
// 因此无论请求从哪个入口进来，同一时刻对同一操作的结论与失败原因完全一致。
type Server struct {
	ctrl         *preview.Controller
	addr         string
	drainTimeout time.Duration
	logger       *log.Logger

	srv          *http.Server
	listener     net.Listener
	listenerAddr atomic.Pointer[string]

	shutdownOnce sync.Once
	shutdownCh   chan struct{} // 首次触发停机时关闭
	doneCh       chan struct{} // 本轮停机收尾全部完成时关闭
	shutdownErr  error
}

// Config 是服务配置。
type Config struct {
	Addr string // 监听地址，默认 ":18080"

	// DrainTimeout 是优雅停机的最长等待；超时后强制停止残留实例。
	DrainTimeout time.Duration

	// Logger 可选；为空时用标准 logger。
	Logger *log.Logger
}

// New 创建服务。
func New(ctrl *preview.Controller, cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":18080"
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{
		ctrl:         ctrl,
		addr:         cfg.Addr,
		drainTimeout: cfg.DrainTimeout,
		logger:       logger,
		shutdownCh:   make(chan struct{}),
		doneCh:       make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/environments/acquire", s.handleAcquire)
	mux.HandleFunc("POST /v1/environments/release", s.handleRelease)
	mux.HandleFunc("POST /v1/environments/renew", s.handleRenew)
	mux.HandleFunc("POST /v1/reclaim", s.handleReclaim)
	mux.HandleFunc("GET /v1/environments", s.handleList)
	mux.HandleFunc("GET /v1/environments/{name}", s.handleGet)
	mux.HandleFunc("POST /v1/shutdown", s.handleShutdown)

	s.srv = &http.Server{
		Handler:           s.withRequestID(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

// Start 建立监听并阻塞服务，直到 Shutdown 让其优雅停止或监听失败。
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", s.addr, err)
	}
	s.listener = ln
	addr := ln.Addr().String()
	s.listenerAddr.Store(&addr)
	s.logger.Printf("[service] 预览环境服务已启动，监听 %s", addr)

	err = s.srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown 触发停机收尾并等待完成：
//  1. 控制器立即停止接受新申请（进行中请求照常完成）；
//  2. 等待占用者离开并回收空闲实例；超时后强制停止残留实例，不留后台进程；
//  3. 停止 HTTP 监听。
//
// 可被信号处理器和 /v1/shutdown 入口重复触发：所有调用等待同一轮收尾，结论一致。
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		close(s.shutdownCh)
		reqID := "shutdown-" + randID()
		s.logger.Printf("[req=%s] 触发停机收尾", reqID)

		drainCtx, cancel := context.WithTimeout(context.Background(), s.drainTimeout)
		defer cancel()
		report, derr := s.ctrl.Drain(drainCtx, reqID)
		if derr != nil {
			s.shutdownErr = derr
			s.logger.Printf("[req=%s] 停机收尾存在残留失败: %v failures=%d", reqID, derr, len(report.Failures))
		} else {
			s.logger.Printf("[req=%s] 停机收尾完成 reclaimed=%v failures=%d", reqID, report.Reclaimed, len(report.Failures))
		}
		// 控制器收尾后再停 HTTP：期间进来的请求仍能拿到稳定响应（申请被明确拒绝）。
		if err := s.srv.Shutdown(context.Background()); err != nil {
			s.logger.Printf("[req=%s] HTTP 层停止异常: %v", reqID, err)
			if s.shutdownErr == nil {
				s.shutdownErr = err
			}
		}
		close(s.doneCh)
	})

	if ctx == nil {
		<-s.shutdownCh
		// shutdownOnce 内的收尾可能尚未结束；通过 nil ctx 时退化为不等待完整收尾。
		return s.shutdownErr
	}
	// 等待本轮收尾真正完成（用一次 Do 内的完成信号近似：见 waitDone）。
	s.waitDone(ctx)
	return s.shutdownErr
}

// done 在收尾全部完成后关闭；供重复调用方等待同一轮收尾。
func (s *Server) waitDone(ctx context.Context) {
	if ctx == nil {
		return
	}
	// shutdownOnce.Do 的函数返回即代表收尾完成，这里用一个轻量完成通道保证可等待。
	select {
	case <-s.doneCh:
	case <-ctx.Done():
	}
}

// ShutdownTriggered 报告是否已触发停机。
func (s *Server) ShutdownTriggered() bool {
	select {
	case <-s.shutdownCh:
		return true
	default:
		return false
	}
}

// ListenAddr 返回实际监听地址（:0 随机端口场景下用于测试）。
func (s *Server) ListenAddr() string {
	if p := s.listenerAddr.Load(); p != nil {
		return *p
	}
	return ""
}

// withRequestID 为每个请求注入/透传申请标识，并记录请求边界日志。
func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			reqID = randID()
		}
		ctx := context.WithValue(r.Context(), reqIDKey{}, reqID)
		w.Header().Set("X-Request-ID", reqID)
		s.logger.Printf("[req=%s] %s %s 进入", reqID, r.Method, r.URL.Path)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type reqIDKey struct{}

func requestID(r *http.Request) string {
	if v, ok := r.Context().Value(reqIDKey{}).(string); ok {
		return v
	}
	return "unknown"
}

func randID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// writeJSON 写出统一成功响应。
func (s *Server) writeJSON(w http.ResponseWriter, status int, reqID string, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response{RequestID: reqID, Data: data})
}

// writeError 把 *preview.Error 映射为稳定的 HTTP 状态码与错误体。
func (s *Server) writeError(w http.ResponseWriter, reqID string, err error) {
	var pe *preview.Error
	code := "INTERNAL"
	msg := err.Error()
	status := http.StatusInternalServerError
	if errors.As(err, &pe) {
		code = string(pe.Code)
		msg = pe.Msg
		status = statusForCode(pe.Code)
	}
	s.logger.Printf("[req=%s] 拒绝/失败 code=%s http=%d msg=%s", reqID, code, status, msg)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(response{RequestID: reqID, Error: &errBody{Code: code, Message: msg}})
}

func statusForCode(code preview.ErrCode) int {
	switch code {
	case preview.ErrNotFound:
		return http.StatusNotFound
	case preview.ErrNotHeld, preview.ErrLeaseExpired:
		return http.StatusConflict
	case preview.ErrNameConflict, preview.ErrEnvInUse:
		return http.StatusConflict
	case preview.ErrQuotaExceeded, preview.ErrPortExhausted:
		return http.StatusServiceUnavailable
	case preview.ErrShuttingDown:
		// 停机期间的新申请：稳定的 503 + 明确原因码，而非断连或半响应。
		return http.StatusServiceUnavailable
	case preview.ErrBackend:
		return http.StatusBadGateway
	default:
		return http.StatusInternalServerError
	}
}

func decodeBody(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("请求体不是合法 JSON: %w", err)
	}
	return nil
}

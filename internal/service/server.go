// Package service 把预览环境编排能力包装成可长期运行的本地 HTTP 服务，
// 对外提供申请、释放、回收、查询与租约续期入口，并支持优雅停机收尾。
package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

// Server 是常驻 HTTP 服务。多个使用方并发调用时，结论由底层控制器的
// 互斥锁收敛；停机期间通过 draining 闸门给出稳定一致的拒绝结果。
type Server struct {
	ctrl    *preview.Controller
	addr    string
	logger  *log.Logger
	handler http.Handler

	shutdownGrace time.Duration // 等待在用环境变空闲的最长时间

	mu         sync.Mutex
	draining   bool           // 是否进入停机
	inflight   sync.WaitGroup // 在途变更请求
	httpServer *http.Server
	listener   net.Listener
	shutdownMu sync.Mutex
	shutDone   bool
	shutReport *preview.StopReport
	shutErr    error
}

// NewServer 创建常驻服务。addr 为监听地址（如 "127.0.0.1:18080"），
// grace 为停机时等待占用者释放的最长时间，超时后强制停止残留实例。
func NewServer(ctrl *preview.Controller, addr string, grace time.Duration, logger *log.Logger) *Server {
	if logger == nil {
		logger = log.Default()
	}
	s := &Server{ctrl: ctrl, addr: addr, logger: logger, shutdownGrace: grace}
	s.handler = s.registerRoutes()
	return s
}

// Start 在后台开始监听。返回后服务即可接受请求。
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.httpServer != nil {
		return errors.New("服务已启动")
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.listener = ln
	s.httpServer = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := s.httpServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Printf("[req=server] Serve 异常退出: %v", err)
		}
	}()
	s.addr = ln.Addr().String()
	s.logger.Printf("[req=server] 预览环境服务已启动，监听 %s", s.addr)
	return nil
}

// Addr 返回实际监听地址。
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// gateNew 用于申请/续期：停机期间一律拒绝，返回 false 表示已写回稳定的
// 503 响应。释放不在此列——停机等待期内仍允许占用者主动释放，以便环境
// 尽快变空闲、被干净收尾。
func (s *Server) gateNew(reqID string, w http.ResponseWriter, op string) bool {
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		s.logger.Printf("[req=%s] %s -> 拒绝 code=%s 依据: 服务停机收尾中，不接受新申请/续期",
			reqID, op, preview.ErrServiceDraining)
		writeJSON(w, http.StatusServiceUnavailable, errorResp{
			RequestID: reqID,
			Code:      string(preview.ErrServiceDraining),
			Error:     "服务正在停机收尾，不再接受 " + op + " 请求",
		})
		return false
	}
	s.inflight.Add(1)
	s.mu.Unlock()
	return true
}

// trackRelease 登记释放类请求（释放/空闲销毁）。停机期间仍允许释放，
// 但在途请求会被 Shutdown 等待结束，不会只做一半。
func (s *Server) trackRelease() { s.inflight.Add(1) }

func (s *Server) leaveMutating() { s.inflight.Done() }

// Shutdown 执行优雅停机，可被重复触发：
//  1. 置 draining，此后保持监听但拒绝新的申请/续期/释放/销毁（稳定 503）；
//  2. 等待已在途的变更请求结束；
//  3. 轮询等待仍在用的环境变空闲（至多 shutdownGrace）；
//  4. StopAll 停止全部自拉起实例（含仍在用者），不留后台进程，失败有限次重试；
//  5. 收尾完成后才关闭 HTTP 监听，退出时不存在半开服务或残留实例。
//
// 重复调用直接返回第一次的收尾结论，不会二次执行。
func (s *Server) Shutdown(ctx context.Context) (*preview.StopReport, error) {
	s.shutdownMu.Lock()
	if s.shutDone {
		rep, err := s.shutReport, s.shutErr
		s.shutdownMu.Unlock()
		return rep, err
	}
	s.shutdownMu.Unlock()

	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.logger.Printf("[req=shutdown] 进入停机收尾：监听保持，但不再接受新变更请求")

	// 先等在途变更请求结束；此时监听仍在，新的变更请求会被闸门稳定拒绝为 503，
	// 而不是遇到连接被拒或静默断开。
	s.inflight.Wait()
	s.logger.Printf("[req=shutdown] 在途变更请求已全部结束")

	s.waitBusyIdle(ctx)

	rep := s.ctrl.StopAll("shutdown")
	for attempt := 1; attempt < 3 && rep.HasFailures(); attempt++ {
		s.logger.Printf("[req=shutdown] %d 个实例停止失败，%d 秒后安全重试",
			len(rep.Failures), attempt)
		select {
		case <-time.After(time.Duration(attempt) * time.Second):
		case <-ctx.Done():
			s.closeHTTP(ctx)
			s.recordShutdown(rep, ctx.Err())
			return rep, ctx.Err()
		}
		rep = s.ctrl.StopAll("shutdown-retry-" + time.Duration(attempt).String())
	}

	// 实例全部收尾后再停止接受 HTTP 请求，保证停机过程结论稳定、退出不留半成品。
	if err := s.closeHTTP(ctx); err != nil {
		s.recordShutdown(rep, err)
		return rep, err
	}
	if rep.HasFailures() {
		err := errors.New("停机收尾后仍有实例未能停止，详见报告 Failures")
		s.recordShutdown(rep, err)
		return rep, err
	}
	s.recordShutdown(rep, nil)
	s.logger.Printf("[req=shutdown] 收尾完成 stopped=%v，监听已关闭，无后台残留实例", rep.Stopped)
	return rep, nil
}

// closeHTTP 幂等关闭 HTTP 监听，等待在途连接处理完。
func (s *Server) closeHTTP(ctx context.Context) error {
	s.mu.Lock()
	srv := s.httpServer
	s.httpServer = nil
	s.mu.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// waitBusyIdle 轮询等待在用环境全部释放，使停机尽量只回收"已无人用"的实例；
// 超过 grace 或 ctx 取消则返回，交由 StopAll 强制停止。
func (s *Server) waitBusyIdle(ctx context.Context) {
	deadline := time.Now().Add(s.shutdownGrace)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		busy := 0
		for _, e := range s.ctrl.List() {
			if e.InUse() {
				busy++
			}
		}
		if busy == 0 {
			s.logger.Printf("[req=shutdown] 全部环境已空闲，开始停止实例")
			return
		}
		s.logger.Printf("[req=shutdown] 等待 %d 个在用环境释放…", busy)
		if time.Now().After(deadline) {
			s.logger.Printf("[req=shutdown] 等待宽限期 %s 结束，强制停止剩余在用实例", s.shutdownGrace)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) recordShutdown(rep *preview.StopReport, err error) {
	s.shutdownMu.Lock()
	s.shutDone = true
	s.shutReport = rep
	s.shutErr = err
	s.shutdownMu.Unlock()
}

// requestID 取请求头 X-Request-Id；缺省时生成一个，保证日志可追踪。
func (s *Server) requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" {
		return id
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "auto-" + hex.EncodeToString(b[:])
}

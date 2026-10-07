// previewd 是预览环境编排器的常驻服务入口：
// 启动后长期对外提供申请、释放、续期、回收与查询 HTTP API；
// 收到 SIGINT/SIGTERM 或 POST /v1/shutdown 时优雅停机，
// 停收新申请、回收空闲实例、超时强制停止残留实例，不留后台进程。
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
	"github.com/xingjianluo/preview-environment-controller-go/internal/service"
)

func main() {
	var (
		addr         = flag.String("addr", ":18080", "HTTP 监听地址")
		maxEnvs      = flag.Int("max-envs", 8, "配额：最大并存环境数")
		portBase     = flag.Int("port-base", 19000, "端口池起始")
		portEnd      = flag.Int("port-end", 19031, "端口池结束")
		ttl          = flag.Duration("ttl", 30*time.Minute, "单个占用者租约默认有效期")
		idleTTL      = flag.Duration("idle-ttl", 10*time.Minute, "空闲回收阈值（最后一个占用者离开后多久可回收）")
		drainTimeout = flag.Duration("drain-timeout", 60*time.Second, "优雅停机最长等待，超时强制停止残留实例")
		reclaimEvery = flag.Duration("reclaim-every", 30*time.Second, "后台空闲回收扫描间隔")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "[previewd] ", log.LstdFlags|log.Lmicroseconds)
	ctrl := preview.New(preview.Config{
		MaxEnvs:  *maxEnvs,
		PortBase: *portBase,
		PortEnd:  *portEnd,
		TTL:      *ttl,
		IdleTTL:  *idleTTL,
	}, preview.NewProcessBackend(), logger)

	srv := service.New(ctrl, service.Config{Addr: *addr, DrainTimeout: *drainTimeout})

	// 后台周期性空闲回收：回收失败只在报告中体现，下一轮自动安全重试。
	stopReclaim := startReclaimLoop(ctrl, *reclaimEvery, logger)
	defer stopReclaim()

	// 信号触发的停机与 HTTP 触发的停机共用同一条幂等收尾路径。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Printf("收到信号 %v，开始停机收尾", sig)
		ctx, cancel := context.WithTimeout(context.Background(), *drainTimeout+5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Printf("停机收尾异常: %v", err)
		}
	}()

	if err := srv.Start(); err != nil {
		logger.Fatalf("服务退出: %v", err)
	}
	logger.Print("服务已干净退出，无残留后台实例")
}

func startReclaimLoop(ctrl *preview.Controller, every time.Duration, logger *log.Logger) func() {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				report := ctrl.ReclaimIdle("reclaim-loop")
				if len(report.Reclaimed) > 0 || report.HasFailures() {
					logger.Printf("周期回收: reclaimed=%v skipped=%d failures=%d",
						report.Reclaimed, len(report.Skipped), len(report.Failures))
				}
			}
		}
	}()
	return cancel
}

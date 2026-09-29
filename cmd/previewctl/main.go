// previewctl 是本地验证入口：用本地进程模拟环境实例，
// 演示申请、并发收敛、释放、过期回收与失败重试的完整生命周期。
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/xingjianluo/preview-environment-controller-go/internal/preview"
)

func main() {
	var (
		maxEnvs  = flag.Int("max-envs", 3, "配额：最大并存环境数")
		portBase = flag.Int("port-base", 18000, "端口池起始")
		portEnd  = flag.Int("port-end", 18002, "端口池结束")
		ttl      = flag.Duration("ttl", 30*time.Second, "环境过期时长")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "[previewctl] ", log.Lmicroseconds)
	c := preview.New(preview.Config{
		MaxEnvs:  *maxEnvs,
		PortBase: *portBase,
		PortEnd:  *portEnd,
		TTL:      *ttl,
	}, preview.NewProcessBackend(), logger)

	fmt.Println("== 1. 申请环境 demo-a（新建） ==")
	mustAcquire(c, "cli-1", "demo-a", "user-1")

	fmt.Println("== 2. 并发申请同一环境 demo-a（应收敛到同一实例） ==")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.Acquire(fmt.Sprintf("cli-conc-%d", i), "demo-a", fmt.Sprintf("user-%d", i+2))
			if err != nil {
				fmt.Println("  并发申请失败:", err)
				return
			}
			fmt.Printf("  并发申请结果: created=%v port=%d 依据=%s\n", r.Created, r.Env.Port, r.Rationale)
		}(i)
	}
	wg.Wait()

	fmt.Println("== 3. 销毁使用中的环境（应被拒绝 ENV_IN_USE） ==")
	printErr(c.Destroy("cli-2", "demo-a"))

	fmt.Println("== 4. 配额/端口边界：继续申请直到被拒 ==")
	for i := 0; ; i++ {
		_, err := c.Acquire(fmt.Sprintf("cli-fill-%d", i), fmt.Sprintf("fill-%d", i), "filler")
		if err != nil {
			printErr(err)
			break
		}
	}

	fmt.Println("== 5. 释放全部占用者后销毁 demo-a ==")
	for _, h := range []string{"user-1", "user-2", "user-3", "user-4", "user-5"} {
		if err := c.Release("cli-rel", "demo-a", h); err != nil {
			var pe *preview.Error
			if errors.As(err, &pe) && pe.Code == preview.ErrNotHeld {
				continue
			}
			printErr(err)
		}
	}
	printErr(c.Destroy("cli-3", "demo-a"))

	fmt.Println("== 6. 等待 TTL 过期后回收空闲环境 ==")
	time.Sleep(*ttl + 100*time.Millisecond)
	reclaimed, err := c.Reclaim("cli-reclaim")
	fmt.Printf("  回收结果: %v err=%v\n", reclaimed, err)

	fmt.Println("== 7. 回收后重新申请 demo-a（应为新建） ==")
	r := mustAcquire(c, "cli-4", "demo-a", "user-9")
	fmt.Printf("  状态变化: 回收后重新申请 -> created=%v state=%s port=%d\n", r.Created, r.Env.State, r.Env.Port)
}

func mustAcquire(c *preview.Controller, reqID, name, holder string) *preview.AcquireResult {
	r, err := c.Acquire(reqID, name, holder)
	if err != nil {
		fmt.Println("  申请失败:", err)
		os.Exit(1)
	}
	fmt.Printf("  申请结果: created=%v state=%s port=%d 依据=%s\n", r.Created, r.Env.State, r.Env.Port, r.Rationale)
	return r
}

func printErr(err error) {
	if err == nil {
		fmt.Println("  操作成功")
		return
	}
	var pe *preview.Error
	if errors.As(err, &pe) {
		fmt.Printf("  被拒绝: code=%s 原因=%s\n", pe.Code, pe.Msg)
		return
	}
	fmt.Println("  错误:", err)
}

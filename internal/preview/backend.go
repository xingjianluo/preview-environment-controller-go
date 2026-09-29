package preview

import (
	"errors"
	"fmt"
	"os/exec"
	"syscall"
)

// Handle 是后端实例的句柄，用于后续停止。
type Handle struct {
	PID     int
	cmd     *exec.Cmd
	stopped bool // 保证 Stop 幂等
}

// Backend 抽象环境实例的运行载体（本地进程或容器）。
type Backend interface {
	// Start 启动一个环境实例，返回句柄。
	Start(env *Environment) (*Handle, error)
	// Stop 停止实例。必须幂等：重复停止同一实例不报错。
	Stop(h *Handle) error
}

// ProcessBackend 用本地进程模拟环境实例。
type ProcessBackend struct {
	// Command 是要启动的命令，默认 "sleep"。
	Command string
	// Args 是命令参数，默认 {"3600"}。
	Args []string
}

// NewProcessBackend 返回默认的本地进程后端。
func NewProcessBackend() *ProcessBackend {
	return &ProcessBackend{Command: "sleep", Args: []string{"3600"}}
}

// Start 启动本地进程作为环境实例。
func (b *ProcessBackend) Start(env *Environment) (*Handle, error) {
	cmd := exec.Command(b.Command, b.Args...)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("启动环境 %s 的进程失败: %w", env.Name, err)
	}
	return &Handle{PID: cmd.Process.Pid, cmd: cmd}, nil
}

// Stop 停止本地进程。幂等：已停止的句柄直接返回 nil；
// 进程已退出（ESRCH / Wait 返回退出错误）也视为停止成功。
func (b *ProcessBackend) Stop(h *Handle) error {
	if h == nil || h.stopped {
		return nil
	}
	h.stopped = true
	if err := h.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			_ = h.cmd.Wait()
			return nil
		}
		return fmt.Errorf("停止进程 %d 失败: %w", h.PID, err)
	}
	// Wait 收割子进程；进程被信号杀死会返回退出错误，属预期。
	_ = h.cmd.Wait()
	return nil
}

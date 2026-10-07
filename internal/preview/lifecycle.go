package preview

import "sort"

// StopItem 描述停机收尾时单个实例的停止结果。
type StopItem struct {
	Name    string // 环境名
	Stopped bool   // 是否成功停止
	Err     string // 失败原因
}

// StopReport 是停机收尾报告：成功停止与仍然失败的实例都对外可见。
type StopReport struct {
	Stopped  []string
	Failures []StopItem
}

// HasFailures 报告是否存在未能停止的实例。
func (r *StopReport) HasFailures() bool { return r != nil && len(r.Failures) > 0 }

// StopAll 在停机收尾阶段停止控制器拉起的全部实例——无论该环境是否仍登记
// 有占用者，都不允许留下还在后台运行的进程。
//
// 幂等：已回收（句柄已清理）的实例直接跳过；重复调用不出错。后端 Stop
// 本身幂等，因此失败后再次调用 StopAll 可安全重试。失败实例记录在
// StopReport.Failures 中，绝不对外报成功。
func (c *Controller) StopAll(requestID string) *StopReport {
	c.mu.Lock()
	defer c.mu.Unlock()

	names := make([]string, 0, len(c.handles))
	for name := range c.handles {
		names = append(names, name)
	}
	sort.Strings(names) // 稳定顺序，便于日志与断言

	report := &StopReport{}
	for _, name := range names {
		h := c.handles[name]
		env := c.envs[name]
		prev := StateReady
		if env != nil {
			prev = env.State
		}
		if env != nil {
			env.State = StateReclaiming
		}
		c.logf(requestID, "stop-all name=%s -> state=%s->%s 依据: 停机收尾，强制停止全部自拉起实例",
			name, prev, StateReclaiming)
		if err := c.backend.Stop(h); err != nil {
			if env != nil {
				env.State = prev // 回滚，保持可再次收尾
				env.ReclaimErr = err.Error()
			}
			c.logf(requestID, "stop-all name=%s -> 失败(可重试) code=%s err=%v", name, ErrBackend, err)
			report.Failures = append(report.Failures, StopItem{Name: name, Err: err.Error()})
			continue
		}
		if env != nil {
			env.State = StateReclaimed
			env.ReclaimErr = ""
			delete(c.ports, env.Port)
		}
		delete(c.handles, name)
		report.Stopped = append(report.Stopped, name)
		c.logf(requestID, "stop-all name=%s -> state=%s->%s 依据: 已停止，不留后台实例",
			name, StateReclaiming, StateReclaimed)
	}
	c.logf(requestID, "stop-all 完成 stopped=%v failures=%d", report.Stopped, len(report.Failures))
	return report
}

package preview

import "time"

// Renew 由正在占用环境的 holder 给自己的租约续期 extend 时长。
//
// 续期只延长调用方自己那一份可用时间（LeaseExpiry[holder]），
// 环境整体到期时间随后按所有占用者租约的最大值重算，因此不影响别的
// 使用方。失败原因以 *Error 的 Code 互相区分：
//
//   - NOT_FOUND      环境不存在或已回收；
//   - NAME_CONFLICT  环境正处于回收中，无法续期；
//   - NOT_HELD       该调用方并没有占用这个环境；
//   - LEASE_EXPIRED  调用方自己的租约已经到期，错过可续期的时间点。
//
// 任何失败都不会改动环境状态（先判定、后写入）。
func (c *Controller) Renew(requestID, name, holder string, extend time.Duration) (*Environment, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	env, ok := c.envs[name]
	if !ok || env.State == StateReclaimed {
		c.logf(requestID, "renew name=%s holder=%s extend=%s -> 拒绝 code=%s",
			name, holder, extend, ErrNotFound)
		return nil, newError(ErrNotFound, "环境 %q 不存在或已回收，无法续期", name)
	}
	if env.State == StateReclaiming {
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s 依据: 环境回收中",
			name, holder, ErrNameConflict)
		return nil, newError(ErrNameConflict, "环境 %q 正在回收中，无法续期", name)
	}
	if !env.HasHolder(holder) {
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s holders=%v",
			name, holder, ErrNotHeld, env.Holders)
		return nil, newError(ErrNotHeld, "holder %q 并未占用环境 %q，无权续期", holder, name)
	}
	now := c.now()
	old := env.LeaseExpiry[holder]
	if !now.Before(old) {
		// 已到期：错过可续期的时间点。环境本身保持可用（可能仍有其他占用者），
		// 只是本调用方不能再续，需重新申请。
		c.logf(requestID, "renew name=%s holder=%s -> 拒绝 code=%s 依据: 租约已到期 now=%s leaseExpiry=%s",
			name, holder, ErrLeaseExpired, now.Format(time.RFC3339), old.Format(time.RFC3339))
		return nil, newError(ErrLeaseExpired,
			"holder %q 对环境 %q 的租约已于 %s 到期，无法续期，请重新申请", holder, name, old.Format(time.RFC3339))
	}

	newDeadline := old.Add(extend)
	env.LeaseExpiry[holder] = newDeadline
	c.recomputeExpiryLocked(env)
	c.logf(requestID, "renew name=%s holder=%s extend=%s -> 成功 leaseExpiry=%s->%s envExpiresAt=%s 依据: 仅延长本占用者租约，其他占用者不受影响",
		name, holder, extend, old.Format(time.RFC3339), newDeadline.Format(time.RFC3339),
		env.ExpiresAt.Format(time.RFC3339))
	return cloneEnv(env), nil
}

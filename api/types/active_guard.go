/*
 * Copyright 2023 The RuleGo Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package types

import (
	"context"
	"errors"
	"sync"
	"time"
)

// LeaseRenewer is an optional Locker capability that atomically extends the
// TTL of a held lock without releasing it, so the holder keeps the lease
// across renewals. Backends without renewal support fall back to
// release-and-reacquire, which opens a short window where another replica
// can take over.
// LeaseRenewer 是 Locker 的可选能力：原子顺延已持有锁的 TTL，续约期间持有权
// 不让渡。无续约能力的后端退化为释放后重取，窗口内可能被其他副本抢占。
type LeaseRenewer interface {
	// Renew extends the lock TTL. It returns false when the key is missing,
	// expired, or held with a different token; a backend failure returns an
	// error. Both cases mean the caller must treat the lease as lost.
	// Renew 顺延锁 TTL。键不存在、已过期或凭证不匹配时返回 false，
	// 后端故障返回错误；两者都表示租约已丢失。
	Renew(ctx context.Context, key, token string, expiration time.Duration) (bool, error)
}

// ErrLeaseRenewUnsupported is returned by wrappers whose inner Locker does
// not implement LeaseRenewer.
// ErrLeaseRenewUnsupported 在包装器内部的 Locker 未实现 LeaseRenewer 时返回。
var ErrLeaseRenewUnsupported = errors.New("rulego: locker does not support lease renewal")

// activeLockKeyPrefix is the lock key namespace of ActiveGuard.
// activeLockKeyPrefix 是 ActiveGuard 锁键的统一命名空间前缀。
const activeLockKeyPrefix = "rulego:active:"

// activeGuardDefaultTTL is the default lease duration. It bounds the failover
// delay: a dead leader's lease expires after at most one TTL, then a standby
// takes over on its next poll.
// activeGuardDefaultTTL 是默认租约时长，决定故障切换上限：宕机 leader 的租约
// 至多一个 TTL 后过期，待命副本在下个轮询周期接管。
const activeGuardDefaultTTL = 15 * time.Second

// ActiveGuard elects a single active holder of a shared resource — an
// endpoint subscription, a binlog reader — among replicas sharing the same
// Locker, keeping at-least-once delivery: only the leader consumes, so no
// message is dropped, unlike message-level deduplication.
//
// When Config.Locker is nil the guard is always active, so callers can keep
// it in place unconditionally: single process deployments are unaffected,
// replicated deployments elect a leader as soon as a Locker is injected.
//
// Unlike OnceGuard this makes no at-most-once promise; it only prevents
// concurrent duplicate consumption. Use it for broadcast sources without a
// natural message id (Redis Pub/Sub, AMQP per-connection queues, binlog);
// sources with single-delivery semantics — Kafka consumer groups, MQTT
// shared subscriptions — must not use it.
//
// ActiveGuard 在共享同一 Locker 的副本间为一个共享资源（端点订阅、binlog
// 读取器）选出唯一活跃持有者，保持 at-least-once：只有 leader 消费，不会丢
// 消息，这点与消息级去重不同。
//
// Config.Locker 为 nil 时恒为活跃态，调用方可无条件常驻：单机部署不受影响，
// 多副本部署注入 Locker 后自动选主。
//
// 与 OnceGuard 不同，本守卫不承诺至多一次，只消除并发重复消费。适用于没有
// 天然消息 ID 的广播源（Redis Pub/Sub、AMQP 每连接队列、binlog）；已具备
// 单投语义的源（Kafka 消费组、MQTT 共享订阅）不应使用。
type ActiveGuard struct {
	locker Locker
	logger Logger
	// key is the full lock key of the lease.
	// key 是租约的完整锁键。
	key      string
	ttl      time.Duration
	interval time.Duration

	mu     sync.Mutex
	active bool
	token  string
}

// ActiveGuardOption customizes an ActiveGuard.
// ActiveGuardOption 自定义选主行为。
type ActiveGuardOption func(*ActiveGuard)

// WithActiveTTL sets the lease duration. It bounds how long a dead leader
// stays elected; shorter TTLs fail over faster at the cost of more renewals.
// WithActiveTTL 设置租约时长，决定宕机 leader 的最长占用；TTL 越短切换越快，
// 续约开销越大。
func WithActiveTTL(ttl time.Duration) ActiveGuardOption {
	return func(g *ActiveGuard) {
		if ttl > 0 {
			g.ttl = ttl
			if g.interval <= 0 || g.interval > ttl/3 {
				g.interval = ttl / 3
			}
		}
	}
}

// WithActiveInterval sets the renew and takeover poll cadence. It defaults to
// a third of the TTL.
// WithActiveInterval 设置续约与接管轮询周期，默认为 TTL 的三分之一。
func WithActiveInterval(d time.Duration) ActiveGuardOption {
	return func(g *ActiveGuard) {
		if d > 0 {
			g.interval = d
		}
	}
}

// NewActiveGuard creates a guard bound to ruleConfig.Locker. scope isolates
// unrelated resources and typically names the component type, owner, chain id
// and a per-instance key, for example OnceScope(Type, owner, chainId, instanceKey).
// NewActiveGuard 创建绑定 ruleConfig.Locker 的选主守卫。scope 用于隔离互不相关
// 的资源，通常包含组件类型、所属者、链 ID 与实例标识，如
// OnceScope(Type, owner, chainId, instanceKey)。
func NewActiveGuard(ruleConfig Config, scope string, opts ...ActiveGuardOption) *ActiveGuard {
	g := &ActiveGuard{
		key: activeLockKeyPrefix + scope,
		ttl: activeGuardDefaultTTL,
	}
	g.interval = g.ttl / 3
	if ruleConfig.Locker != nil {
		g.locker = ruleConfig.Locker
	}
	if ruleConfig.Logger != nil {
		g.logger = ruleConfig.Logger
	}
	for _, opt := range opts {
		if opt != nil {
			opt(g)
		}
	}
	return g
}

// IsActive reports whether this replica currently holds the lease. Without a
// Locker it is always true.
// IsActive 报告本副本当前是否持有租约。未配置 Locker 时恒为 true。
func (g *ActiveGuard) IsActive() bool {
	if g == nil || g.locker == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.active
}

// Run drives the election until ctx is done. It blocks, so run it in a
// goroutine.
//
// Without a Locker, onPromoted runs once and the guard just waits for ctx,
// making a single replica behave exactly as before the guard was added.
//
// With a Locker, the guard polls every interval: while standby it tries to
// take the lease; as leader it renews it. Losing the lease — expired, taken
// over, or backend failure — invokes onDemoted and returns to standby, so a
// demoted leader stops consuming and a recovered backend restarts consuming.
// onPromoted returning an error releases the lease and stays standby. It is
// invoked once per leadership episode and must not block; onDemoted must be
// idempotent.
//
// Run 驱动选主直到 ctx 结束，阻塞执行，需在协程中运行。
//
// 未配置 Locker 时 onPromoted 仅执行一次，之后等待 ctx，单副本行为与加入
// 守卫前完全一致。
//
// 配置 Locker 后每个周期轮询一次：待命态尝试抢占租约，持有态续约。租约丢失
// ——过期、被接管、后端故障——会回调 onDemoted 并回到待命态，被降级副本因此
// 停止消费，后端恢复后重新开抢。onPromoted 返回错误则释放租约保持待命。它在
// 每次成为 leader 时执行一次且不得阻塞；onDemoted 必须可重入。
func (g *ActiveGuard) Run(ctx context.Context, onPromoted func() error, onDemoted func()) {
	if g == nil {
		return
	}
	if g.locker == nil {
		if err := onPromoted(); err != nil {
			g.logf("warn", "active guard %s activate: %v", g.key, err)
		}
		<-ctx.Done()
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			g.mu.Lock()
			token := g.token
			g.mu.Unlock()
			if token != "" {
				g.release(token)
			}
			return
		case <-timer.C:
		}
		g.tick(ctx, onPromoted, onDemoted)
		timer.Reset(g.interval)
	}
}

func (g *ActiveGuard) tick(ctx context.Context, onPromoted func() error, onDemoted func()) {
	g.mu.Lock()
	active, token := g.active, g.token
	g.mu.Unlock()

	if !active {
		t, ok, err := g.locker.TryLock(ctx, g.key, g.ttl)
		if err != nil {
			g.logf("warn", "active guard %s try lock: %v", g.key, err)
			return
		}
		if !ok {
			return
		}
		if err := onPromoted(); err != nil {
			g.logf("warn", "active guard %s activate: %v", g.key, err)
			g.release(t)
			return
		}
		g.mu.Lock()
		g.active, g.token = true, t
		g.mu.Unlock()
		g.logf("info", "active guard %s acquired", g.key)
		return
	}

	if g.renew(ctx, token) {
		return
	}
	g.mu.Lock()
	g.active, g.token = false, ""
	g.mu.Unlock()
	g.logf("warn", "active guard %s lost lease, demote", g.key)
	if onDemoted != nil {
		onDemoted()
	}
}

// renew extends the lease; it reports whether the caller still holds it.
// renew 顺延租约，返回调用方是否仍持有。
func (g *ActiveGuard) renew(ctx context.Context, token string) bool {
	if renewer, ok := g.locker.(LeaseRenewer); ok {
		ok, err := renewer.Renew(ctx, g.key, token, g.ttl)
		if err != nil {
			g.logf("warn", "active guard %s renew: %v", g.key, err)
		}
		return err == nil && ok
	}
	// 无续约能力的后端：释放后立即重取，窗口内可能被其他副本接管
	if err := g.locker.Unlock(ctx, g.key, token); err != nil {
		g.logf("warn", "active guard %s renew unlock: %v", g.key, err)
		return false
	}
	t, ok, err := g.locker.TryLock(ctx, g.key, g.ttl)
	if err != nil || !ok {
		if err != nil {
			g.logf("warn", "active guard %s renew relock: %v", g.key, err)
		}
		return false
	}
	g.mu.Lock()
	g.token = t
	g.mu.Unlock()
	return true
}

// release gives up the token so a standby can take over promptly.
// release 释放持有凭证，让待命副本尽快接管。
func (g *ActiveGuard) release(token string) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = g.locker.Unlock(ctx, g.key, token)
}

func (g *ActiveGuard) logf(level string, format string, v ...interface{}) {
	if g.logger == nil {
		return
	}
	switch level {
	case "debug":
		g.logger.Debugf(format, v...)
	case "info":
		g.logger.Infof(format, v...)
	default:
		g.logger.Warnf(format, v...)
	}
}

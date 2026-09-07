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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestActiveGuardWithoutLockerAlwaysActive(t *testing.T) {
	g := NewActiveGuard(Config{}, "test:scope")
	if !g.IsActive() {
		t.Fatalf("expected always active without locker")
	}
	promoted := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Run(ctx, func() error {
		close(promoted)
		<-ctx.Done()
		return nil
	}, nil)
	select {
	case <-promoted:
	case <-time.After(time.Second):
		t.Fatalf("onPromoted not invoked without locker")
	}
}

// TestActiveGuardSingleLeader 两个副本共享一个 Locker 时只能有一个 leader，
// leader 停机主动释放租约后，待命副本不等 TTL 过期、在轮询周期内接管。
func TestActiveGuardSingleLeader(t *testing.T) {
	locker := NewLocalLocker()

	var leaders atomic.Int32
	promotedWg1 := make(chan struct{})
	promotedWg2 := make(chan struct{})
	var promotedOnce1, promotedOnce2 sync.Once

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	// TTL 1s：若 leader 停机时不主动释放，接管要等 ~1s；断言 500ms 内
	// 接管即证明主动释放生效
	g1 := NewActiveGuard(Config{Locker: locker}, "election:test",
		WithActiveTTL(time.Second), WithActiveInterval(50*time.Millisecond))
	go g1.Run(ctx1, func() error {
		leaders.Add(1)
		promotedOnce1.Do(func() { close(promotedWg1) })
		return nil
	}, nil)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	g2 := NewActiveGuard(Config{Locker: locker}, "election:test",
		WithActiveTTL(time.Second), WithActiveInterval(50*time.Millisecond))
	go g2.Run(ctx2, func() error {
		leaders.Add(1)
		promotedOnce2.Do(func() { close(promotedWg2) })
		return nil
	}, nil)

	<-promotedWg1
	select {
	case <-promotedWg2:
		t.Fatalf("two replicas promoted concurrently")
	case <-time.After(500 * time.Millisecond):
	}

	// leader 停机释放租约后，待命副本必须接管
	cancel1()
	<-promotedWg2

	if n := leaders.Load(); n != 2 {
		t.Fatalf("expected exactly one leader at a time, got %d promotions", n)
	}
}

// TestActiveGuardPromoteError 覆盖 onPromoted 失败：释放租约并保持待命，
// 由其他副本接管。
func TestActiveGuardPromoteError(t *testing.T) {
	locker := NewLocalLocker()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	g1 := NewActiveGuard(Config{Locker: locker}, "election:err",
		WithActiveTTL(200*time.Millisecond), WithActiveInterval(50*time.Millisecond))
	go g1.Run(ctx1, func() error {
		return context.DeadlineExceeded
	}, nil)

	// g1 拿到租约但激活失败，g2 最终必须能成为 leader
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	g2 := NewActiveGuard(Config{Locker: locker}, "election:err",
		WithActiveTTL(200*time.Millisecond), WithActiveInterval(50*time.Millisecond))
	promoted2 := make(chan struct{})
	go g2.Run(ctx2, func() error {
		close(promoted2)
		<-ctx2.Done()
		return nil
	}, nil)

	select {
	case <-promoted2:
	case <-time.After(2 * time.Second):
		t.Fatalf("standby did not take over after promote failure")
	}
}

// flakyRenewer 在 fail 置位时模拟续约失败（键被接管或后端故障）。
type flakyRenewer struct {
	*LocalLocker
	fail atomic.Bool
}

func (f *flakyRenewer) Renew(ctx context.Context, key, token string, expiration time.Duration) (bool, error) {
	if f.fail.Load() {
		return false, nil
	}
	return f.LocalLocker.Renew(ctx, key, token, expiration)
}

// TestActiveGuardRenewFailure leader 续约失败后必须回调 onDemoted，
// 待命副本在后续轮询中接管。
func TestActiveGuardRenewFailure(t *testing.T) {
	locker := &flakyRenewer{LocalLocker: NewLocalLocker()}

	newGuard := func() *ActiveGuard {
		return NewActiveGuard(Config{Locker: locker}, "election:renew",
			WithActiveTTL(200*time.Millisecond), WithActiveInterval(50*time.Millisecond))
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	g1 := newGuard()
	demoted1 := make(chan struct{})
	go g1.Run(ctx1, func() error { return nil }, func() { close(demoted1) })

	// 等 g1 成为 leader
	deadline := time.Now().Add(2 * time.Second)
	for !g1.IsActive() {
		if time.Now().After(deadline) {
			t.Fatalf("g1 never became leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 续约失败 → 降级
	locker.fail.Store(true)
	select {
	case <-demoted1:
	case <-time.After(2 * time.Second):
		t.Fatalf("leader not demoted after renew failure")
	}
	if g1.IsActive() {
		t.Fatalf("guard should be standby after renew failure")
	}

	// 恢复后 g2 接管成为 leader
	locker.fail.Store(false)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	g2 := newGuard()
	promoted2 := make(chan struct{})
	go g2.Run(ctx2, func() error {
		close(promoted2)
		return nil
	}, nil)

	select {
	case <-promoted2:
	case <-time.After(2 * time.Second):
		t.Fatalf("standby did not take over after leader demoted")
	}
}

func TestLocalLockerRenew(t *testing.T) {
	locker := NewLocalLocker()
	token, ok, err := locker.TryLock(context.Background(), "k", 50*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("try lock: %v %v", ok, err)
	}
	if ok, err := locker.Renew(context.Background(), "k", token, 100*time.Millisecond); err != nil || !ok {
		t.Fatalf("renew own lock: %v %v", ok, err)
	}
	time.Sleep(60 * time.Millisecond)
	// 续约后原 TTL 已顺延，锁仍被持有
	if _, ok, _ := locker.TryLock(context.Background(), "k", 50*time.Millisecond); ok {
		t.Fatalf("lock should still be held after renew")
	}
	if ok, err := locker.Renew(context.Background(), "k", "wrong-token", 100*time.Millisecond); ok || err != nil {
		t.Fatalf("renew with wrong token should be false, nil: %v %v", ok, err)
	}
}

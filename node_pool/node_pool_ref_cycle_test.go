/*
 * Copyright 2026 The RuleGo Authors.
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

package node_pool

import (
	"strings"
	"testing"

	"github.com/rulego/rulego/test/assert"
)

// refTestEndpoint is a minimal ref-aware endpoint stub: RefTarget exposes its
// ref:// borrow target for the pool's registration-time cycle check.
type refTestEndpoint struct {
	aliasTestEndpoint
	id     string
	target string
}

func (e *refTestEndpoint) Id() string                       { return e.id }
func (e *refTestEndpoint) GetInstance() (interface{}, error) { return e, nil }
func (e *refTestEndpoint) RefTarget() string                { return e.target }

// TestPoolRefCycleRejected: registering an entry whose ref:// chain closes a
// cycle is rejected; the offending entry stays out of the pool.
func TestPoolRefCycleRejected(t *testing.T) {
	pool, _ := newAliasTestPool()
	epA := &refTestEndpoint{id: "pool_a", target: "ref://pool_b"}
	epB := &refTestEndpoint{id: "pool_b", target: "ref://pool_a"}

	_, err := pool.AddNode(epA)
	assert.Nil(t, err)
	_, err = pool.AddNode(epB)
	if err == nil {
		t.Fatal("cyclic ref:// registration should be rejected")
	}
	if !strings.Contains(err.Error(), "circular ref://") {
		t.Fatalf("error should mention circular ref, got: %v", err)
	}
	// pool_b stays out of the pool; pool_a remains usable
	if _, ok := pool.Get("pool_b"); ok {
		t.Fatal("pool_b should not be registered after cycle rejection")
	}
	if _, ok := pool.Get("pool_a"); !ok {
		t.Fatal("pool_a should remain registered")
	}
}

// TestPoolRefChainNoCycle: a linear borrow chain (b→a) registers fine.
func TestPoolRefChainNoCycle(t *testing.T) {
	pool, _ := newAliasTestPool()
	epA := &refTestEndpoint{id: "pool_a", target: ""}
	epB := &refTestEndpoint{id: "pool_b", target: "ref://pool_a"}

	_, err := pool.AddNode(epA)
	assert.Nil(t, err)
	_, err = pool.AddNode(epB)
	assert.Nil(t, err)
	if _, ok := pool.Get("pool_b"); !ok {
		t.Fatal("pool_b should be registered (linear chain is fine)")
	}
}

// TestPoolSelfRefRejected: an entry referencing itself is rejected.
func TestPoolSelfRefRejected(t *testing.T) {
	pool, _ := newAliasTestPool()
	ep := &refTestEndpoint{id: "pool_self", target: "ref://pool_self"}
	if _, err := pool.AddNode(ep); err == nil {
		t.Fatal("self ref:// should be rejected")
	} else if !strings.Contains(err.Error(), "circular ref://") {
		t.Fatalf("error should mention circular ref, got: %v", err)
	}
}

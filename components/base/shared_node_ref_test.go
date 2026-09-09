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

package base

import (
	"strings"
	"sync"
	"testing"

	"github.com/rulego/rulego/api/types"
)

// stubRegistry is a minimal types.ResourceRegistry mirroring
// engine.resourceRegistry, so tests here do not need to import engine.
type stubRegistry struct {
	mu    sync.RWMutex
	items map[string]any
}

func newStubRegistry() *stubRegistry {
	return &stubRegistry{items: map[string]any{}}
}

func (r *stubRegistry) Lookup(id string) (any, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.items[id]
	return v, ok
}

func (r *stubRegistry) Register(id string, resource any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items[id] = resource
}

func (r *stubRegistry) Unregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.items, id)
}

// stubChainCtx overrides only Resources/ResourceRegistry; the nil embedded
// interface panics loudly if any other ChainCtx method is touched.
type stubChainCtx struct {
	types.ChainCtx
	reg *stubRegistry
}

func (s *stubChainCtx) Resources() types.ResourceLookup          { return s.reg }
func (s *stubChainCtx) ResourceRegistry() types.ResourceRegistry { return s.reg }

// stubConn is an identifiable fake connection.
type stubConn struct{ addr string }

// otherConn is an unrelated connection type for the cross-type case.
type otherConn struct{ addr string }

// stubEndpoint mimics an endpoint embedding SharedNode: EndpointAspect registers
// the endpoint instance itself (not a connHolder) into the chain directory, so a
// ref:// borrower must resolve it through the GetInstance fallback.
type stubEndpoint struct {
	SharedNode[*stubConn]
}

func bindChainConfiguration(ctx types.ChainCtx, nodeId string) types.Configuration {
	return types.Configuration{
		types.NodeConfigurationKeyChainCtx:       ctx,
		types.NodeConfigurationKeySelfDefinition: types.RuleNode{Id: nodeId},
	}
}

// TestUnpackHolderEndpointFallbackHit verifies that a borrower resolves a
// chain-registered endpoint to the endpoint's underlying connection.
func TestUnpackHolderEndpointFallbackHit(t *testing.T) {
	ctx := &stubChainCtx{reg: newStubRegistry()}

	ep := &stubEndpoint{}
	_ = ep.InitWithClose(types.Config{}, "stub/endpoint", "ep-server", false, func() (*stubConn, error) {
		return &stubConn{addr: "ep-server"}, nil
	}, nil)
	ctx.reg.Register("ep1", ep)

	borrower := &SharedNode[*stubConn]{}
	_ = borrower.InitWithClose(types.Config{}, "stub/borrower", "ref://ep1", false, nil, nil)
	borrower.BindChain(bindChainConfiguration(ctx, "borrower"))

	got, err := borrower.GetSafely()
	if err != nil {
		t.Fatalf("borrower GetSafely: %v", err)
	}
	underlying, err := ep.GetSafely()
	if err != nil {
		t.Fatalf("endpoint GetSafely: %v", err)
	}
	if got != underlying {
		t.Fatalf("borrower got %p, want the endpoint's connection %p", got, underlying)
	}
	if got.addr != "ep-server" {
		t.Fatalf("connection addr = %q, want ep-server", got.addr)
	}
}

// TestUnpackHolderEndpointFallbackCrossType verifies that borrowing an endpoint
// whose underlying connection has a different type reports an incompatible error.
func TestUnpackHolderEndpointFallbackCrossType(t *testing.T) {
	ctx := &stubChainCtx{reg: newStubRegistry()}

	ep := &stubEndpoint{}
	_ = ep.InitWithClose(types.Config{}, "stub/endpoint", "ep-server", false, func() (*stubConn, error) {
		return &stubConn{addr: "ep-server"}, nil
	}, nil)
	ctx.reg.Register("ep1", ep)

	borrower := &SharedNode[*otherConn]{}
	_ = borrower.InitWithClose(types.Config{}, "stub/borrower", "ref://ep1", false, nil, nil)
	borrower.BindChain(bindChainConfiguration(ctx, "borrower"))

	if _, err := borrower.GetSafely(); err == nil {
		t.Fatal("cross-type borrow should fail")
	} else if !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("error should mention incompatibility, got: %v", err)
	}
}

// TestUnpackHolderEndpointFallbackCycle verifies that a circular ref:// chain is
// rejected instead of recursing until stack overflow.
func TestUnpackHolderEndpointFallbackCycle(t *testing.T) {
	ctx := &stubChainCtx{reg: newStubRegistry()}

	// An endpoint whose server refs itself and is chain-bound: the only setup
	// that can loop the chain-directory resolution path.
	ep := &stubEndpoint{}
	_ = ep.InitWithClose(types.Config{}, "stub/endpoint", "ref://ep1", false, func() (*stubConn, error) {
		return &stubConn{addr: "unused"}, nil
	}, nil)
	ep.BindChain(bindChainConfiguration(ctx, "ep1"))
	ctx.reg.Register("ep1", ep)

	if _, err := ep.GetSafely(); err == nil {
		t.Fatal("self-referential ref:// should fail")
	} else if !strings.Contains(err.Error(), "circular ref://") {
		t.Fatalf("error should mention circular reference, got: %v", err)
	}
}

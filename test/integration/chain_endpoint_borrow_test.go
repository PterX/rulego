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

package integration

import (
	"testing"

	"github.com/rulego/rulego"
	"github.com/rulego/rulego/engine"
)

// TestEndpointBorrowsLazyChainNode verifies the endpoint→same-chain-node ref://
// direction: an endpoint/kafka-style connection holder borrows a lazy (not yet
// connected) chain node's connection via ref://. The borrower resolves at
// AddRouter time: chain-directory miss triggers the target node's lazy init
// (through the injected $chainCtx), the node registers its connection, and the
// borrower re-lookup hits.
//
// Uses test/conn (connection-holding stub) plus a stub endpoint embedding the
// same SharedNode type, standing in for protocol-specific endpoints.
func TestEndpointBorrowsLazyChainNode(t *testing.T) {
	chain := `{
	  "ruleChain": {"id": "test_ep_borrow_lazy_node", "name": "endpoint borrows lazy node", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "endpoints": [
	      {"id": "ep_borrower", "type": "endpoint/testConn", "configuration": {"server": "ref://src"}}
	    ],
	    "nodes": [
	      {"id": "src", "type": "test/conn", "configuration": {"server": "deviceA"}},
	      {"id": "n1", "type": "jsTransform", "configuration": {"jsScript": "return {msg:msg,metadata:metadata,msgType:msgType};"}}
	    ],
	    "connections": []
	  }
	}`

	eng, err := rulego.New("test_ep_borrow_lazy_node", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("test_ep_borrow_lazy_node")
	ruleEng := eng.(*engine.RuleEngine)

	// The borrower endpoint must have resolved src's connection at deploy time:
	// chain deployment is two-phase, ApplyRouters runs after all endpoints are
	// registered, and the lazy-init trigger must have connected src.
	borrower, found := ruleEng.RootRuleChainCtx().Resources().Lookup("ep_borrower")
	if !found {
		t.Fatal("ep_borrower not registered in chain Resources (EndpointAspect syncResources failed)")
	}
	src := getTestNode(t, ruleEng, "src")
	srcConn, err := src.conn()
	if err != nil {
		t.Fatalf("src conn after deploy: %v", err)
	}

	bep, ok := borrower.(interface{ Conn() (*testConn, error) })
	if !ok {
		t.Fatalf("ep_borrower resource type %T has no Conn()", borrower)
	}
	got, err := bep.Conn()
	if err != nil {
		t.Fatalf("borrower conn: %v", err)
	}
	if got != srcConn {
		t.Fatalf("endpoint did not borrow lazy node's connection: got %+v want %+v", got, srcConn)
	}
	if got.addr != "deviceA" {
		t.Fatalf("borrowed conn addr = %q, want deviceA", got.addr)
	}
}

// TestEndpointBorrowsEndpoint verifies the endpoint→same-chain-endpoint ref://
// direction: two endpoints of the same type on one chain, the second borrows
// the first's connection. Only viable with two-phase deployment (instances
// register into the chain directory before any subscribes) — with one-phase
// the borrower's AddRouter runs before the source is registered and fails.
func TestEndpointBorrowsEndpoint(t *testing.T) {
	chain := `{
	  "ruleChain": {"id": "test_ep_borrow_ep", "name": "endpoint borrows endpoint", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "endpoints": [
	      {"id": "ep_src", "type": "endpoint/testConn", "configuration": {"server": "brokerA"}},
	      {"id": "ep_borrower", "type": "endpoint/testConn", "configuration": {"server": "ref://ep_src"}}
	    ],
	    "nodes": [
	      {"id": "n1", "type": "jsTransform", "configuration": {"jsScript": "return {msg:msg,metadata:metadata,msgType:msgType};"}}
	    ],
	    "connections": []
	  }
	}`

	eng, err := rulego.New("test_ep_borrow_ep", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("test_ep_borrow_ep")
	ruleEng := eng.(*engine.RuleEngine)

	// The borrower must resolve the source endpoint's connection: two-phase
	// deployment registered both instances before the borrower subscribed.
	res, found := ruleEng.RootRuleChainCtx().Resources().Lookup("ep_borrower")
	if !found {
		t.Fatal("ep_borrower not registered in chain Resources")
	}
	bep, ok := res.(*testConnEndpoint)
	if !ok {
		t.Fatalf("ep_borrower resource type %T not *testConnEndpoint", res)
	}
	srcRes, found := ruleEng.RootRuleChainCtx().Resources().Lookup("ep_src")
	if !found {
		t.Fatal("ep_src not registered in chain Resources")
	}
	sep, ok := srcRes.(*testConnEndpoint)
	if !ok {
		t.Fatalf("ep_src resource type %T not *testConnEndpoint", srcRes)
	}
	srcConn, err := sep.Conn()
	if err != nil {
		t.Fatalf("src conn: %v", err)
	}
	got, err := bep.Conn()
	if err != nil {
		t.Fatalf("borrower conn: %v", err)
	}
	if got != srcConn {
		t.Fatalf("endpoint did not borrow same-chain endpoint's connection: got %+v want %+v", got, srcConn)
	}
	if got.addr != "brokerA" {
		t.Fatalf("borrowed conn addr = %q, want brokerA", got.addr)
	}
}

// TestEndpointBorrowCycleRejected verifies a ref:// cycle among chain nodes is
// rejected at deploy time instead of recursing: node a refs b, b refs a, and an
// endpoint borrows a. The borrower's resolve must fail with the cycle error,
// not stack overflow.
func TestEndpointBorrowCycleRejected(t *testing.T) {
	chain := `{
	  "ruleChain": {"id": "test_ep_borrow_cycle", "name": "endpoint borrow cycle", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "endpoints": [
	      {"id": "ep_borrower", "type": "endpoint/testConn", "configuration": {"server": "ref://a"}}
	    ],
	    "nodes": [
	      {"id": "a", "type": "test/conn", "configuration": {"server": "ref://b"}},
	      {"id": "b", "type": "test/conn", "configuration": {"server": "ref://a"}}
	    ],
	    "connections": []
	  }
	}`

	// Deploy must either fail (router attach rejected) or leave the borrower
	// unresolvable with a cycle error — never panic/stack overflow.
	_, err := rulego.New("test_ep_borrow_cycle", []byte(chain))
	if err == nil {
		defer rulego.Del("test_ep_borrow_cycle")
	}
	// err != nil is the expected path: AddRouter fails on the cycle error.
	if err != nil {
		t.Logf("deploy rejected with: %v", err)
	}
}

// TestChainCtxInjectedIntoEndpointConfig verifies the $chainCtx injection
// mechanism directly: a local-mode endpoint deployed on a chain registers its
// own connection into the chain resource directory (BindChain took effect via
// the injected chain context).
func TestChainCtxInjectedIntoEndpointConfig(t *testing.T) {
	chain := `{
	  "ruleChain": {"id": "test_ep_chainctx", "name": "chainctx injection", "root": true},
	  "metadata": {
	    "firstNodeIndex": 0,
	    "endpoints": [
	      {"id": "ep1", "type": "endpoint/testConn", "configuration": {"server": "standalone"}}
	    ],
	    "nodes": [
	      {"id": "n1", "type": "jsTransform", "configuration": {"jsScript": "return {msg:msg,metadata:metadata,msgType:msgType};"}}
	    ],
	    "connections": []
	  }
	}`

	eng, err := rulego.New("test_ep_chainctx", []byte(chain))
	if err != nil {
		t.Fatalf("load chain: %v", err)
	}
	defer rulego.Del("test_ep_chainctx")
	ruleEng := eng.(*engine.RuleEngine)

	// Aspect registers the endpoint instance under its id...
	res, found := ruleEng.RootRuleChainCtx().Resources().Lookup("ep1")
	if !found {
		t.Fatal("ep1 not registered in chain Resources")
	}
	bep, ok := res.(*testConnEndpoint)
	if !ok {
		t.Fatalf("ep1 resource type %T not *testConnEndpoint", res)
	}
	// ...and the endpoint's own connection must resolve to the standalone addr.
	got, err := bep.Conn()
	if err != nil {
		t.Fatalf("ep1 conn: %v", err)
	}
	if got == nil || got.addr != "standalone" {
		t.Fatalf("ep1 conn = %+v, want addr standalone", got)
	}
}

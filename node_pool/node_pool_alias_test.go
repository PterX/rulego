/*
 * Copyright 2024 The RuleGo Authors.
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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint/impl"
	"github.com/rulego/rulego/engine"
	"github.com/rulego/rulego/test/assert"
)

// aliasTestEndpoint 模拟连接地址作为主键的 endpoint 组件（如 mqtt/rest 的 Id()=Config.Server），
// 全程无网络依赖。
type aliasTestEndpoint struct {
	impl.BaseEndpoint
	id        string
	destroyed int32
}

func (e *aliasTestEndpoint) Type() string                                     { return "aliasTestEndpoint" }
func (e *aliasTestEndpoint) New() types.Node                                  { return &aliasTestEndpoint{} }
func (e *aliasTestEndpoint) Init(_ types.Config, _ types.Configuration) error { return nil }
func (e *aliasTestEndpoint) Id() string                                       { return e.id }
func (e *aliasTestEndpoint) Start() error                                     { return nil }
func (e *aliasTestEndpoint) Destroy()                                         { atomic.AddInt32(&e.destroyed, 1) }
func (e *aliasTestEndpoint) AddRouter(_ endpointApi.Router, _ ...interface{}) (string, error) {
	return "1", nil
}
func (e *aliasTestEndpoint) RemoveRouter(_ string, _ ...interface{}) error { return nil }
func (e *aliasTestEndpoint) GetInstance() (interface{}, error)             { return e, nil }

// aliasTestSharedNode 供 NewFromRuleNode 路径使用，需注册进自定义组件表。
type aliasTestSharedNode struct {
	destroyed int32
}

func (n *aliasTestSharedNode) Type() string { return "aliasTestSharedNode" }
func (n *aliasTestSharedNode) New() types.Node {
	return &aliasTestSharedNode{}
}
func (n *aliasTestSharedNode) Init(_ types.Config, _ types.Configuration) error { return nil }
func (n *aliasTestSharedNode) OnMsg(_ types.RuleContext, _ types.RuleMsg)       {}
func (n *aliasTestSharedNode) Destroy()                                         { atomic.AddInt32(&n.destroyed, 1) }
func (n *aliasTestSharedNode) GetInstance() (interface{}, error)                { return n, nil }

func newAliasTestPool() (*NodePool, types.Config) {
	registry := engine.NewCustomComponentRegistry(engine.Registry, new(engine.RuleComponentRegistry))
	_ = registry.Register(&aliasTestSharedNode{})
	config := engine.NewConfig(types.WithComponentsRegistry(registry))
	pool := NewNodePool(config)
	config.NodePool = pool
	return pool, config
}

// 主键与别名解析到同一实例，别名不计入遍历结果。
func TestAddNodeWithAlias(t *testing.T) {
	pool, _ := newAliasTestPool()
	ep := &aliasTestEndpoint{id: "tcp://127.0.0.1:1883"}

	ctx, err := pool.AddNodeWithAlias("gateway_mqtt", ep)
	assert.Nil(t, err)
	assert.NotNil(t, ctx)

	//主键仍是组件自身 Id，而非别名
	byPrimary, ok := pool.Get("tcp://127.0.0.1:1883")
	assert.True(t, ok)
	byAlias, ok := pool.Get("gateway_mqtt")
	assert.True(t, ok)
	assert.True(t, byPrimary == byAlias)

	//GetInstance 与 Lookup（ref:// 解析路径）按主键和别名取到同一实例
	insPrimary, err := pool.GetInstance("tcp://127.0.0.1:1883")
	assert.Nil(t, err)
	insAlias, err := pool.GetInstance("gateway_mqtt")
	assert.Nil(t, err)
	assert.True(t, insPrimary.(*aliasTestEndpoint) == ep)
	assert.True(t, insAlias.(*aliasTestEndpoint) == ep)

	ins, found := pool.Lookup("gateway_mqtt")
	assert.True(t, found)
	assert.True(t, ins.(*aliasTestEndpoint) == ep)

	//别名不重复计入遍历与定义导出
	assert.Equal(t, 1, len(pool.GetAll()))
	defs, err := pool.GetAllDef()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(defs["aliasTestEndpoint"]))

	visited := 0
	pool.Range(func(_, _ any) bool {
		visited++
		return true
	})
	assert.Equal(t, 1, visited)
}

// AddAlias 支持主键或已有别名定位节点，可批量追加。
func TestAddAlias(t *testing.T) {
	pool, _ := newAliasTestPool()
	ep := &aliasTestEndpoint{id: "tcp://127.0.0.1:1883"}
	_, err := pool.AddNode(ep)
	assert.Nil(t, err)

	assert.Nil(t, pool.AddAlias("tcp://127.0.0.1:1883", "alias_a", "alias_b"))
	_, ok := pool.Get("alias_a")
	assert.True(t, ok)
	_, ok = pool.Get("alias_b")
	assert.True(t, ok)

	//用已有别名定位同一节点，继续追加
	assert.Nil(t, pool.AddAlias("alias_a", "alias_c"))
	_, ok = pool.Get("alias_c")
	assert.True(t, ok)

	node, err := pool.GetInstance("alias_c")
	assert.Nil(t, err)
	assert.True(t, node.(*aliasTestEndpoint) == ep)

	assert.Equal(t, 1, len(pool.GetAll()))
}

// 别名等于主键 no-op；重复绑同一别名 no-op。
func TestAliasIdempotent(t *testing.T) {
	pool, _ := newAliasTestPool()
	ep := &aliasTestEndpoint{id: "tcp://127.0.0.1:1883"}
	_, err := pool.AddNodeWithAlias("gateway_mqtt", ep)
	assert.Nil(t, err)

	assert.Nil(t, pool.AddAlias("tcp://127.0.0.1:1883", "tcp://127.0.0.1:1883"))
	assert.Nil(t, pool.AddAlias("gateway_mqtt", "gateway_mqtt"))

	byAlias, ok := pool.Get("gateway_mqtt")
	assert.True(t, ok)
	assert.True(t, byAlias.GetNodeId().Id == "tcp://127.0.0.1:1883")
}

// 别名冲突规则：占用他人主键报错，占用他人别名报错且不影响既有绑定。
func TestAliasConflict(t *testing.T) {
	pool, _ := newAliasTestPool()
	epA := &aliasTestEndpoint{id: "server_a"}
	epB := &aliasTestEndpoint{id: "server_b"}
	_, err := pool.AddNodeWithAlias("shared_name", epA)
	assert.Nil(t, err)
	_, err = pool.AddNode(epB)
	assert.Nil(t, err)

	//别名占用另一节点主键
	err = pool.AddAlias("server_b", "server_a")
	assert.NotNil(t, err)
	//别名已被其他节点占用
	err = pool.AddAlias("server_b", "shared_name")
	assert.NotNil(t, err)

	//既有绑定不受冲突影响
	ins, err := pool.GetInstance("shared_name")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestEndpoint) == epA)
}

// AddNodeWithAlias 别名冲突时节点保留在池中（主键可用），别名维持旧绑定。
func TestAddNodeWithAliasConflictKeepsNode(t *testing.T) {
	pool, _ := newAliasTestPool()
	epA := &aliasTestEndpoint{id: "server_a"}
	epB := &aliasTestEndpoint{id: "server_b"}
	_, err := pool.AddNodeWithAlias("dup", epA)
	assert.Nil(t, err)

	ctx, err := pool.AddNodeWithAlias("dup", epB)
	assert.NotNil(t, err)
	assert.NotNil(t, ctx) //节点已入池，返回其上下文供调用方使用

	_, ok := pool.Get("server_b")
	assert.True(t, ok)
	ins, err := pool.GetInstance("dup")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestEndpoint) == epA)

	//换绑：先解绑旧节点，B 即可占用该别名
	pool.Del("server_a")
	assert.Nil(t, pool.AddAlias("server_b", "dup"))
	ins, err = pool.GetInstance("dup")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestEndpoint) == epB)
}

// 空别名直接报错且节点不入池。
func TestAddNodeWithAliasEmpty(t *testing.T) {
	pool, _ := newAliasTestPool()

	ctx, err := pool.AddNodeWithAlias("", &aliasTestEndpoint{id: "server_a"})
	assert.NotNil(t, err)
	assert.Nil(t, ctx)
	assert.Equal(t, 0, len(pool.GetAll()))

	_, err = pool.AddNode(&aliasTestEndpoint{id: "server_b"})
	assert.Nil(t, err)
	assert.NotNil(t, pool.AddAlias("server_b", ""))
	assert.NotNil(t, pool.AddAlias("not_found", "x"))
}

// 别名不得被新节点主键抢占（AddNode/NewFromEndpoint/NewFromRuleNode 一致拒绝）。
func TestAliasShadowGuard(t *testing.T) {
	pool, _ := newAliasTestPool()
	_, err := pool.AddNodeWithAlias("occupied", &aliasTestEndpoint{id: "server_a"})
	assert.Nil(t, err)

	_, err = pool.AddNode(&aliasTestEndpoint{id: "occupied"})
	assert.NotNil(t, err)

	_, err = pool.NewFromEndpoint(types.EndpointDsl{RuleNode: types.RuleNode{
		Id:   "occupied",
		Type: "endpoint/mqtt",
		Configuration: types.Configuration{
			"server": "127.0.0.1:1883",
		},
	}})
	assert.NotNil(t, err)

	_, err = pool.NewFromRuleNode(types.RuleNode{
		Id:   "occupied",
		Type: "aliasTestSharedNode",
	})
	assert.NotNil(t, err)

	//既有绑定不受影响
	ins, err := pool.GetInstance("occupied")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestEndpoint).id == "server_a")
}

// 按主键或别名 Del 均删除节点并清理全部别名；删除后别名可复用。
func TestDelAlias(t *testing.T) {
	pool, _ := newAliasTestPool()
	ep := &aliasTestEndpoint{id: "server_a"}
	_, err := pool.AddNodeWithAlias("alias_a", ep)
	assert.Nil(t, err)
	assert.Nil(t, pool.AddAlias("alias_a", "alias_b"))

	//按别名删除
	pool.Del("alias_b")
	assert.Equal(t, 0, len(pool.GetAll()))
	_, ok := pool.Get("server_a")
	assert.False(t, ok)
	_, ok = pool.Get("alias_a")
	assert.False(t, ok)
	_, ok = pool.Get("alias_b")
	assert.False(t, ok)
	assert.Equal(t, int32(1), atomic.LoadInt32(&ep.destroyed))

	//删除后别名可重新绑定到新节点
	ep2 := &aliasTestEndpoint{id: "server_b"}
	_, err = pool.AddNodeWithAlias("alias_a", ep2)
	assert.Nil(t, err)
	ins, err := pool.GetInstance("alias_a")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestEndpoint) == ep2)

	//按主键删除
	pool.Del("server_b")
	assert.Equal(t, 0, len(pool.GetAll()))
	_, ok = pool.Get("alias_a")
	assert.False(t, ok)
	assert.Equal(t, int32(1), atomic.LoadInt32(&ep2.destroyed))

	//重复 Del 与删除不存在的 id 均为 no-op
	pool.Del("server_b")
	pool.Del("alias_a")
	assert.Equal(t, 0, len(pool.GetAll()))
}

// Stop 释放所有节点并清空别名。
func TestStopAliasCleanup(t *testing.T) {
	pool, _ := newAliasTestPool()
	epA := &aliasTestEndpoint{id: "server_a"}
	epB := &aliasTestEndpoint{id: "server_b"}
	_, err := pool.AddNodeWithAlias("alias_a", epA)
	assert.Nil(t, err)
	_, err = pool.AddNodeWithAlias("alias_b", epB)
	assert.Nil(t, err)

	pool.Stop()
	assert.Equal(t, 0, len(pool.GetAll()))
	_, ok := pool.Get("alias_a")
	assert.False(t, ok)
	_, ok = pool.Get("alias_b")
	assert.False(t, ok)
	assert.Equal(t, int32(1), atomic.LoadInt32(&epA.destroyed))
	assert.Equal(t, int32(1), atomic.LoadInt32(&epB.destroyed))
}

// NewFromRuleNode 创建的共享节点同样可绑别名。
func TestRuleNodeAlias(t *testing.T) {
	pool, _ := newAliasTestPool()
	ctx, err := pool.NewFromRuleNode(types.RuleNode{
		Id:   "shared_db",
		Type: "aliasTestSharedNode",
	})
	assert.Nil(t, err)
	assert.NotNil(t, ctx)

	assert.Nil(t, pool.AddAlias("shared_db", "db"))
	ins, err := pool.GetInstance("db")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestSharedNode) != nil)

	nodeCtx, ok := pool.Get("db")
	assert.True(t, ok)
	nodeCtx.Destroy()
	//销毁经 RuleNodeCtx 透传到底层节点
	assert.Equal(t, int32(1), atomic.LoadInt32(&ins.(*aliasTestSharedNode).destroyed))
}

// 主键与别名读、幂等别名写并发下无 panic，结果一致。
func TestAliasConcurrentAccess(t *testing.T) {
	pool, _ := newAliasTestPool()
	ep := &aliasTestEndpoint{id: "server_a"}
	_, err := pool.AddNodeWithAlias("alias_0", ep)
	assert.Nil(t, err)
	//先绑满全部别名，并发阶段只做幂等重复绑定，避免绑定前 miss 的预期窗口
	for i := 1; i < 4; i++ {
		assert.Nil(t, pool.AddAlias("server_a", fmt.Sprintf("alias_%d", i)))
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				alias := fmt.Sprintf("alias_%d", j%4)
				if j%3 == 0 {
					_ = pool.AddAlias("server_a", alias)
				}
				ctxP, okP := pool.Get("server_a")
				ctxA, okA := pool.Get(alias)
				if okP != okA {
					t.Error("primary and alias resolved inconsistently")
					return
				}
				if okP && ctxP != ctxA {
					t.Error("primary and alias resolved to different contexts")
					return
				}
			}
		}(i)
	}
	wg.Wait()

	ins, err := pool.GetInstance("server_a")
	assert.Nil(t, err)
	assert.True(t, ins.(*aliasTestEndpoint) == ep)
}

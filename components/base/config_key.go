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

package base

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/rulego/rulego/api/types"
)

// ConfigKey derives a stable identity from component configuration: identical
// configs across replicas hash to the same key, so it can name distributed
// locks (election, dedup scopes). Marshaling failure degrades to a constant;
// component configurations are plain data and do not fail to marshal.
// ConfigKey 从组件配置推导稳定标识：相同配置在各副本散列出相同键，可用于
// 分布式锁键（选主、去重 scope）。序列化失败退化为常量；组件配置均为纯数据，
// 序列化不会失败。
func ConfigKey(configuration interface{}) string {
	b, err := json.Marshal(configuration)
	if err != nil {
		return "config"
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// NodeIdOf reads the endpoint node Id injected via
// NodeConfigurationKeySelfDefinition. The Id is stable across replicas and
// unique within its chain, which makes it the preferred instance identity for
// election scopes; returns empty when absent (direct instantiation, tests).
// NodeIdOf 读取端点节点 Id（NodeConfigurationKeySelfDefinition 注入）。
// 该 Id 跨副本一致且链内唯一，是选主 scope 的首选实例标识；未注入时返回空
// （直接实例化、测试场景）。
func NodeIdOf(configuration types.Configuration) string {
	if v, ok := configuration[types.NodeConfigurationKeySelfDefinition]; ok {
		if rn, ok := v.(types.RuleNode); ok {
			return rn.Id
		}
	}
	return ""
}

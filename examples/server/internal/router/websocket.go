package router

import (
	"examples/server/config"
	"examples/server/internal/constants"
	"examples/server/internal/controller"
	"examples/server/internal/service"
	"github.com/gorilla/websocket"
	"github.com/rulego/rulego/api/types"
	endpointApi "github.com/rulego/rulego/api/types/endpoint"
	"github.com/rulego/rulego/endpoint/rest"
	websocketEndpoint "github.com/rulego/rulego/endpoint/websocket"
	"github.com/rulego/rulego/utils/json"
	"net/http"
	"time"
)

// NewWebsocketServe Websocket服务 接收端点
func NewWebsocketServe(c config.Config, restEndpoint *rest.Rest) *websocketEndpoint.Endpoint {
	//初始化日志
	wsEndpoint := &websocketEndpoint.Endpoint{
		Rest: restEndpoint,
		Upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true // 允许所有跨域请求
			},
		},
	}
	wsEndpoint.OnEvent = func(eventName string, params ...interface{}) {
		switch eventName {
		case endpointApi.EventConnect:
			exchange := params[0].(*endpointApi.Exchange)
			username := exchange.In.Headers().Get(constants.KeyUsername)
			if username == "" {
				username = config.C.DefaultUsername
			}
			if s, ok := service.UserRuleEngineServiceImpl.Get(username); ok {
				chainId := exchange.In.GetParam(constants.KeyChainId)
				clientId := exchange.In.GetParam(constants.KeyClientId)
				s.AddOnDebugObserver(chainId, clientId, func(chainId, flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
					errStr := ""
					if err != nil {
						errStr = err.Error()
					}
					var log = map[string]interface{}{
						"chainId":      chainId,
						"flowType":     flowType,
						"nodeId":       nodeId,
						"relationType": relationType,
						"err":          errStr,
						"msg":          msg,
						"ts":           time.Now().UnixMilli(),
					}
					jsonStr, _ := json.Marshal(log)
					exchange.Out.SetBody(jsonStr)
					//写入报错
					if exchange.Out.GetError() != nil {
						s.RemoveOnDebugObserver(clientId)
					}
				})
			}
		case endpointApi.EventDisconnect:
			exchange := params[0].(*endpointApi.Exchange)
			username := exchange.In.Headers().Get(constants.KeyUsername)
			if username == "" {
				username = config.C.DefaultUsername
			}
			if s, ok := service.UserRuleEngineServiceImpl.Get(username); ok {
				s.RemoveOnDebugObserver(exchange.In.GetParam(constants.KeyClientId))
			}
		}
	}
	_, _ = wsEndpoint.AddRouter(controller.WsNodeLogRouter(apiBasePath + "/event/ws/:chainId/:clientId"))

	return wsEndpoint
}

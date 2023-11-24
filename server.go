package itdepthserver

import (
	"context"
	"net"
	"time"

	"github.com/bryanchen463/IT_depthServer/logger"
	"github.com/panjf2000/gnet/v2"
	"github.com/xh23123/IT_hftcommon/pkg/crexServer/codec"
	"github.com/xh23123/IT_hftcommon/pkg/crexServer/codec/message"
	"go.uber.org/zap"
)

type connKey string

type ServerHandler struct {
	logger      logger.Logger
	engine      gnet.Engine
	codec       codec.CodecInterface
	isLittleEnd bool
}

func send(c net.Conn, date []byte) error {
	left := len(date)
	total := len(date)
	for left > 0 {
		n, err := c.Write(date[total-left:])
		if err != nil {
			return err
		}
		left = left - n
	}
	return nil
}

func (h *ServerHandler) response(c gnet.Conn, proxyRsp *message.ProxyRsp) (action gnet.Action) {
	response, err := h.codec.Encode(proxyRsp, h.isLittleEnd)
	if err != nil {
		h.logger.Error("onTraffic Marshal", zap.Error(err), zap.String("proxyRsp", proxyRsp.String()))
		return gnet.Close
	}
	err = send(c, response)
	if err != nil {
		return gnet.Close
	}
	return gnet.None
}

// OnBoot implements gnet.EventHandler.
func (*ServerHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	return gnet.None
}

// OnClose implements gnet.EventHandler.
func (*ServerHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	return gnet.None
}

// OnOpen implements gnet.EventHandler.
func (*ServerHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	return nil, gnet.None
}

// OnShutdown implements gnet.EventHandler.
func (*ServerHandler) OnShutdown(eng gnet.Engine) {
}

// OnTick implements gnet.EventHandler.
func (*ServerHandler) OnTick() (delay time.Duration, action gnet.Action) {
	return time.Second, gnet.None
}

// OnTraffic implements gnet.EventHandler.

func (h *ServerHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	messages, err := codec.TryGetMessage(c, h.isLittleEnd)
	for _, oneMessage := range messages {
		if err != nil {
			h.logger.Error("onTraffic get message failed", zap.Error(err))
			return gnet.Close
		}
		msg, err := h.codec.Decode(oneMessage)
		if err != nil {
			h.logger.Error("OnTraffic", zap.Error(err))
			return gnet.Close
		}

		switch msg.Type {
		case message.Reqtype_WEBSOCKET:
			var conn *WebSocketConn
			ctx := c.Context()
			if ctx != nil {
				c := ctx.(context.Context)
				conn = c.Value(connKey("conn")).(*WebSocketConn)
			} else {
				conn = NewWebSocketConn(c, h.codec, h.isLittleEnd)
				ctx := context.Background()
				ctx = context.WithValue(ctx, connKey("conn"), conn)
				c.SetContext(ctx)
			}
			resp, err := conn.OnMessage(context.Background(), msg.GetMessage().GetWebsocketStartReq())
			proxyRsp := &message.ProxyRsp{
				Message: new(message.Message),
			}
			proxyRsp.Seq = msg.GetSeq()
			proxyRsp.Type = message.Reqtype_WEBSOCKET
			proxyRsp.Message.Body = &message.Message_WebsocketStartRsp{WebsocketStartRsp: resp}
			if err != nil {
				proxyRsp.Error = err.Error()
			}
			action = h.response(c, proxyRsp)
			if action != gnet.None {
				return action
			}

		case message.Reqtype_WEBSOCKETWRITE:
			ctx := c.Context()
			if ctx == nil {
				h.logger.Error("onTraffic write too early", zap.String("addr", c.RemoteAddr().String()))
				proxyRsp := message.ProxyRsp{
					Message: new(message.Message),
				}
				proxyRsp.Error = "write too early"
				action = h.response(c, &proxyRsp)
				if action != gnet.None {
					return action
				}
			}
			ctt := ctx.(context.Context)
			conn := ctt.Value(connKey("conn"))
			if conn == nil {
				h.logger.Error("onTraffic no conn", zap.String("addr", c.RemoteAddr().String()))
				proxyRsp := message.ProxyRsp{
					Message: new(message.Message),
				}
				proxyRsp.Error = "no conn"
				action = h.response(c, &proxyRsp)
				if action != gnet.None {
					return action
				}
			}
			websocketConn := conn.(*WebSocketConn)
			resp, err := websocketConn.WriteMessage(context.Background(), msg.GetMessage().GetWebsocketWriteReq())
			proxyRsp := &message.ProxyRsp{
				Message: new(message.Message),
			}
			proxyRsp.Seq = msg.GetSeq()
			proxyRsp.Type = message.Reqtype_WEBSOCKETWRITE
			proxyRsp.Message.Body = &message.Message_WebsocketWriteRsp{WebsocketWriteRsp: resp}
			if err != nil {
				proxyRsp.Error = err.Error()
			}
			action = h.response(c, proxyRsp)
			if action != gnet.None {
				return action
			}
		case message.Reqtype_HEARTBEAT:
			action := h.response(c, &message.ProxyRsp{
				Type:    msg.GetType(),
				Seq:     msg.GetSeq(),
				Message: msg.GetMessage(),
			})
			if action != gnet.None {
				return action
			}
		}
	}
	return gnet.None
}

func Start() {
	err := gnet.Run(new(ServerHandler), ":9000", gnet.WithTicker(true), gnet.WithLockOSThread(true),
		gnet.WithMulticore(true), gnet.WithTCPKeepAlive(time.Minute*1), gnet.WithTCPNoDelay(gnet.TCPNoDelay))
	if err != nil {
		panic(err)
	}
}

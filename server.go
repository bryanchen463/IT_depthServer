package itdepthserver

import (
	"context"
	"time"

	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/bryanchen463/IT_depthServer/logger"
	"github.com/bryanchen463/IT_depthServer/proxy"
	"github.com/panjf2000/gnet/v2"
)

type ServerHandler struct {
	logger logger.Logger
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
func (s *ServerHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	msg, err := proxy.RecvMsg(c)
	if err != nil {
		return gnet.Close
	}
	ctx := c.Context()
	var wsConn *WebSocketConn
	if ctx == nil {
		wsConn = NewWebSocketConn(c, codec.NewDefaultCodec(true), true)
		c.SetContext(wsConn)
	} else {
		wsConn = ctx.(*WebSocketConn)
	}
	_, err = wsConn.OnMessage(context.Background(), c, msg)
	if err != nil {
		s.logger.Error("OnMessage", err)
		return gnet.Close
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

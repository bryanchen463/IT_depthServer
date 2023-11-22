package itdepthserver

import (
	"time"

	"github.com/panjf2000/gnet/v2"
)

type ServerHandler struct {
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
func (*ServerHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	return gnet.None
}

func Start() {
	err := gnet.Run(new(ServerHandler), ":9000", gnet.WithTicker(true), gnet.WithLockOSThread(true),
		gnet.WithMulticore(true), gnet.WithTCPKeepAlive(time.Minute*1), gnet.WithTCPNoDelay(gnet.TCPNoDelay), gnet.WithTCPKeepAlive(time.Second*5))
	if err != nil {
		panic(err)
	}
}

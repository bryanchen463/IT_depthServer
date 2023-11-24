package itdepthserver

import (
	"context"
	"errors"
	"net"
	"sync"

	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/bryanchen463/IT_depthServer/handler"
	pub "github.com/bryanchen463/IT_depthServer/message"
	"github.com/bryanchen463/IT_depthServer/proxy"
	"github.com/panjf2000/gnet/v2"
	"github.com/xh23123/IT_hftcommon/pkg/common"
	"google.golang.org/protobuf/proto"
)

var pubReqPool = sync.Pool{
	New: func() interface{} {
		return &pub.Req{}
	},
}

type noCopy struct{}

func (n *noCopy) Lock()   {}
func (n *noCopy) Unlock() {}

type Conn interface {
	OnMessage(ctx context.Context, conn gnet.Conn, data []byte) error
	Shutdown() error
}

type WebSocketConn struct {
	noCopy
	conn           *proxy.Conn
	isConnect      bool
	codec          codec.CodecInterface
	isLittleEndian bool
}

func NewWebSocketConn(conn gnet.Conn, codec codec.CodecInterface, isLittleEndian bool) *WebSocketConn {
	return &WebSocketConn{
		conn: &proxy.Conn{
			C: conn,
		},
		codec:          codec,
		isLittleEndian: isLittleEndian,
	}
}

func (c *WebSocketConn) OnMessage(ctx context.Context, conn net.Conn, data []byte) ([]byte, error) {
	req := pubReqPool.Get().(*pub.Req)
	defer pubReqPool.Put(req)
	req.Reset()
	err := proto.Unmarshal(data, req)
	if err != nil {
		return nil, err
	}
	if subReq := req.GetSub(); subReq != nil {
		handler := handler.GetHandler(common.ExchangeID(subReq.Exchange))
		if handler == nil {
			return []byte(""), errors.New("handler not found")
		}
		err := handler.OnSubscribeMessage(conn, subReq)
		if err != nil {
			return []byte(""), err
		}
	} else if unsubReq := req.GetUnsub(); unsubReq != nil {
		handler := handler.GetHandler(common.ExchangeID(unsubReq.Exchange))
		if handler == nil {
			return []byte(""), errors.New("handler not found")
		}
		err := handler.OnUnSubscribeMessage(conn, unsubReq)
		if err != nil {
			return []byte(""), err
		}
	} else if proxyReq := req.GetHeartbeat(); proxyReq != nil {
		return []byte(""), nil
	}
	return []byte(""), errors.New("req type not found")
}

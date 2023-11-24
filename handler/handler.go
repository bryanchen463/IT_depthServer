package handler

import (
	"errors"
	"net"
	"sync"

	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/bryanchen463/IT_depthServer/logger"
	"github.com/bryanchen463/IT_depthServer/proxy"
	"github.com/bryanchen463/IT_depthServer/topic"
	"github.com/gorilla/websocket"
	"github.com/xh23123/IT_hftcommon/pkg/common"
	"go.uber.org/zap"
)

type MessgeHandler interface {
	OnWsMessage(conn net.Conn, messageType int, message []byte) error
	OnMessage(conn net.Conn, msg []byte, url string, exchange common.ExchangeID) error
}

type SubscribeInfo struct {
	Data sync.Map
}

var Handlers = sync.Map{}

func GetHandler(exchangeID common.ExchangeID) MessgeHandler {
	if handler, ok := Handlers.Load(exchangeID); ok {
		return handler.(MessgeHandler)
	}
	return nil
}

type ExchangeConn struct {
	Conn     *proxy.WebsocketConn
	Exchange topic.Exchange
}

var _ MessgeHandler = (*DefaultHandler)(nil)

type DefaultHandler struct {
	logger     logger.Logger
	ExchangeID common.ExchangeID
	codec      codec.CodecInterface
	wsConns    sync.Map
}

// OnMessage implements MessgeHandler.
func (*DefaultHandler) OnMessage(conn net.Conn, msg []byte, url string, exchange common.ExchangeID) error {
	panic("unimplemented")
}

// OnWsMessage implements MessgeHandler.
func (*DefaultHandler) OnWsMessage(conn net.Conn, messageType int, message []byte) error {
	panic("unimplemented")
}

func NewDefaultHandler(exchangeID common.ExchangeID, codec codec.CodecInterface) *DefaultHandler {
	res := &DefaultHandler{
		ExchangeID: exchangeID,
		codec:      codec,
		logger:     common.Logger,
	}
	return res
}

func (h *DefaultHandler) getWsConn(url string, exchang string, needCreate bool) (*ExchangeConn, bool, error) {
	if c, ok := h.wsConns.Load(url); ok {
		return c.(*ExchangeConn), false, nil
	}
	if !needCreate {
		return nil, false, errors.New("websocket connection not exist")
	}
	c, err := proxy.WebsocketStartRequest(url, exchang)
	if err != nil {
		return nil, false, err
	}
	websocketConn := c.(*proxy.WebsocketConn)
	exchangeConn := &ExchangeConn{Conn: websocketConn, Exchange: topic.NewExchangeBybit()}
	h.wsConns.Store(url, exchangeConn)
	return exchangeConn, true, nil
}

func (c *DefaultHandler) Stop() {
	c.wsConns.Range(func(key, value any) bool {
		value.(*proxy.WebsocketConn).Close()
		return true
	})
}

func (c *DefaultHandler) Forward(wsConn *proxy.WebsocketConn, conn net.Conn, fn func(messageType uint32, message []byte) error) {
	var err error
	defer func() {
		wsConn.Close()
	}()
	wsConn.SetPongHandler(func(appData string) error {
		return fn(websocket.PongMessage, []byte(appData))
	})

	wsConn.SetPingHandler(func(appData string) error {
		return fn(websocket.PongMessage, []byte(appData))
	})
	for {
		var messageType int
		var msg []byte
		messageType, msg, err = wsConn.ReadMsg()
		if err != nil {
			c.logger.Error("Forward failed", zap.Error(err))
			return
		}
		c.logger.Info("Forward", zap.String("message", string(msg)), zap.Int("type", messageType))
		err = fn(uint32(messageType), msg)
		if err != nil {
			c.logger.Error("Forward Write failed", zap.Error(err))
			return
		}
	}
}

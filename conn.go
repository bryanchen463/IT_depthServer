package itdepthserver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/bryanchen463/IT_depthServer/handler"
	"github.com/bryanchen463/IT_depthServer/proxy"
	"github.com/panjf2000/gnet/v2"
	"github.com/valyala/fasthttp"
	"github.com/xh23123/IT_hftcommon/pkg/common"
	"github.com/xh23123/IT_hftcommon/pkg/crexServer/codec/message"
	"github.com/xh23123/IT_hftcommon/pkg/crexServer/logger"
	"go.uber.org/zap"
)

type noCopy struct{}

func (n *noCopy) Lock()   {}
func (n *noCopy) Unlock() {}

type Conn interface {
	OnMessage(ctx context.Context, conn gnet.Conn, data []byte) error
	Shutdown() error
}

const (
	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096
)

var proxyRspPoll = sync.Pool{
	New: func() any {
		return &message.ProxyRsp{
			Message: new(message.Message),
		}
	},
}

var websocketPushPool = sync.Pool{
	New: func() any {
		return &message.WebsocketPushMessage{}
	},
}

var HttpClient = &fasthttp.Client{
	NoDefaultUserAgentHeader: true, // Don't send: User-Agent: fasthttp
	MaxConnsPerHost:          100,
	ReadBufferSize:           defaultReadBufferSize,  // Make sure to set this big enough that your whole request can be read at once.
	WriteBufferSize:          defaultWriteBufferSize, // Same but for your response.
	ReadTimeout:              8 * time.Second,
	WriteTimeout:             8 * time.Second,
	MaxIdleConnDuration:      30 * time.Second,

	DisableHeaderNamesNormalizing: true, // If you set the case on your headers correctly you can enable this.
}

type HttpConn struct {
	noCopy
	c          gnet.Conn
	codec      codec.CodecInterface
	httpClient *fasthttp.Client
}

func NewHttpConn(c gnet.Conn, codec codec.CodecInterface) *HttpConn {
	return &HttpConn{
		c:          c,
		httpClient: &fasthttp.Client{},
		codec:      codec,
	}
}

func handleHttpReq(ctx context.Context, httpRequest *message.HttpReq) (msgResp *message.HttpRsp, err error) {
	request := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(request)
	for _, headerPair := range httpRequest.GetHeader().GetHeaderPair() {
		for _, value := range headerPair.Value {
			request.Header.Add(headerPair.Key, value)
		}
	}
	request.AppendBody([]byte(httpRequest.GetBody()))
	request.SetRequestURI(httpRequest.GetUrl())
	request.Header.SetMethod(httpRequest.Method)

	if err != nil {
		return nil, err
	}
	httpResp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(httpResp)
	err = HttpClient.DoTimeout(request, httpResp, time.Second*2)
	msgResp = new(message.HttpRsp)
	if err != nil {
		msgResp.Error = err.Error()
		return nil, err
	}
	msgResp.Resp = make([]byte, 0, len(httpResp.Body()))
	msgResp.Resp = append(msgResp.Resp, httpResp.Body()...)
	msgResp.StatusCode = uint32(httpResp.StatusCode())

	return
}

func (c *HttpConn) OnMessage(ctx context.Context, req *message.HttpReq) (*message.HttpRsp, error) {
	return handleHttpReq(ctx, req)
}

func (c *HttpConn) Shutdown() error {

	return nil
}

type WebSocketConn struct {
	noCopy
	conn           *proxy.Conn
	isConnect      bool
	codec          codec.CodecInterface
	isLittleEndian bool
	Exchange       common.ExchangeID
	Url            string
}

func NewWebSocketConn(conn gnet.Conn, codec codec.CodecInterface, isLittleEndian bool) *WebSocketConn {
	return &WebSocketConn{
		conn:           proxy.NewConn(conn, codec),
		codec:          codec,
		isLittleEndian: isLittleEndian,
	}
}

func getExchangId(url string) common.ExchangeID {
	if strings.Count(url, "bybit") > 0 {
		return common.BYBIT
	}
	return ""
}

func (c *WebSocketConn) OnMessage(ctx context.Context, websocketStartReq *message.WebsocketStartReq) (*message.WebsocketStartRsp, error) {
	exchangeId := getExchangId(websocketStartReq.Url)
	if exchangeId == "" {
		return nil, errors.New("exchange not support")
	}
	c.Exchange = exchangeId
	c.Url = websocketStartReq.Url
	// websocketConn, err := proxy.WebsocketStartRequest(websocketStartReq.Url, exchangeId)
	// if err != nil {
	// 	return nil, err
	// }
	// c.wsConn = websocketConn.(*proxy.WebsocketConn)
	c.isConnect = true
	var websocketStartResp message.WebsocketStartRsp
	// go c.Forward()
	return &websocketStartResp, nil
}

func clear(proxyRsp *message.ProxyRsp) {
	proxyRsp.Code = 0
	proxyRsp.Type = 0
	proxyRsp.Error = ""
	proxyRsp.Seq = 0
}

func (c *WebSocketConn) forwardWsPushMessage(content []byte, messageType uint32) error {
	proxyRsp := proxyRspPoll.Get().(*message.ProxyRsp)
	defer proxyRspPoll.Put(proxyRsp)
	clear(proxyRsp)
	proxyRsp.Type = message.Reqtype_WEBSOCKETPUSH
	messagePush := websocketPushPool.Get().(*message.WebsocketPushMessage)
	defer websocketPushPool.Put(messagePush)
	messagePush.Reset()
	messagePush.MessageType = messageType
	messagePush.Message = content

	proxyRsp.Message.Body = &message.Message_WebsocketPushMessage{WebsocketPushMessage: messagePush}
	rsp, err := c.codec.Encode(proxyRsp)
	if err != nil {
		logger.Error("Encode", zap.Error(err))
		return err
	}
	err = send(c.conn, rsp)
	if err != nil {
		logger.Error("send", zap.Error(err))
		return err
	}
	return nil
}

func (c *WebSocketConn) WriteMessage(ctx context.Context, WebsocketWriteReq *message.WebsocketWriteReq) (*message.WebsocketWriteRsp, error) {
	var websocketWriteRsp message.WebsocketWriteRsp
	handler := handler.GetHandler(c.Exchange)
	err := handler.OnMessage(c.conn, WebsocketWriteReq.Message, c.Url, c.Exchange)
	if err != nil {
		return nil, err
	}
	return &websocketWriteRsp, nil
}

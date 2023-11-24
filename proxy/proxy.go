package proxy

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xh23123/IT_hftcommon/pkg/crexServer/codec/message"
	"google.golang.org/protobuf/proto"
)

var (
	proxyReqPool = sync.Pool{
		New: func() any {
			return &message.ProxyReq{
				Message: new(message.Message),
			}
		},
	}
	httpReqPool = sync.Pool{
		New: func() any {
			return new(message.HttpReq)
		},
	}
	webSocketStartPool = sync.Pool{
		New: func() any {
			return new(message.WebsocketStartReq)
		},
	}
	websocketWritePool = sync.Pool{
		New: func() any {
			return new(message.WebsocketWriteReq)
		},
	}
	proxyRspPool = sync.Pool{
		New: func() any {
			return new(message.ProxyRsp)
		},
	}
)

var seq atomic.Uint32

func genSeq() uint32 {
	d := seq.Add(1)
	return d
}

var isLittleEnd = true

func RecvMsg(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	n, err := conn.Read(header)
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return header, errHeaderNotEnough
	}
	dataLen := 0
	if isLittleEnd {
		dataLen = int(binary.LittleEndian.Uint32(header))
	} else {
		dataLen = int(binary.BigEndian.Uint32(header))
	}

	resp := make([]byte, dataLen)
	left := dataLen
	for left > 0 {
		n, err = conn.Read(resp[dataLen-left:])
		if err != nil {
			return nil, err
		}
		left -= n
	}
	return resp, nil
}

func HttpRequest(request *http.Request, exchange string) ([]byte, error) {
	conn, recycleFn, err := getConn(exchange)
	if err != nil {
		return nil, err
	}
	defer recycleFn()
	httpReq := httpReqPool.Get().(*message.HttpReq)
	httpReq.Reset()
	defer httpReqPool.Put(httpReq)
	httpReq.Url = request.URL.String()
	httpReq.Method = request.Method
	httpReq.Header = new(message.Header)
	for key, value := range request.Header {
		httpReq.Header.HeaderPair = append(httpReq.Header.HeaderPair, &message.HeaderPair{Key: key, Value: value})
	}
	if request.Body != nil {
		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		httpReq.Body = body
	}
	proxyReq := proxyReqPool.Get().(*message.ProxyReq)
	defer proxyReqPool.Put(proxyReq)
	proxyReq.Type = message.Reqtype_HTTP
	proxyReq.Seq = genSeq()
	proxyReq.Message.Body = &message.Message_HttpReq{HttpReq: httpReq}

	data, err := conn.(*Conn).Codec.Encode(proxyReq)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write(data)
	if err != nil {
		return nil, err
	}
	proxyRsp := proxyRspPool.Get().(*message.ProxyRsp)
	defer proxyRspPool.Put(proxyRsp)
	proxyRsp.Reset()
	resp, err := RecvMsg(conn)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(resp, proxyRsp)
	if err != nil {
		return nil, err
	}
	httpRsp := proxyRsp.GetMessage().GetHttpRsp()
	httpData := make([]byte, len(httpRsp.GetResp()))
	copy(httpData, httpRsp.GetResp())
	return httpData, nil
}

func WebsocketStartRequest(url string, exchange string) (conn net.Conn, err error) {
	crxAddr, err := getExchangeAddr(exchange)
	if err != nil {
		return nil, err
	}
	conn, err = NewWebSocketConn(crxAddr, time.Second*2)
	if err != nil {
		return nil, err
	}
	proxyReq := proxyReqPool.Get().(*message.ProxyReq)
	defer proxyReqPool.Put(proxyReq)
	proxyReq.Type = message.Reqtype_WEBSOCKET
	proxyReq.Seq = genSeq()

	websocketStartReq := webSocketStartPool.Get().(*message.WebsocketStartReq)
	defer webSocketStartPool.Put(websocketStartReq)
	websocketStartReq.Reset()
	websocketStartReq.Url = url

	proxyReq.Message.Body = &message.Message_WebsocketStartReq{WebsocketStartReq: websocketStartReq}
	data, err := conn.(*Conn).Codec.Encode(proxyReq)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write(data)
	if err != nil {
		return nil, err
	}

	resp, err := RecvMsg(conn)
	if err != nil {
		return nil, err
	}
	proxyRsp := proxyRspPool.Get().(*message.ProxyRsp)
	defer proxyRspPool.Put(proxyRsp)
	proxyRsp.Reset()
	err = proto.Unmarshal(resp, proxyRsp)
	if err != nil {
		return nil, err
	}
	if proxyRsp.Error != "" {
		return nil, errors.New(proxyRsp.Error)
	}

	return
}

func ReadWebsocketMessage(c net.Conn) (messageType int, data []byte, err error) {
	for {
		var msg []byte
		msg, err = RecvMsg(c)
		if err != nil {
			return
		}
		proxyRsp := proxyRspPool.Get().(*message.ProxyRsp)
		defer proxyRspPool.Put(proxyRsp)
		proxyRsp.Reset()
		err = proto.Unmarshal(msg, proxyRsp)
		if err != nil {
			return websocket.CloseAbnormalClosure, nil, err
		}
		if proxyRsp.Type != message.Reqtype_WEBSOCKETPUSH {
			continue
		}
		if proxyRsp.Error != "" {
			return websocket.CloseAbnormalClosure, nil, errors.New(proxyRsp.Error)
		}
		websocketPush := proxyRsp.GetMessage().GetWebsocketPushMessage()

		resp := make([]byte, len(websocketPush.GetMessage()))
		copy(resp, websocketPush.GetMessage())
		return int(websocketPush.GetMessageType()), resp, nil
	}
}

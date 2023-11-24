package proxy

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/gorilla/websocket"
	"github.com/xh23123/IT_hftcommon/pkg/common"
	"github.com/xh23123/IT_hftcommon/pkg/crexServer/codec/message"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var (
	errHeaderNotEnough = errors.New("header length is not enough")
	errDataTooShort    = errors.New("data length is too short")
)

var _ net.Conn = (*Conn)(nil)

type Conn struct {
	C            net.Conn
	lastUpdateId int64
	Codec        codec.CodecInterface
}

func Dial(addr string, timeout time.Duration) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	setNoDelay(c)
	conn := Conn{C: c}
	return &conn, nil
}

func NewConn(c net.Conn, codec codec.CodecInterface) *Conn {
	conn := Conn{C: c, Codec: codec}
	return &conn
}

func setNoDelay(conn net.Conn) error {
	switch conn := conn.(type) {
	case *net.TCPConn:
		var err error
		if err = conn.SetNoDelay(false); err != nil {
			return err
		}
		return err

	default:
		return fmt.Errorf("unknown connection type %T", conn)
	}
}

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

func clear(proxyRsp *message.ProxyRsp) {
	proxyRsp.Code = 0
	proxyRsp.Type = 0
	proxyRsp.Error = ""
	proxyRsp.Seq = 0
}

func (c *Conn) ForwardWsPushMessage(messageType uint32, content []byte) error {
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
	rsp, err := c.Codec.Encode(proxyRsp)
	if err != nil {
		common.Logger.Error("Encode", zap.Error(err))
		return err
	}
	_, err = c.Write(rsp)
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) Write(p []byte) (n int, err error) {
	left := len(p)
	total := len(p)
	for left > 0 {
		n, err := c.C.Write(p[total-left:])
		if err != nil {
			return 0, err
		}
		left = left - n
	}
	return total, nil
}

func (c *Conn) RecvMsg() ([]byte, error) {
	header := make([]byte, 4)
	n, err := c.Read(header)
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, errHeaderNotEnough
	}
	dataLen := 0
	if isLittleEnd {
		dataLen = int(binary.LittleEndian.Uint32(header))
	} else {
		dataLen = int(binary.BigEndian.Uint32(header))
	}
	resp := make([]byte, dataLen)
	n, err = c.Read(resp)
	if dataLen != n {
		common.Logger.Error("RecvMsg", zap.Int("datalen", dataLen), zap.Int("n", n))
		return nil, errDataTooShort
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (c *Conn) GetLastUpdateId() int64 {
	return c.lastUpdateId
}

func (c *Conn) SetLastUpdateId(updateID int64) {
	atomic.StoreInt64(&c.lastUpdateId, updateID)
}

func (c *Conn) LocalAddr() net.Addr {
	return c.C.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.C.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	return c.C.SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.C.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.C.SetWriteDeadline(t)
}

func (c *Conn) Read(buff []byte) (n int, err error) {
	return c.C.Read(buff)
}

func (c *Conn) Ping() error {
	proxyReq := message.ProxyReq{
		Type: message.Reqtype_HEARTBEAT,
		Seq:  genSeq(),
	}
	msg, err := c.Codec.Encode(&proxyReq)
	if err != nil {
		return err
	}
	_, err = c.Write(msg)
	return err
}

func (c *Conn) Close() error {
	return c.C.Close()
}

type WebsocketConn struct {
	Conn
	OnPong   func(msg string) error
	OnPing   func(msg string) error
	close    chan struct{}
	message  chan []byte
	once     sync.Once
	isClosed bool
}

func NewWebSocketConn(addr string, timeout time.Duration) (*WebsocketConn, error) {
	conn, err := Dial(addr, timeout)
	if err != nil {
		return nil, err
	}
	websocketConn := &WebsocketConn{
		Conn:    *conn,
		close:   make(chan struct{}),
		message: make(chan []byte, 1024),
	}
	go websocketConn.start()
	return websocketConn, nil
}

func (c *WebsocketConn) start() {
	for {
		select {
		case <-c.close:
			return
		case msg := <-c.message:
			_, err := c.Conn.Write(msg)
			if err != nil {
				common.Logger.Error("send", zap.Error(err))
				return
			}
		}
	}
}

func (c *WebsocketConn) Close() error {
	c.once.Do(func() {
		close(c.close)
		close(c.message)
		c.Conn.Close()
		c.isClosed = true
	})
	return nil
}

func (c *WebsocketConn) WriteAsync(msg []byte) (int, error) {
	if c.isClosed {
		return 0, errors.New("conn closed")
	}
	c.message <- msg
	return len(msg), nil
}

func (c *WebsocketConn) SetPongHandler(onPong func(msg string) error) {
	c.OnPong = onPong
}

func (c *WebsocketConn) SetPingHandler(onPing func(msg string) error) {
	c.OnPing = onPing
}

func (c *WebsocketConn) ReadMsg() (messageType int, data []byte, err error) {
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
		if websocketPush.GetMessageType() == websocket.PongMessage {
			if c.OnPong != nil {
				c.OnPong(string(websocketPush.GetMessage()))
			}
		} else if websocketPush.GetMessageType() == websocket.PingMessage {
			if c.OnPing != nil {
				c.OnPing(string(websocketPush.GetMessage()))
			} else {
				c.WriteControl(websocket.PongMessage, []byte(websocketPush.GetMessage()), time.Time{})
			}
		} else {
			resp := make([]byte, len(websocketPush.GetMessage()))
			copy(resp, websocketPush.GetMessage())
			return int(websocketPush.GetMessageType()), resp, nil
		}
	}
}

func (conn *WebsocketConn) WebsocketWriteRequest(messageType int, content []byte) (err error) {
	proxyReq := proxyReqPool.Get().(*message.ProxyReq)
	defer proxyReqPool.Put(proxyReq)
	proxyReq.Type = message.Reqtype_WEBSOCKETWRITE
	proxyReq.Seq = genSeq()

	websocketWriteReq := websocketWritePool.Get().(*message.WebsocketWriteReq)
	defer websocketWritePool.Put(websocketWriteReq)
	websocketWriteReq.Reset()
	websocketWriteReq.MessageType = uint32(messageType)
	websocketWriteReq.Message = []byte(content)

	proxyReq.Message.Body = &message.Message_WebsocketWriteReq{WebsocketWriteReq: websocketWriteReq}

	msg, err := conn.Codec.Encode(proxyReq)
	if err != nil {
		return err
	}
	_, err = conn.WriteAsync(msg)
	if err != nil {
		return err
	}

	return
}

func (c *WebsocketConn) WriteControl(messageType int, content []byte, deadline time.Time) error {
	return c.WebsocketWriteRequest(messageType, content)
}

func (c *WebsocketConn) WriteJSON(message any) error {
	v, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return c.WebsocketWriteRequest(websocket.TextMessage, v)
}

package handler

import (
	"encoding/json"
	"errors"
	"net"
	"strings"

	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/bryanchen463/IT_depthServer/proxy"
	"github.com/bryanchen463/IT_depthServer/topic"
	"github.com/gorilla/websocket"
	"github.com/xh23123/IT_hftcommon/pkg/common"
)

type BybitSubscribeMsg struct {
	ReqId string   `json:"req_id"`
	Op    string   `json:"op"`
	Args  []string `json:"args"`
}

func init() {
	Handlers.Store(common.BYBIT, NewBybitHandler(common.BYBIT, codec.NewDefaultCodec(true)))
}

var _ MessgeHandler = (*BybitHandler)(nil)

type BybitHandler struct {
	DefaultHandler
}

func NewBybitHandler(exchangeID common.ExchangeID, codec codec.CodecInterface) *BybitHandler {
	return &BybitHandler{
		DefaultHandler: *NewDefaultHandler(exchangeID, codec),
	}
}

func (c *BybitHandler) OnMessage(conn net.Conn, msg []byte, url string, exchange common.ExchangeID) error {
	var bybitSubscribeMsg BybitSubscribeMsg
	err := json.Unmarshal(msg, &bybitSubscribeMsg)
	if err != nil {
		c.logger.Error("OnSubscribeMessage failed:", err, " msg:", string(msg))
		return err
	}
	if bybitSubscribeMsg.Op == "subscribe" {
		return c.OnSubscribeMessage(conn, &bybitSubscribeMsg, url, exchange)
	} else if bybitSubscribeMsg.Op == "unsubscribe" {
		return c.OnUnSubscribeMessage(conn, &bybitSubscribeMsg, url, exchange)
	}
	return nil
}

func (c *BybitHandler) onWsMessage(message []byte, messageType uint32, ex *topic.ExchangeBybit) error {
	if messageType == websocket.PingMessage {
	} else if messageType == websocket.PongMessage {
	}
	recvData := &topic.BybitWebsocketRecv{}
	err := json.Unmarshal(message, recvData)
	if err != nil {
		common.Logger.Sugar().Errorf("Bybit unmarshal recv data fail %s %s", string(message), err)
		return err
	}
	switch {
	case strings.HasPrefix(recvData.Topic, "orderbook."):
		c.handleOrderBook(recvData, ex)
	}
	topic := ex.ParseTopic(recvData.Topic)
	if topic == nil {
		return errors.New("ParseTopic failed")
	}
	for _, v := range ex.Topics {
		if v.IsContains(topic) {
			v.Notify(message)
		}
	}
	return nil
}

func (c *BybitHandler) handleOrderBook(recvData *topic.BybitWebsocketRecv, ex *topic.ExchangeBybit) {
	data := recvData.Data
	c.OnOrderBook(recvData.Topic, &data, ex)
}

func (c *BybitHandler) OnOrderBook(dataType string, data *topic.BybitOrderBookData, ex *topic.ExchangeBybit) error {
	var bids []topic.Book
	var asks []topic.Book
	for _, bid := range data.Bid {
		bids = append(bids, topic.Book{
			Price:  bid[0],
			Amount: bid[1],
		})
	}
	for _, ask := range data.Ask {
		asks = append(asks, topic.Book{
			Price:  ask[0],
			Amount: ask[1],
		})
	}
	ex.GetOrderBook().SetUpdateId(data.UpdateID)
	if dataType == "snapshot" {
		ex.GetOrderBook().PutSnapshot(data.Symbol, bids, asks, data.UpdateID)
	} else {
		ex.GetOrderBook().PutDelta(data.Symbol, bids, asks, data.UpdateID)
	}
	return nil
}

func (o *BybitHandler) GetOrderBook(symbol string, ex topic.ExchangeBybit) (string, error) {
	orderBook := ex.GetOrderBook().GetSymbolOrderBook(symbol)
	bids := make([][2]string, 0)
	asks := make([][2]string, 0)
	for _, bid := range orderBook.Bids {
		bids = append(bids, [2]string{bid.Price, bid.Amount})
	}
	for _, ask := range orderBook.Asks {
		asks = append(asks, [2]string{ask.Price, ask.Amount})
	}
	bybitOrderBook := topic.BybitOrderBookData{
		Symbol:   symbol,
		UpdateID: ex.GetOrderBook().UpdateID,
		Ask:      asks,
		Bid:      bids,
	}
	v, err := json.Marshal(bybitOrderBook)
	if err != nil {
		return "", err
	}
	return string(v), nil
}

func (h *BybitHandler) OnSubscribeMessage(conn net.Conn, bybitSubscribeMsg *BybitSubscribeMsg, url string, exchange common.ExchangeID) error {
	args := make([]string, len(bybitSubscribeMsg.Args))
	exchangeConn, isCreate, err := h.getWsConn(url, string(exchange), true)
	if err != nil {
		return err
	}
	if isCreate {
		h.Forward(exchangeConn.Conn, conn, func(messageType uint32, message []byte) error {
			return h.onWsMessage(message, messageType, exchangeConn.Exchange.(*topic.ExchangeBybit))
		})
	} else {
		for _, ch := range bybitSubscribeMsg.Args {
			create, err := exchangeConn.Exchange.(*topic.ExchangeBybit).AddConn(conn.(*proxy.Conn), ch)
			if err != nil {
				h.logger.Error("AddConn failed:", err)
				return err
			}
			if create {
				args = append(args, ch)
			}
		}
		exchangeConn.Exchange.(*topic.ExchangeBybit).AddConn(conn.(*proxy.Conn), "ping")
		exchangeConn.Exchange.(*topic.ExchangeBybit).AddConn(conn.(*proxy.Conn), "pong")
		bybitSubscribeMsg.Args = args
	}
	if len(args) == 0 {
		return nil
	}
	return exchangeConn.Conn.WriteJSON(bybitSubscribeMsg)
}

func (h *BybitHandler) OnUnSubscribeMessage(conn net.Conn, bybitSubscribeMsg *BybitSubscribeMsg, url string, exchange common.ExchangeID) error {
	args := make([]string, len(bybitSubscribeMsg.Args))
	if len(args) == 0 {
		return nil
	}
	bybitSubscribeMsg.Args = args
	websocketConn, _, err := h.getWsConn(url, string(exchange), false)
	if err != nil {
		h.logger.Error("getWsConn failed:", err)
		return err
	}
	for _, ch := range bybitSubscribeMsg.Args {
		t := websocketConn.Exchange.(*topic.ExchangeBybit).ParseTopic(ch)
		if t == nil {
			return errors.New("ParseTopic failed")
		}
		for _, v := range websocketConn.Exchange.(*topic.ExchangeBybit).Topics {
			if v.Equla(t) {
				v.RemoveConn(conn.(*proxy.Conn))
			}
			if v.GetConnCount() == 0 {
				args = append(args, v.GetTopic())
			}
		}
	}
	if len(args) == 0 {
		return nil
	}
	bybitSubscribeMsg.Args = args
	websocketConn.Conn.WriteJSON(bybitSubscribeMsg)
	return nil
}

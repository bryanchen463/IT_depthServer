package topic

import (
	"net"
	"time"
)

type BybitOrderBookData struct {
	Symbol   string      `json:"s"`
	Bid      [][2]string `json:"b"`
	Ask      [][2]string `json:"a"`
	UpdateID int64       `json:"u"`
	Sequence int64       `json:"seq"`
}

type BybitWebsocketRecv struct {
	// op result
	Success bool   `json:"success"`
	RetMsg  string `json:"ret_msg"`
	ConnID  string `json:"conn_id"`
	OP      string `json:"op"`
	// subscibe data
	Topic string             `json:"topic"`
	Ts    int64              `json:"ts"`
	Type  string             `json:"type"`
	Cs    int64              `json:"cs"`
	Data  BybitOrderBookData `json:"data"`
}

type ExchangeBybit struct {
	DefaultExchange
	Topic string
	Conn  []net.Conn
}

func NewExchangeBybit() *ExchangeBybit {
	return &ExchangeBybit{}
}

func (e *ExchangeBybit) GetExchangeName() string {
	return "bybit"
}

func (e *ExchangeBybit) GetSnapOrderBook(depth int) ([]*BybitWebsocketRecv, int, error) {
	snapOrderBook := make([]*BybitWebsocketRecv, len(e.orderBook.SymbolOrderBook.Items()))
	for _, v := range e.orderBook.SymbolOrderBook.Items() {
		orderBook := v.(*SymbolOrderBook)
		bids := make([][2]string, 0)
		asks := make([][2]string, 0)

		for _, bid := range orderBook.Bids {
			bids = append(bids, [2]string{bid.Price, bid.Amount})
		}
		for _, ask := range orderBook.Asks {
			asks = append(asks, [2]string{ask.Price, ask.Amount})
		}
		bybitOrderBook := BybitOrderBookData{
			Symbol:   orderBook.Symbol,
			UpdateID: e.orderBook.UpdateID,
			Ask:      asks,
			Bid:      bids,
		}

		bybitWebsocketRecv := BybitWebsocketRecv{
			Topic:   e.Topic,
			Data:    bybitOrderBook,
			Success: true,
			RetMsg:  "",
			ConnID:  "",
			OP:      "",
			Ts:      time.Now().UnixMilli(),
			Type:    "snapshot",
			Cs:      0,
		}
		snapOrderBook = append(snapOrderBook, &bybitWebsocketRecv)
	}
	return snapOrderBook, 0, nil
}

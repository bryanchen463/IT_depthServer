package topic

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bryanchen463/IT_depthServer/proxy"
	ringbuffer "github.com/bryanchen463/IT_depthServer/ringbuff"
	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/shopspring/decimal"
)

type Book struct {
	Price  string
	Amount string
}

type SymbolOrderBook struct {
	sync.RWMutex
	Symbol string
	Asks   []Book
	Bids   []Book
}

func (o *SymbolOrderBook) PutSnapshot(bids, asks []Book) {
	o.Lock()
	defer o.Unlock()
	o.Bids = append(o.Bids, bids...)
	o.Asks = append(o.Asks, asks...)
}

func (o *SymbolOrderBook) RemoveBook(books []Book, price string) []Book {
	for index, v := range books {
		if v.Price == price {
			books = append(books[:index], books[index+1:]...)
			break
		}
	}
	return books
}

func (o *SymbolOrderBook) AddBook(books []Book, book Book, isReverse bool) []Book {
	for index, v := range books {
		if isReverse {
			if v.Price > book.Price {
				continue
			}
		} else {
			if v.Price < book.Price {
				continue
			}
		}
		end := books[index+1:]
		books = append(books[:index], book)
		books = append(books, end...)
		break
	}
	return books
}

func (o *SymbolOrderBook) PutDelta(bids, asks []Book) error {
	o.Lock()
	defer o.Unlock()
	for _, bid := range bids {
		amount, err := decimal.NewFromString(bid.Amount)
		if err != nil {
			return err
		}
		if amount.IsZero() {
			o.Bids = o.RemoveBook(o.Bids, bid.Price)
		} else {
			o.AddBook(o.Bids, bid, true)
		}
	}
	for _, ask := range asks {
		amount, err := decimal.NewFromString(ask.Amount)
		if err != nil {
			return err
		}
		if amount.IsZero() {
			o.Asks = o.RemoveBook(o.Asks, ask.Price)
		} else {
			o.AddBook(o.Asks, ask, false)
		}
	}
	return nil
}

func NewSymbolOrderBook(symbol string) *SymbolOrderBook {
	return &SymbolOrderBook{
		Symbol: symbol,
	}
}

type OrderBook struct {
	UpdateID        int64
	SymbolOrderBook cmap.ConcurrentMap
}

func NewOrderBook() *OrderBook {
	return &OrderBook{
		SymbolOrderBook: cmap.New(),
	}
}

func (o *OrderBook) SetUpdateId(updateId int64) {
	atomic.StoreInt64(&o.UpdateID, updateId)
}

func (o *OrderBook) GetSymbolOrderBook(symbol string) *SymbolOrderBook {
	if _, ok := o.SymbolOrderBook.Get(symbol); !ok {
		o.SymbolOrderBook.Set(symbol, NewSymbolOrderBook(symbol))
	}
	v, _ := o.SymbolOrderBook.Get(symbol)
	return v.(*SymbolOrderBook)
}

func (o *OrderBook) PutSnapshot(symbol string, bids, asks []Book, updateID int64) {
	o.UpdateID = updateID
	orderBook := o.GetSymbolOrderBook(symbol)
	orderBook.PutSnapshot(bids, asks)
}

func (o *OrderBook) PutDelta(symbol string, bids, asks []Book, updateId int64) error {
	o.UpdateID = updateId
	orderBook := o.GetSymbolOrderBook(symbol)
	orderBook.PutDelta(bids, asks)
	return nil
}

type OrderBookTopicData struct {
	Symbol string
	Depth  int
}

func (o *OrderBookTopicData) IsContains(orderBookTopicData *OrderBookTopicData) bool {
	return o.Symbol == orderBookTopicData.Symbol && o.Depth >= orderBookTopicData.Depth
}

func (o *OrderBookTopicData) Equla(orderBookTopicData *OrderBookTopicData) bool {
	return o.Symbol == orderBookTopicData.Symbol && o.Depth == orderBookTopicData.Depth
}

var _ Topic = (*OrderBookTopic)(nil)

type OrderBookTopic struct {
	OrderBookTopicData
	DefaultTopic
	WebSocketRecvs *ringbuffer.RingBuffer[*BybitWebsocketRecv]
	Msg            chan *BybitWebsocketRecv
	Exchange       Exchange
}

func NewOrderBookTopic(exchange Exchange, symbol string, depth int) *OrderBookTopic {
	res := &OrderBookTopic{
		OrderBookTopicData: OrderBookTopicData{
			Symbol: symbol,
			Depth:  depth,
		},
		WebSocketRecvs: ringbuffer.New[*BybitWebsocketRecv](1000),
		Msg:            make(chan *BybitWebsocketRecv, 1000),
		Exchange:       exchange,
	}
	go res.Start()
	return res
}

func (o *OrderBookTopic) Start() {
	for {
		select {
		case <-o.stop:
			return
		case websocketRecv := <-o.Msg:
			o.WebSocketRecvs.Push(websocketRecv)
			errConn := make([]interface{}, 0)
			o.conns.Range(func(key, value interface{}) bool {
				conn := value.(*proxy.Conn)
				err := o.pushDelta(conn)
				if err != nil {
					errConn = append(errConn, key)
					return true
				}
				if conn.GetLastUpdateId() == 0 {
					o.pushSnap(conn)
				} else {
					o.pushDelta(conn)
				}
				return true
			})
			for _, conn := range errConn {
				o.conns.Delete(conn)
			}
		}
	}
}

func (o *OrderBookTopic) GetBuffedOrderBooks() []*BybitWebsocketRecv {
	return o.WebSocketRecvs.FetchAll()
}

func (o *OrderBookTopic) IsContains(topic Topic) bool {
	orderBookTopicData, ok := topic.(*OrderBookTopic)
	if !ok {
		return false
	}
	return o.OrderBookTopicData.IsContains(&orderBookTopicData.OrderBookTopicData)
}

func (o *OrderBookTopic) Equla(topic Topic) bool {
	orderBookTopicData, ok := topic.(*OrderBookTopic)
	if !ok {
		return false
	}
	return o.OrderBookTopicData.Equla(&orderBookTopicData.OrderBookTopicData)
}

func (o *OrderBookTopic) push(conn *proxy.Conn, orderBookData *BybitWebsocketRecv) error {
	if orderBookData.Data.UpdateID <= conn.GetLastUpdateId() {
		return nil
	}
	v, err := sonic.Marshal(orderBookData)
	if err != nil {
		return err
	}
	err = conn.ForwardWsPushMessage(websocket.BinaryMessage, v)
	if err != nil {
		return err
	}
	conn.SetLastUpdateId(orderBookData.Data.UpdateID)
	return err
}

func (o *OrderBookTopic) pushSnap(conn *proxy.Conn) error {
	snapOrderBooks, _, err := o.Exchange.GetSnapOrderBook(o.Depth)
	if err != nil {
		return err
	}

	for _, snapOrderBook := range snapOrderBooks {
		o.push(conn, snapOrderBook)
	}
	return nil
}

func (o *OrderBookTopic) pushDelta(conn *proxy.Conn) error {
	for _, orderBook := range o.GetBuffedOrderBooks() {
		if orderBook.Data.UpdateID <= conn.GetLastUpdateId() {
			continue
		}
		err := o.push(conn, orderBook)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *OrderBookTopic) AddConn(conn *proxy.Conn) error {
	err := o.pushSnap(conn)
	if err != nil {
		return err
	}
	o.conns.Store(conn, conn)
	return nil
}

func (o *OrderBookTopic) GetTopic() string {
	return fmt.Sprintf("orderBook.%d.%s", o.Depth, o.Symbol)
}

func (o *OrderBookTopic) Notify(msg []byte) {
	websocketRecv := &BybitWebsocketRecv{}
	err := sonic.Unmarshal(msg, websocketRecv)
	if err != nil {
		return
	}
	o.Msg <- websocketRecv
}

var _ Topic = (*DefaultTopic)(nil)

type DefaultTopic struct {
	Topic string
	conns sync.Map
	Msg   chan []byte
	stop  chan struct{}
}

func NewDefaultTopic(topic string) *DefaultTopic {
	res := &DefaultTopic{
		Topic: topic,
		Msg:   make(chan []byte, 1000),
	}
	go res.Start()
	return res
}

func (d *DefaultTopic) Start() {
	for {
		select {
		case <-d.stop:
			return
		case msg := <-d.Msg:
			errConn := make([]interface{}, 0)
			d.conns.Range(func(key, value interface{}) bool {
				conn := value.(*proxy.Conn)
				err := conn.ForwardWsPushMessage(websocket.BinaryMessage, []byte(msg))
				if err != nil {
					errConn = append(errConn, key)
				}
				return true
			})
			for _, conn := range errConn {
				d.conns.Delete(conn)
			}
		}
	}
}

func (d *DefaultTopic) AddConn(conn *proxy.Conn) error {
	d.conns.Store(conn, conn)
	return nil
}

func (d *DefaultTopic) RemoveConn(conn *proxy.Conn) error {
	d.conns.Delete(conn)
	return nil
}

func (d *DefaultTopic) GetTopic() string {
	return d.Topic
}

func (d *DefaultTopic) Notify(msg []byte) {
	d.Msg <- msg
}

func (d *DefaultTopic) IsContains(topic Topic) bool {
	return d.Topic == topic.GetTopic()
}

func (d *DefaultTopic) Equla(topic Topic) bool {
	return d.Topic == topic.GetTopic()
}

func (d *DefaultTopic) GetConnCount() int {
	count := 0
	d.conns.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

type Topic interface {
	Start()
	AddConn(conn *proxy.Conn) error
	RemoveConn(conn *proxy.Conn) error
	GetTopic() string
	Notify([]byte)
	IsContains(topic Topic) bool
	Equla(topic Topic) bool
	GetConnCount() int
}

type TopicManager struct {
	Topics sync.Map
}

type Exchange interface {
	GetSnapOrderBook(depth int) ([]*BybitWebsocketRecv, int, error)
	GetOrderBook() *OrderBook
	GetExchangeName() string
	RemoveConn(conn *proxy.Conn, topic string) error
	RemoveTopic(topic Topic) error
}

var _ Exchange = (*ExchangeBybit)(nil)

type DefaultExchange struct {
	orderBook *OrderBook
	Topics    []Topic
	PingTopic Topic
	PongTopic Topic
}

func (e *DefaultExchange) Start() {
	for _, topic := range e.Topics {
		go topic.Start()
	}
}

func (e *DefaultExchange) RemoveTopic(topic Topic) error {
	for index, t := range e.Topics {
		if t.Equla(topic) {
			e.Topics = append(e.Topics[:index], e.Topics[index+1:]...)
			break
		}
	}
	return nil
}

func (e *DefaultExchange) ParseTopic(topic string) Topic {
	channels := strings.Split(topic, ".")
	if channels[0] == "orderBook" {
		symbol := channels[2]
		depthStr := channels[1]
		depth, err := strconv.Atoi(depthStr)
		if err != nil {
			return nil
		}
		return NewOrderBookTopic(e, symbol, depth)
	} else {
		return NewDefaultTopic(topic)
	}
}

func (e *DefaultExchange) ParseOrderBookTopic(topic string) (*OrderBookTopicData, error) {
	channels := strings.Split(topic, ".")
	symbol := channels[2]
	depthStr := channels[1]
	depth, err := strconv.Atoi(depthStr)
	if err != nil {
		return nil, err
	}
	return &OrderBookTopicData{
		Symbol: symbol,
		Depth:  depth,
	}, nil
}

func (e *DefaultExchange) AddConn(conn *proxy.Conn, topic string) (bool, error) {
	if topic == "ping" {
		e.PingTopic = NewDefaultTopic(topic)
		e.PingTopic.AddConn(conn)
		return true, nil
	}
	if topic == "pong" {
		e.PongTopic = NewDefaultTopic(topic)
		e.PongTopic.AddConn(conn)
		return true, nil
	}

	t := e.ParseTopic(topic)
	if t == nil {
		return false, fmt.Errorf("ParseTopic failed")
	}
	isInsert := false
	for _, topic := range e.Topics {
		if topic.Equla(t) {
			err := topic.AddConn(conn)
			if err != nil {
				return false, err
			}
			isInsert = true
		}
	}
	if !isInsert {
		err := t.AddConn(conn)
		if err != nil {
			return false, err
		}
		e.Topics = append(e.Topics, t)
	}
	return !isInsert, nil
}

func (e *DefaultExchange) RemoveConn(conn *proxy.Conn, topic string) error {
	t := e.ParseTopic(topic)
	if t == nil {
		return fmt.Errorf("ParseTopic failed")
	}
	for _, topic := range e.Topics {
		if topic.Equla(t) {
			topic.RemoveConn(conn)
		}
	}
	return nil
}

func (e *DefaultExchange) GetExchangeName() string {
	return ""
}

func (e *DefaultExchange) GetOrderBook() *OrderBook {
	return e.orderBook
}

func (e *DefaultExchange) GetSnapOrderBook(depth int) ([]*BybitWebsocketRecv, int, error) {
	return nil, 0, nil
}

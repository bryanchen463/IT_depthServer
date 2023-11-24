package proxy

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/silenceper/pool"
)

var (
	ErrNotSupportExchange = errors.New("not support exchange")
)

var poolMap = NewPoolMap()

type PoolMap struct {
	sync.RWMutex
	pools map[string]pool.Pool
}

func NewPoolMap() *PoolMap {
	return &PoolMap{pools: make(map[string]pool.Pool)}
}

func init() {
	err := initConfig("proxy_conf.yaml")
	if err != nil {
		log.Println("init pool config, err:", err)
		return
	}
	for _, exchange := range Config.Exchanges {
		addresses := []string{}
		for _, address := range exchange.Addresses {
			addresses = append(addresses, address.IP+":"+strconv.Itoa(address.Port))
		}
		pool, err := createNewPool(addresses...)
		if err != nil {
			log.Println("init pool createNewPool", err)
			return
		}
		poolMap.pools[exchange.Name] = pool
	}
}

func createNewPool(addresses ...string) (pool.Pool, error) {
	factory := func() (interface{}, error) {
		index := rand.Intn(len(addresses))
		return Dial(addresses[index], time.Second*2)
	}

	//close Specify the method to close the connection
	close := func(v interface{}) error { return v.(*Conn).Close() }
	// ping := func(v interface{}) error { return v.(*Conn).Ping() }
	poolConfig := &pool.Config{
		InitialCap: 10,
		MaxIdle:    20,
		MaxCap:     30,
		Factory:    factory,
		Close:      close,
		// Ping:       ping,
		//The maximum idle time of the connection, the connection exceeding this time will be closed, which can avoid the problem of automatic failure when connecting to EOF when idle
		IdleTimeout: 15 * time.Second,
	}
	return pool.NewChannelPool(poolConfig)
}

func getConn(exchange string) (net.Conn, func(), error) {
	p, ok := poolMap.pools[exchange]
	if !ok {
		return nil, nil, ErrNotSupportExchange
	}
	v, err := p.Get()
	if err != nil {
		return nil, nil, err
	}
	return v.(net.Conn), func() {
		p.Put(v)
	}, nil
}

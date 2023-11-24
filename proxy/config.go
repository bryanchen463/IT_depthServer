package proxy

import (
	"errors"
	"math/rand"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

var (
	ErrNotAvailableAddress = errors.New("not available address")
)

var Config config

func initConfig(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, &Config)
}

type address struct {
	IP   string `yaml:"ip"`
	Port int    `yaml:"port"`
}

type exchange struct {
	Name      string    `yaml:"name"`
	Addresses []address `yaml:"addresses"`
}

type config struct {
	Exchanges []exchange `yaml:"exchanges"`
}

func getExchangeAddr(name string) (string, error) {
	for _, ex := range Config.Exchanges {
		if ex.Name == name {
			address := ex.Addresses[rand.Intn(len(ex.Addresses))]
			return address.IP + ":" + strconv.Itoa(address.Port), nil
		}
	}
	return "", ErrNotAvailableAddress
}

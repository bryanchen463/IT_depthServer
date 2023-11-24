package main

import (
	itdepthserver "github.com/bryanchen463/IT_depthServer"
	"github.com/bryanchen463/IT_depthServer/codec"
	"github.com/bryanchen463/IT_depthServer/handler"
	"github.com/xh23123/IT_hftcommon/pkg/common"
)

func init() {
	common.InitLogger("itdepthserver.log", "debug")
}

func main() {
	handler.Handlers.Store(common.BYBIT, handler.NewBybitHandler(common.BYBIT, codec.NewDefaultCodec(true)))
	itdepthserver.Start()
}

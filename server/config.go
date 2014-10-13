package main
import (
	"flag"
)

var (
	Conf *Config
)

func init() {
	Conf = &Config{RPCBind: "127.0.0.1:9527"}
	flag.StringVar(&Conf.RPCBind, "h", "127.0.0.1:9527", "rpc侦听地址")
}

type Config struct {
	RPCBind		string // rpc侦听地址和端口
}
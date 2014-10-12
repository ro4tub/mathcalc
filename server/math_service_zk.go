package main

import (
	log "code.google.com/p/log4go"
	common "github.com/ro4tub/mathcalc/common"
	myzk "github.com/ro4tub/mathcalc/common/zk"
	"github.com/samuel/go-zookeeper/zk"
	"time"
	"encoding/json"
)

// InitZK create zookeeper root path, and register a temp node.
func InitZK() (*zk.Conn, error) {
	conn, err := myzk.Connect([]string{"10.211.55.5:2181"}, 30*time.Second)
	if err != nil {
		log.Error("zk.Connect() error(%v)", err)
		return nil, err
	}
	if err = myzk.Create(conn, "/mathcalcservice"); err != nil {
		log.Error("zk.Create() error(%v)", err)
		return conn, err
	}
	nodeInfo := common.MessageNodeInfo{}
	nodeInfo.Rpc = []string{"127.0.0.1:9527"}
	nodeInfo.Weight = 1
	data, err := json.Marshal(nodeInfo)
	if err != nil {
		log.Error("json.Marshal(() error(%v)", err)
		return conn, err
	}
	log.Debug("zk data: \"%s\"", string(data))
	// tcp, websocket and rpc bind address store in the zk
	if err = myzk.RegisterTemp(conn, "/mathcalcservice", data); err != nil {
		log.Error("zk.RegisterTemp() error(%v)", err)
		return conn, err
	}
	return conn, nil
}
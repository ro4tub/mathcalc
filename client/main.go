package main

import (
	log "code.google.com/p/log4go"
	common "github.com/ro4tub/mathcalc/common"
	myzk "github.com/ro4tub/mathcalc/common/zk"
	"github.com/samuel/go-zookeeper/zk"
	"time"
)

// func Calc() {
//
// }

func InitZK() (*zk.Conn, error) {
	conn, err := myzk.Connect([]string{"10.211.55.5:2181"}, 30*time.Second)
	if err != nil {
		log.Error("myzk.Connect() error(%v)", err)
		return nil, err
	}
	// watch and update
	common.InitMessage(conn, "/mathcalcservice", time.Second, time.Second)
	return conn, nil
}

func main() {
	log.LoadConfiguration("log.xml")
	defer log.Close()
	
	// init zookeeper
	zkConn, err := InitZK()
	if err != nil {
		if zkConn != nil {
			zkConn.Close()
		}
		panic(err)
	}
	// 定时器执行
	// rpc调用
	for {
		select {
		case <-time.After(time.Second * 3):
			c := common.MessageRPC.Get()
			if c == nil {
				log.Error("c == nil")
				continue
			}
			req := &common.MathCalcReq{Arg1: 100, Arg2: 214, Opt:1}
			ack := &common.MathCalcAck{}
			if err := c.Call(common.MathServiceCalc, req, ack); err != nil {
				log.Error("%s rpc call error(%v)", common.MathServiceCalc, err)
				continue
			}
			log.Info("Calc Ret=%d", ack.Ret)
		}
	}
}
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
	// // TEST [[
	// client, err := rpc.Dial("tcp", "127.0.0.1:9527")
	// if err != nil {
	// 	log.Error("rpc.Dial error(%v)", err)
	// 	return
	// }
	// req := &common.MathCalcReq{Arg1: 100, Arg2: 214, Opt:1}
	// ack := &common.MathCalcAck{}
	// if err := client.Call(common.MathServiceCalc, req, ack); err != nil {
	// 	fmt.Printf("%s rpc call error(%v)\n", common.MathServiceCalc, err)
	// 	log.Error("%s rpc call error(%v)", common.MathServiceCalc, err)
	// 	return
	// }
	// log.Info("Calc Ret=%d", ack.Ret)
	// // ret := 0
	// // err = client.Call("MathRPC.Ping", 0, &ret)
	// // if err != nil {
	// // 	log.Error("ping error: %v", err)
	// // }
	// // ]]
	
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
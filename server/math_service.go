package main
import (
	log "code.google.com/p/log4go"
	"net"
	"net/rpc"
	common "github.com/ro4tub/mathcalc/common"
)
type MathRpc struct {
	
}

func (r *MathRpc) Calc(req *common.MathCalcReq, ack *common.MathCalcAck) error {
	if req == nil /*|| req.Player == nil*/ {
		return common.ErrParam
	}
	log.Debug("Calc: %d, %d, %d", req.Arg1, req.Arg2, req.Opt)
	ack.Ret = req.Arg1 + req.Arg2
	return nil
}

// Server Ping interface
func (r *MathRpc) Ping(p int, ret *int) error {
	log.Debug("ping ok")
	return nil
}


func InitRpc() error {
	math := &MathRpc{}
	rpc.Register(math)
	go rpcListen("0.0.0.0:9527")
	return nil
}

func rpcListen(remoteip string) {
	l, err := net.Listen("tcp", remoteip)
	if err != nil {
		log.Error("net.Listen failed: %s, %v", remoteip, err)
		panic(err)
	}
	defer func() {
		if err := l.Close(); err != nil {
			log.Error("Close failed %v", err)
		}
	}()
	rpc.Accept(l)
}


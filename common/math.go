package common

import (
	
)

const (
	// message rpc service
	MathService             = "MathRpc"
	MathServiceCalc   = "MathRpc.Calc"
)


type MathCalcReq struct {
	Arg1	int
	Arg2	int
	Opt		int // 1 加法 2 减法 3 乘法 4 除法
}

type MathCalcAck struct {
	Ret		int
}

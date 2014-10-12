package main

import (
	log "code.google.com/p/log4go"
)



func main() {
	log.LoadConfiguration("log.xml")
	defer log.Close()
	
	// start pprof http FIXME
	// perf.Init(Conf.PprofBind)
	if err := InitRpc(); err != nil {
		panic(err)
	}
	
	// init zookeeper
	zk, err := InitZK()
	if err != nil {
		if zk != nil {
			zk.Close()
		}
		panic(err)
	}
	sig := InitSignal()
	HandleSignal(sig)
}
package common
import (
	log "code.google.com/p/log4go"
	myzk "github.com/ro4tub/mathcalc/common/zk"
	"github.com/samuel/go-zookeeper/zk"
	"encoding/json"
	"net/rpc"
	"time"
	"path"
)


var (
	MessageRPC *RandLB
)

func init() {
	MessageRPC, _ = NewRandLB(map[string]*WeightRpc{}, MathService, 0, 0, false)
}

// watchMessageRoot watch the message root path.
func watchMessageRoot(conn *zk.Conn, fpath string, ch chan *MessageNodeEvent) error {
	for {
		nodes, watch, err := myzk.GetNodesW(conn, fpath)
		if err == myzk.ErrNodeNotExist {
			log.Warn("zk don't have node \"%s\", retry in %d second", fpath, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		} else if err == myzk.ErrNoChild {
			log.Warn("zk don't have any children in \"%s\", retry in %d second", fpath, waitNodeDelay)
			// all child died, kick all the nodes
			for _, client := range MessageRPC.Clients {
				log.Debug("node: \"%s\" send del node event", client.Addr)
				ch <- &MessageNodeEvent{Event: eventNodeDel, Key: &WeightRpc{Addr: client.Addr, Weight: client.Weight}}
			}
			time.Sleep(waitNodeDelaySecond)
			continue
		} else if err != nil {
			log.Error("getNodes error(%v), retry in %d second", err, waitNodeDelay)
			time.Sleep(waitNodeDelaySecond)
			continue
		}
		nodesMap := map[string]bool{}
		// handle new add nodes
		for _, node := range nodes {
			data, _, err := conn.Get(path.Join(fpath, node))
			if err != nil {
				log.Error("zk.Get(\"%s\") error(%v)", path.Join(fpath, node), err)
				continue
			}
			// parse message node info
			nodeInfo := &MessageNodeInfo{}
			if err := json.Unmarshal(data, nodeInfo); err != nil {
				log.Error("json.Unmarshal(\"%s\", nodeInfo) error(%v)", string(data), err)
				continue
			}
			for _, addr := range nodeInfo.Rpc {
				// if not exists in old map then trigger a add event
				if _, ok := MessageRPC.Clients[addr]; !ok {
					ch <- &MessageNodeEvent{Event: eventNodeAdd, Key: &WeightRpc{Addr: addr, Weight: nodeInfo.Weight}}
				}
				nodesMap[addr] = true
			}
		}
		// handle delete nodes
		for _, client := range MessageRPC.Clients {
			if _, ok := nodesMap[client.Addr]; !ok {
				ch <- &MessageNodeEvent{Event: eventNodeDel, Key: client}
			}
		}
		// blocking wait node changed
		event := <-watch
		log.Info("zk path: \"%s\" receive a event %v", fpath, event)
	}
}

// handleNodeEvent add and remove MessageRPC.Clients, copy the src map to a new map then replace the variable.
func handleMessageNodeEvent(conn *zk.Conn, retry, ping time.Duration, ch chan *MessageNodeEvent) {
	for {
		ev := <-ch
		// copy map from src
		tmpMessageRPCMap := make(map[string]*WeightRpc, len(MessageRPC.Clients))
		for k, v := range MessageRPC.Clients {
			tmpMessageRPCMap[k] = v
			// reuse rpc connection
			v.Client = nil
		}
		// handle event
		if ev.Event == eventNodeAdd {
			log.Info("add message rpc node: \"%s\"", ev.Key.Addr)
			rpcTmp, err := rpc.Dial("tcp", ev.Key.Addr)
			if err != nil {
				log.Error("rpc.Dial(\"tcp\", \"%s\") error(%v)", ev.Key, err)
				log.Warn("discard message rpc node: \"%s\", connect failed", ev.Key)
				continue
			}
			ev.Key.Client = rpcTmp
			tmpMessageRPCMap[ev.Key.Addr] = ev.Key
		} else if ev.Event == eventNodeDel {
			log.Info("del message rpc node: \"%s\"", ev.Key.Addr)
			delete(tmpMessageRPCMap, ev.Key.Addr)
		} else {
			log.Error("unknown node event: %d", ev.Event)
			panic("unknown node event")
		}
		tmpMessageRPC, err := NewRandLB(tmpMessageRPCMap, MathService, retry, ping, true)
		if err != nil {
			log.Error("NewRandLR() error(%v)", err)
			panic(err)
		}
		oldMessageRPC := MessageRPC
		// atomic update
		MessageRPC = tmpMessageRPC
		// release resource
		oldMessageRPC.Destroy()
		log.Debug("MessageRPC.Client length: %d", len(MessageRPC.Clients))
	}
}

// InitMessage init a rand lb rpc for message module.
func InitMessage(conn *zk.Conn, fpath string, retry, ping time.Duration) {
	// watch message path
	ch := make(chan *MessageNodeEvent, 1024)
	go handleMessageNodeEvent(conn, retry, ping, ch)
	go watchMessageRoot(conn, fpath, ch)
}
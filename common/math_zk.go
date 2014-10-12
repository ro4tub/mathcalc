package common

import (
)
type MessageNodeEvent struct {
	Key *WeightRpc
	// event type
	Event int
}

// Message node info
type MessageNodeInfo struct {
	Rpc    []string `json:"rpc"`
	Weight int      `json:"weight"`
}
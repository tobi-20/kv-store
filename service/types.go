package service

import (
	"net"
	"sync"
)

type ReplicationEvent struct {
	Seq   uint64
	Key   string
	Value string
}

type Replicator struct {
	seq       uint64
	mu        sync.RWMutex
	followers []*Follower
	eventCh   chan ReplicationEvent
}

type Follower struct {
	conn  net.Conn
	queue chan ReplicationEvent
}

type Replica interface {
	Send(ReplicationEvent) error
}

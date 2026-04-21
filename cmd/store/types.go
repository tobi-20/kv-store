package store

import (
	"net"
	"os"
	"sync"
)

type Store struct {
	memtable     map[string]string
	memtableSize int
	sstableCount int
	path         string
	wal          *os.File
	index        map[string][]SparseIndexEntry
	mu           sync.RWMutex
	Replicator   *Replicator
}
type Replicator struct {
	seq       uint64
	mu        sync.RWMutex
	followers []*Follower
	eventCh   chan ReplicationEvent
}
type SparseIndexEntry struct {
	offset int64
	key    string
	Op     Operation
}

type Replica interface {
	Send(ReplicationEvent) error
}
type ReplicationEvent struct {
	ID    string
	Seq   uint64
	Key   string
	Value string
	Op    Operation
}

type Follower struct {
	conn  net.Conn
	queue chan ReplicationEvent
}

type Operation uint8

const (
	OpSet Operation = iota
	OpDelete
)

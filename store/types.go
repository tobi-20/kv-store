package main

import (
	"os"
	"sync"
)

type SparseIndexEntry struct {
	key    string
	offset int64
}
type Store struct {
	memtable     map[string]string
	memtableSize int
	sstableCount int
	path         string
	wal          *os.File
	index        map[string][]SparseIndexEntry
	mu           sync.RWMutex
}

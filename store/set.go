package main

import (
	"log"
)

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//"When a write comes in, add it to an in-memory balanced tree data structure. This in-memory tree is sometimes called a memtable. When the memtable gets bigger than some threshold write it out to disk as an SSTable file."
	msg, err := encoder(key, value)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := s.wal.Write(msg); err != nil {
		log.Fatal(err)
	} // write to WAL on disk before writing to memtable in case of unexpected crash/restart
	s.memtable[key] = value // write to memtable

	s.memtableSize += len(value) + len(key)
	if s.memtableSize >= 4096 {
		s.flushMemtable()
	}

}

package store

import (
	"log"
	"net"
)

func StartLeaderServer(addr string, replicator *Replicator) {

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		follower := NewFollower(conn)

		replicator.mu.Lock()
		replicator.followers = append(replicator.followers, follower)
		replicator.mu.Unlock()
	}
}

func StartWriteServer(addr string, s *Store) {

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go handleWrite(conn, s)
	}
}

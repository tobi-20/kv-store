package store

import "net"

func NewFollower(conn net.Conn) *Follower {
	f := &Follower{
		conn:  conn,
		queue: make(chan ReplicationEvent, 100),
	}

	go f.writerLoop()
	return f
}

func StartFollower(addr string, store *Store) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}

			go HandleConn(conn, store)
		}
	}()

	return nil
}

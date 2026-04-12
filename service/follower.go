package service

import "net"

func NewFollower(conn net.Conn) *Follower {
	f := &Follower{
		conn:  conn,
		queue: make(chan ReplicationEvent, 100),
	}

	go f.writerLoop()
	return f
}

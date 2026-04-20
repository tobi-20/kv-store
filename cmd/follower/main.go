package main

import (
	"ddia/cmd/store"
	"log"
	"net"
)

func main() {

	s, err := store.NewStore("./data")
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	store.HandleConn(conn, s)
}

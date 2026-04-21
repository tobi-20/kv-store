package main

import (
	"ddia/cmd/store"
	"fmt"
	"log"
	"net"
)

func main() {
	

	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	for i := 1; i <= 10000; i++ {

		event := store.ReplicationEvent{
			ID:    "xyz",
			Seq:   uint64(i),
			Op:    store.OpSet,
			Key:   fmt.Sprintf("x%d", i),
			Value: fmt.Sprintf("%d", i*10),
		}


		if err := store.Encode(conn, event); err != nil {
			log.Fatal(err)
		}
	}
}

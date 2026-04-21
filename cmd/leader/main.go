package main

import (
	"ddia/cmd/store"
	"log"
)

func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	log.SetOutput(log.Writer())

	s, err := store.NewStore("./data")
	if err != nil {
		panic(err)
	}

	replicator, err := store.NewReplicator()
	if err != nil {
		panic(err)
	}
	s.SetReplicator(replicator)

	replicator.Start() // this initializes the channel receiver for the replicator

	go store.StartLeaderServer(":8081", replicator)
	store.StartWriteServer(":8080", s)
	select {}
}

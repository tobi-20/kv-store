package main

import (
	"ddia/cmd/store"
	"log"
)

func main() {

	s, err := store.NewStore("./data")
	if err != nil {
		log.Fatal(err)
	}

	replicator, err := store.NewReplicator()
	if err != nil {
		log.Fatal(err)
	}

	replicator.Start()

	s.SetReplicator(replicator)

	go store.StartLeaderServer(":8080", replicator)

	select {}
}

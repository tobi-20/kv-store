package main

import (
	"fmt"
	"log"
)

func main() {
	store, err := NewStore("store.txt")
	if err != nil {
		log.Fatal(err)
	}
	store.Set("when", "tomorrow")
	res, err := store.Get("when")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res)
}

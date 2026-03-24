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
	val, err := store.Get("crash_test")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(val)
}

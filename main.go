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
	store.Set("name", "tobiloba")
	v, err := store.Get("name")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(v)
	store.Compact()
}

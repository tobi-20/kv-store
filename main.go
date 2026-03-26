package main

import (
	"fmt"
	"log"
)

func main() {
	s, err := NewStore("store.txt")
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 2000; i++ {
		s.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	s.Compact()
}

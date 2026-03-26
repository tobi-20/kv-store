package main

import (
	"fmt"
	"log"
)

func main() {
	// initialize store
	s, err := NewStore("store.txt")
	if err != nil {
		log.Fatal(err)
	}

	// write enough to trigger multiple flushes and build sparse index
	for i := 0; i < 2000; i++ {
		s.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// get via sparse index
	tests := []string{"key0", "key500", "key999", "key1500", "key1999"}
	for _, k := range tests {
		val, err := s.Get(k)
		if err != nil {
			fmt.Printf("FAIL: %s, %v\n", k, err)
		} else {
			fmt.Printf("OK: %s, %s\n", k, val)
		}
	}

	s.Compact()
	fmt.Printf("SSTable count after compact: %d\n", s.sstableCount)

	for _, k := range tests {
		val, err := s.Get(k)
		if err != nil {
			fmt.Printf("FAIL: %s, %v\n", k, err)
		} else {
			fmt.Printf("OK: %s, %s\n", k, val)
		}
	}
}

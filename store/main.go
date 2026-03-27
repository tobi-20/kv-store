package main

import (
	"fmt"
	"log"
	"sync"
)

func main() {
	// initialize store
	s, err := NewStore("store.txt")
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup

	// 100 goroutines writing simultaneously
	for i := 0; i < 100; i++ {
		wg.Add(1) //increments the counter by 1
		go func(i int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}(i)
	}

	wg.Wait() // wait for all writes to finish

	// 100 goroutines reading simultaneously
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			val, err := s.Get(fmt.Sprintf("key%d", i))
			if err != nil {
				fmt.Printf("FAIL: key%d\n", i)
			} else {
				fmt.Printf("OK: key%d → %s\n", i, val)
			}
		}(i)
	}

	wg.Wait()
}

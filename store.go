package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

type Store struct {
	index map[string]string
	path  string
}

// load file from disk
func NewStore(path string) (*Store, error) {

	s := &Store{
		index: make(map[string]string),
		path:  path,
	}

	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return s, nil // no data yet, empty store
	}
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		a := strings.Split(line, ",")
		s.index[a[0]] = a[1]
	}

	return s, nil
}

func (s *Store) Set(key, value string) {

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(f, "%s,%s\n", key, value)
	s.index[key] = value
	f.Close()
	// t := compact()
	// fmt.Println(t)
}
func (s *Store) Get(key string) (string, error) {
	//When you want to look up a value, use the in-memory hash map to find the offset for the key, then seek to that location in the data file and read the value
	if s.index[key] == "" {
		return "", errors.New("key does not exist")
	}
	return s.index[key], nil
}
func (s *Store) Compact() error {
	d := s.index
	f, err := os.Create("compact.txt")
	if err != nil {
		return (err)
	}

	for i, v := range d {
		fmt.Fprintf(f, "%s,%s\n", i, v)
	}
	f.Close()                              // close before rename as windows does not allow this
	err = os.Rename("compact.txt", s.path) //compact.txt gets renamed to s.path, replacing the original store.txt file.
	if err != nil {
		return err
	}

	return nil
}

package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
)

type Store struct {
	index        map[string]string
	memtable     map[string]string
	memtableSize int
	sstableCount int
	path         string
}

// load file from disk
func NewStore(path string) (*Store, error) {

	s := &Store{
		index:    make(map[string]string),
		memtable: make(map[string]string),
		path:     path,
	}

	f, err := os.Open(s.path)
	if os.IsNotExist(err) {
		return s, nil // no data yet, empty store
	}
	if err != nil {
		log.Fatal(err)
	}

	defer f.Close()
	files, err := os.ReadDir(".")
	if err != nil {
		log.Println(err)
	}

	for _, v := range files {
		if strings.HasPrefix(v.Name(), "ssl_") {
			s.sstableCount++
		}
	}

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		a := strings.Split(line, ",")
		s.index[a[0]] = a[1]
	}

	return s, nil
}

func (s *Store) Set(key, value string) {

	//"When a write comes in, add it to an in-memory balanced tree data structure. This in-memory tree is sometimes called a memtable. When the memtable gets bigger than some threshold write it out to disk as an SSTable file."
	s.memtable[key] = value // write to memtable

	s.memtableSize += len(value) + len(key)
	if s.memtableSize >= 4096 {
		s.flushMemtable()
	}

}
func (s *Store) Get(key string) (string, error) {
	v := s.memtable[key]
	if v != "" {
		return v, nil
	}
	for i := s.sstableCount - 1; i >= 0; i-- {

		filePath := fmt.Sprintf("ssl_%d.txt", i)
		f, err := os.Open(filePath)
		if err != nil {
			return "", err

		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			a := strings.Split(line, ",")
			if key == a[0] {
				f.Close()
				return a[1], nil
			}
		}
		f.Close()
	}

	return "", errors.New("key not found")
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

func (s *Store) flushMemtable() {
	filepath := fmt.Sprintf("ssl_%d.txt", s.sstableCount)
	f, err := os.Create(filepath)
	s.sstableCount++
	if err != nil {
		log.Fatal(err)
	}

	a := make([]string, 0, 4097)
	for k := range s.memtable {
		a = append(a, k)
	}
	slices.SortFunc(a, func(a string, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}

		return 0
	})
	for _, k := range a {
		fmt.Fprintf(f, "%s,%s\n", k, s.memtable[k])
	}
	clear(s.memtable)
	s.memtableSize = 0

}

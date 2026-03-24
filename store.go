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
	memtable     map[string]string
	memtableSize int
	sstableCount int
	path         string
	wal          *os.File
}

// load file from disk
func NewStore(path string) (*Store, error) {

	s := &Store{

		memtable: make(map[string]string),
		path:     path,
	}
	wal, err := os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) //"... if the database crashes, the most recent writes (which are in the memtable but not yet written out to disk) are lost. In order to avoid that problem, we can keep a separate log on disk to which every write is immediately appended"
	if err != nil {
		return nil, err
	}

	s.wal = wal

	q, err := os.Open("wal.log")
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(".") //reads the named directory, returning all its directory entries sorted by filename.
	if err != nil {
		log.Println(err)
	}

	for _, v := range files {
		if strings.HasPrefix(v.Name(), "ssl_") {
			s.sstableCount++
		}
	}

	//read the wal log into the memtable in case of crash or sudden restart
	scanner := bufio.NewScanner(q)
	for scanner.Scan() {
		line := scanner.Text()
		a := strings.Split(line, ",")
		if len(a) != 2 {
			continue
		}
		s.memtable[a[0]] = a[1]
	}
	q.Close()

	return s, nil
}

func (s *Store) Set(key, value string) {

	//"When a write comes in, add it to an in-memory balanced tree data structure. This in-memory tree is sometimes called a memtable. When the memtable gets bigger than some threshold write it out to disk as an SSTable file."

	fmt.Fprintf(s.wal, "%s,%s\n", key, value) // write to WAL on disk before writing to memtable in case of unexpected crash/restart
	s.memtable[key] = value                   // write to memtable

	s.memtableSize += len(value) + len(key)
	if s.memtableSize >= 4096 {
		s.flushMemtable()
	}

}
func (s *Store) Get(key string) (string, error) {
	v, ok := s.memtable[key]
	if ok {
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

// func (s *Store) Compact() error {
// 	d := s.index
// 	f, err := os.Create("compact.txt")
// 	if err != nil {
// 		return (err)
// 	}

// 	for i, v := range d {
// 		fmt.Fprintf(f, "%s,%s\n", i, v)
// 	}
// 	f.Close()                              // close before rename as windows does not allow this
// 	err = os.Rename("compact.txt", s.path) //compact.txt gets renamed to s.path, replacing the original store.txt file.
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func (s *Store) flushMemtable() {
	filepath := fmt.Sprintf("ssl_%d.txt", s.sstableCount)
	f, err := os.Create(filepath)
	if err != nil {
		log.Fatal(err)
	}
	s.sstableCount++

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

	s.wal.Close()
	err = os.Truncate("wal.log", 0)
	if err != nil {
		log.Fatal(err)
	}
	clear(s.memtable)
	s.memtableSize = 0

	s.wal, err = os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

}

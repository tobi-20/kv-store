package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"slices"
	"strings"
)

type SparseIndexEntry struct {
	key    string
	offset int64
}
type Store struct {
	memtable     map[string]string
	memtableSize int
	sstableCount int
	path         string
	wal          *os.File
	index        map[string][]SparseIndexEntry
}

// load file from disk
func NewStore(path string) (*Store, error) {

	s := &Store{

		memtable: make(map[string]string),
		path:     path,
		index:    make(map[string][]SparseIndexEntry),
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
	startOffset := int64(0)
	for i := s.sstableCount - 1; i >= 0; i-- {

		filePath := fmt.Sprintf("ssl_%d.txt", i)
		entries := s.index[filePath]
		if len(entries) == 0 {
			startOffset = int64(0)
		}
		//binary search
		for _, v := range entries {
			if v.key <= key {
				startOffset = v.offset
			} else {
				break
			}
		}
		f, err := os.Open(filePath)
		if err != nil {
			return "", err

		}
		f.Seek(startOffset, io.SeekStart)
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ",")
			if len(parts) != 2 {
				continue
			}
			k, v := parts[0], parts[1]
			if k == key {
				return v, nil
			}
		}
		f.Close()
	}

	return "", errors.New("key not found")
}

func (s *Store) Compact() {
	base := 0
	a := 1
	for s.sstableCount > 1 {

		first := fmt.Sprintf("ssl_%d.txt", base)
		consec := fmt.Sprintf("ssl_%d.txt", a)

		out, err := os.Create("ssl_compact.txt")
		if err != nil {
			log.Fatal(err)
		}

		fir, err := os.Open(first)
		if err != nil {
			log.Fatal(err)
		}
		d, err := os.Open(consec)
		if err != nil {
			log.Fatal(err)
		}

		scanner1 := bufio.NewScanner(fir)
		scanner2 := bufio.NewScanner(d)

		hasLine1 := scanner1.Scan()
		hasLine2 := scanner2.Scan()

		var line1, line2 string
		var split1, split2 []string

		if hasLine1 {
			line1 = scanner1.Text()
			split1 = strings.Split(line1, ",")
		}
		if hasLine2 {
			line2 = scanner2.Text()
			split2 = strings.Split(line2, ",")
		}

		//mergesort
		for hasLine1 && hasLine2 {

			if split1[0] < split2[0] {
				fmt.Fprintln(out, line1)
				hasLine1 = scanner1.Scan() //advance and return boolean
				if hasLine1 {
					line1 = scanner1.Text() //returns most recent scanned line
					split1 = strings.Split(line1, ",")
					if len(split1) != 2 {
						log.Fatal("invalid sstable line format")
					}

				}
			} else if split1[0] > split2[0] {
				fmt.Fprintln(out, line2)
				hasLine2 = scanner2.Scan()
				if hasLine2 {
					line2 = scanner2.Text()
					split2 = strings.Split(line2, ",")
					if len(split2) != 2 {
						log.Fatal("invalid sstable line format")
					}
				}
			} else {
				fmt.Fprintln(out, line2) // newer file wins
				hasLine1 = scanner1.Scan()
				hasLine2 = scanner2.Scan()
				if hasLine1 {
					line1 = scanner1.Text()
					split1 = strings.Split(line1, ",")
				}

				if hasLine2 {
					line2 = scanner2.Text()
					split2 = strings.Split(line2, ",")
				}
			}

		}

		for hasLine1 {
			fmt.Fprintln(out, line1)
			hasLine1 = scanner1.Scan()
			if hasLine1 {
				line1 = scanner1.Text()

			}
		}
		for hasLine2 {
			fmt.Fprintln(out, line2)
			hasLine2 = scanner2.Scan()
			if hasLine2 {
				line2 = scanner2.Text()

			}
		}

		//"The merging process is complete, we switch read requests to using the new merged segment instead of the old segments — and then the old segment files can simply be deleted."
		fir.Close()
		d.Close()
		out.Close()
		err = os.Remove(first)
		if err != nil {
			log.Fatal("file failed to delete")
		}
		err = os.Remove(consec)
		if err != nil {
			log.Fatal("file failed to delete")
		}
		err = os.Rename("ssl_compact.txt", "ssl_0.txt")
		if err != nil {
			log.Fatal("file failed to be renamed")
		}
		s.sstableCount--
		a++
	}

}

func (s *Store) flushMemtable() {

	// for{}
	filepath := fmt.Sprintf("ssl_%d.txt", s.sstableCount)
	f, err := os.Create(filepath)
	if err != nil {
		log.Fatal(err)
	}

	indexBuffer := make([]string, 0, 4097)
	for k := range s.memtable {
		indexBuffer = append(indexBuffer, k)
	}
	slices.SortFunc(indexBuffer, func(a string, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}

		return 0
	})

	jump := 10
	count := 0
	if s.index[filepath] == nil {
		s.index[filepath] = make([]SparseIndexEntry, 0, len(indexBuffer)/jump+1) // allocate proper capacity
	}

	for _, k := range indexBuffer {
		offset, err := f.Seek(0, io.SeekCurrent) //beginning of the file (initialized for the first time)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Fprintf(f, "%s,%s\n", k, s.memtable[k]) //write into file being flushed into

		if count%jump == 0 {

			// add to the SSTable's sparse index slice
			s.index[filepath] = append(s.index[filepath], SparseIndexEntry{
				key:    k,
				offset: offset,
			})
		}

		count++

	}
	s.sstableCount++
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

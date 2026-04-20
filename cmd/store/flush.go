package store

import (
	"fmt"
	"io"
	"os"
	"slices"
)

func (s *Store) flushMemtable() error {

	// for{}
	filepath := fmt.Sprintf("ssl_%d.txt", s.sstableCount)
	f, err := os.Create(filepath)
	if err != nil {
		return err
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
			return (err)
		}
		//fmt.Fprintf(f, "%s,%s\n", k, s.memtable[k])
		encode, err := encodeWAL(OpSet, k, s.memtable[k])
		if err != nil {
			return (err)
		}
		if _, err = f.Write(encode); err != nil {
			return (err)
		} //write into file being flushed into

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
	f.Close()
	s.wal.Close()

	err = os.Truncate("wal.log", 0)
	if err != nil {
		return (err)
	}
	clear(s.memtable)
	s.memtableSize = 0

	s.wal, err = os.OpenFile("wal.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return (err)
	}
	return nil
}

package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
		r := bufio.NewReader(f)
		var searchKeyLen, valueLen uint32
		var Op Operation
		for {

			if err := binary.Read(r, binary.BigEndian, &Op); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return "", err
			}
			if err := binary.Read(r, binary.BigEndian, &searchKeyLen); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return "", err
			}
			searchKey := make([]byte, searchKeyLen)
			if _, err = io.ReadFull(r, searchKey); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return "", err
			}
			if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return "", err
			}
			value := make([]byte, valueLen)
			if _, err = io.ReadFull(r, value); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return "", err
			}
			if string(searchKey) == key {
				f.Close()

				return string(value), nil
			}
			if string(searchKey) > key {
				break
			}
		}

		f.Close()

	}

	return "", errors.New("searchkey not found")
}

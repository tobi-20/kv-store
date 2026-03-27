package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
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
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ",")
			if len(parts) != 2 {
				continue
			}
			k, v := parts[0], parts[1]
			if k == key {
				f.Close()
				return v, nil

			}
		}
		f.Close()
	}

	return "", errors.New("key not found")
}

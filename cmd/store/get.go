package store

import (
	"errors"
	"fmt"
)

func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. check MEMTABLE first
	if v, ok := s.memtable[key]; ok {
		if v == Tombstone {
			return "", nil
		}
		return v, nil
	}

	// 2. SSTABLES (newest → oldest)
	for i := s.sstableCount - 1; i >= 0; i-- {

		filePath := fmt.Sprintf("ssl_%d.txt", i)

		entries := s.index[filePath]
		if len(entries) == 0 {
			continue
		}

		// 3. FIND OFFSET IN INDEX (binary search)
		offset, found := findOffset(entries, key)
		if !found {
			continue
		}

		// 4. SEEK + DECODE ONE RECORD
		val, op, err := s.readAt(filePath, offset)
		if err != nil {
			return "", err
		}

		// tombstone rule
		if op == OpDelete {
			return "", nil
		}

		return val, nil
	}

	return "", errors.New("not found")
}

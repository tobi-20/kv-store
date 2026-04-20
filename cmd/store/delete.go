package store

const Tombstone = "__tombstone__" // uses a tombstone
func (s *Store) Delete(key string) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	msg, err := encodeWAL(OpDelete, key, "")
	if err != nil {
		return err
	}

	if _, err := s.wal.Write(msg); err != nil {
		return err
	}

	s.memtable[key] = Tombstone

	s.memtableSize += len(key)

	if s.memtableSize >= 4096 {
		return s.flushMemtable()
	}

	return nil
}

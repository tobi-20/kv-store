package store

const Tombstone = "__tombstone__" // uses a tombstone
func (s *Store) Delete(key string) error {

	s.mu.Lock()
	defer s.mu.Unlock()

	msg, err := encodeWAL(OpDelete, key, Tombstone)
	if err != nil {
		return err
	}

	if _, err := s.wal.Write(msg); err != nil {
		return err
	}

	s.memtable[key] = Tombstone

	s.memtableSize += len(key) + len(Tombstone) // since the new value is Tombstone

	if s.memtableSize >= 4096 {
		return s.flushMemtable()
	}

	return nil
}

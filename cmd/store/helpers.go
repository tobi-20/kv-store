package store

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync/atomic"
)

func (f *Follower) writerLoop() { //3

	for event := range f.queue {
		err := Encode(f.conn, event)
		if err != nil {
			f.conn.Close()
			return
		}
	}
}
func (r *Replicator) Start() { //2

	go func() {
		for event := range r.eventCh {
			dead := make([]*Follower, 0)
			r.mu.RLock()
			//lock so followers do not get modified while reading from them
			for _, f := range r.followers {
				select {
				case f.queue <- event:
				default:
					dead = append(dead, f)
				}
			}
			r.mu.RUnlock()
			if len(dead) > 0 {
				r.mu.Lock()
				for _, d := range dead {
					r.removeFollower(d)
				}
				r.mu.Unlock()
			}

		} //dispatcher

	}()
}
func (r *Replicator) Write(key, value string) { //1

	seq := atomic.AddUint64(&r.seq, 1)

	action := ReplicationEvent{
		Seq:   seq,
		Key:   key,
		Value: value,
	}
	r.eventCh <- action
}

func (r *Replicator) removeFollower(defect *Follower) { //5
	for i, f := range r.followers {
		if f == defect {
			// remove from slice
			r.followers[i] = r.followers[len(r.followers)-1] // replace the last element in the followers slice
			r.followers = r.followers[:len(r.followers)-1]   //remove the second occurrence of the last element

			// cleanup
			close(f.queue)
			f.conn.Close()

			return
		}
	}
}

func Encode(w io.Writer, e ReplicationEvent) error { //4
	if err := binary.Write(w, binary.BigEndian, e.Seq); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, e.Op); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(e.ID))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(e.ID)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(e.Key))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(e.Key)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(e.Value))); err != nil {
		return err
	}
	if _, err := w.Write([]byte(e.Value)); err != nil {
		return err
	}
	return nil

}

func Decode(r io.Reader) (ReplicationEvent, error) {
	var e ReplicationEvent
	var keyLen, valueLen, IDLen uint32

	if err := binary.Read(r, binary.BigEndian, &e.Seq); err != nil {
		return ReplicationEvent{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &e.Op); err != nil {
		return ReplicationEvent{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &IDLen); err != nil {
		return ReplicationEvent{}, err
	}
	ID := make([]byte, IDLen)
	if _, err := io.ReadFull(r, ID); err != nil {
		return ReplicationEvent{}, err
	}

	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return ReplicationEvent{}, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(r, key); err != nil {
		return ReplicationEvent{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
		return ReplicationEvent{}, err
	}
	value := make([]byte, valueLen)

	if _, err := io.ReadFull(r, value); err != nil {
		return ReplicationEvent{}, err
	}

	return ReplicationEvent{
		ID:    string(ID),
		Seq:   e.Seq,
		Key:   string(key),
		Value: string(value),
		Op:    e.Op,
	}, nil
}

func HandleConn(conn net.Conn, store *Store) {
	defer conn.Close()

	for {
		event, err := Decode(conn)
		if err != nil {
			return
		}

		switch event.Op {
		case OpSet:
			store.Set(event.Key, event.Value)

		case OpDelete:
			// store.Delete(event.Key)
		}
	}
}

func encodeWAL(op Operation, key, value string) ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, op); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, uint32(len(key))); err != nil {
		return nil, err
	}

	buf.Write([]byte(key))

	if err := binary.Write(buf, binary.BigEndian, uint32(len(value))); err != nil {
		return nil, err
	}

	buf.Write([]byte(value))

	return buf.Bytes(), nil
}

func (s *Store) SetReplicator(r *Replicator) {
	s.replicator = r

}

func (r *Replicator) WriteDelete(key string) {

	seq := atomic.AddUint64(&r.seq, 1)

	r.eventCh <- ReplicationEvent{
		Seq: seq,
		Key: key,
		Op:  OpDelete,
	}
}

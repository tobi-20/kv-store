package service

import (
	"encoding/json"
	"sync/atomic"
)

func (f *Follower) writerLoop() {
	enc := json.NewEncoder(f.conn)

	for event := range f.queue {
		err := enc.Encode(event)
		if err != nil {
			f.conn.Close()
			return
		}
	}
}
func (r *Replicator) Start() {
	var dead []*Follower
	go func() {
		for event := range r.eventCh {
			r.mu.RLock()

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

		}

	}()
}
func (r *Replicator) Write(key, value string) {

	seq := atomic.AddUint64(&r.seq, 1)

	action := ReplicationEvent{
		Seq:   seq,
		Key:   key,
		Value: value,
	}
	r.eventCh <- action
}

func (r *Replicator) removeFollower(defect *Follower) {
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

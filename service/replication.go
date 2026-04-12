package service

func NewReplicator() (*Replicator, error) {
	r := &Replicator{
		eventCh:   make(chan ReplicationEvent, 100),
		followers: make([]*Follower, 0, 10),
	}
	return r, nil
}

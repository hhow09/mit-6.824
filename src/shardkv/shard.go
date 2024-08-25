package shardkv

import (
	"sync"

	"6.824/labgob"
)

type Shards []*Shard

// 1 KVserver holds a subset of shards
func NewShards(N int) Shards {
	s := make([]*Shard, N)
	for i := 0; i < N; i++ {
		s[i] = NewShard()
	}
	return s
}

func (s *Shards) EncodeSnapshot(encoder *labgob.LabEncoder) error {
	if err := encoder.Encode(s); err != nil {
		return err
	}
	return nil
}

func (s *Shards) DecodeSnapshot(decoder *labgob.LabDecoder) error {
	if err := decoder.Decode(&s); err != nil {
		return err
	}
	return nil
}

func (s *Shards) ReadyForNewConfig() bool {
	for _, shard := range *s {
		if shard.GetStatus() != Serving {
			return false
		}
	}
	return true
}

func (s *Shards) ShardsByStatus(stat ShardStatus) []int {
	shards := make([]int, 0)
	for i, shard := range *s {
		if shard.GetStatus() == stat {
			shards = append(shards, i)
		}
	}
	return shards

}

type ShardStatus string

const (
	// the shard can serving request
	Serving ShardStatus = "Serving"
	// new config add this shard, need to pull from another group
	Pulling ShardStatus = "Pulling"
	// new config remove this shard, need be pulled by another group
	// after pulled, the shard can be deleted
	PulledByOthers ShardStatus = "PulledByOthers"
	// the shard is newly pulled and can serve request,
	// but marked in order to clean up the shard on other group
	// after clean up, changed to Serving
	Cleaning ShardStatus = "Cleaning"
)

type Shard struct {
	mu     sync.RWMutex
	status ShardStatus
	data   map[string]string
}

func NewShard() *Shard {
	return &Shard{
		status: Serving,
		data:   make(map[string]string),
	}
}

func NewNewShardFromData(data map[string]string, status ShardStatus) *Shard {
	copied := make(map[string]string)
	for k, v := range data {
		copied[k] = v
	}
	return &Shard{
		status: status,
		data:   copied,
	}
}

func (s *Shard) GetStatus() ShardStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *Shard) SetStatus(status ShardStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

func (s *Shard) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val := s.data[key]
	return val
}

func (s *Shard) Put(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Shard) Append(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] += value
}

func (s *Shard) DCopyData() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	copied := make(map[string]string)
	for k, v := range s.data {
		copied[k] = v
	}
	return copied
}

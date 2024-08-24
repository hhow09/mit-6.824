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

type Shard struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewShard() *Shard {
	return &Shard{
		data: make(map[string]string),
	}
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

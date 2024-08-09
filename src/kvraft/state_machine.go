package kvraft

import (
	"sync"

	"6.824/labgob"
)

type InMemoryStateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewInMemoryStateMachine() *InMemoryStateMachine {
	return &InMemoryStateMachine{
		data: make(map[string]string),
	}
}

func (s *InMemoryStateMachine) Get(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data[key]
}

func (s *InMemoryStateMachine) Put(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *InMemoryStateMachine) Append(key string, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] += value
}

func (s *InMemoryStateMachine) EncodeSnapshot(encoder *labgob.LabEncoder) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := encoder.Encode(s.data); err != nil {
		return err
	}
	return nil
}

func (s *InMemoryStateMachine) DecodeSnapshot(decoder *labgob.LabDecoder) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := decoder.Decode(&s.data); err != nil {
		return err
	}
	return nil
}

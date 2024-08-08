package kvraft

import "sync"

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

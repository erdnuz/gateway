package hub

import (
	"context"
	"sync"
)

type InMemoryTierStore struct {
	mu     sync.RWMutex
	tiers  map[string]string
	owners map[string]string
}

func NewInMemoryTierStore() *InMemoryTierStore {
	return &InMemoryTierStore{tiers: make(map[string]string)}
}

func (s *InMemoryTierStore) key(prefix, apiKey string) string {
	return prefix + ":" + apiKey
}

func (s *InMemoryTierStore) GetTier(ctx context.Context, prefix, apiKey string) (string, error) {
	_ = ctx
	s.mu.RLock()
	defer s.mu.RUnlock()
	if tier, ok := s.tiers[s.key(prefix, apiKey)]; ok {
		return tier, nil
	}
	return "", nil
}

func (s *InMemoryTierStore) SetTier(ctx context.Context, prefix, apiKey, tierID string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tiers[s.key(prefix, apiKey)] = tierID
	return nil
}

func (s *InMemoryTierStore) DeleteTier(ctx context.Context, prefix, apiKey string) error {
	_ = ctx
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tiers, s.key(prefix, apiKey))
	return nil
}

func (s *InMemoryTierStore) SetOwner(_ context.Context, prefix, apiKey, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners == nil {
		s.owners = map[string]string{}
	}
	if owner == "" {
		delete(s.owners, s.key(prefix, apiKey))
	} else {
		s.owners[s.key(prefix, apiKey)] = owner
	}
	return nil
}

func (s *InMemoryTierStore) GetOwner(_ context.Context, prefix, apiKey string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.owners[s.key(prefix, apiKey)], nil
}

package state

import (
	"gate/internal/config"
	"sync"
)

type LocalState struct {
	sync.RWMutex
	// ServiceID -> Full Service Configuration
	Configs map[string]config.ServiceConfig

	// ServiceID -> APIKey -> TierName (e.g., "GOLD")
	UserTiers map[string]map[string]int64

	// ServiceID -> APIKey -> Last Known Global Count from Redis
	GlobalCounts map[string]map[string]int64

	// ServiceID -> APIKey -> Local Buffer (un-synced requests from this pod)
	LocalBuffer map[string]map[string]int64
}

func NewLocalState() *LocalState {
	return &LocalState{
		Configs:      make(map[string]config.ServiceConfig),
		UserTiers:    make(map[string]map[string]int64),
		GlobalCounts: make(map[string]map[string]int64),
		LocalBuffer:  make(map[string]map[string]int64),
	}
}

// GetProjectedUsage calculates: Last Redis Count + Local Un-synced buffer
func (s *LocalState) GetProjectedUsage(svcID, apiKey string) int64 {
	s.RLock()
	defer s.RUnlock()

	global := s.GlobalCounts[svcID][apiKey]
	local := s.LocalBuffer[svcID][apiKey]
	return global + local
}

// IncrementLocal adds one to the local buffer before the next sync
func (s *LocalState) IncrementLocal(svcID, apiKey string, weight int64) {
	s.Lock()
	defer s.Unlock()

	// Initialization check to prevent "assignment to entry in nil map"
	if s.LocalBuffer[svcID] == nil {
		s.LocalBuffer[svcID] = make(map[string]int64)
	}

	s.LocalBuffer[svcID][apiKey] += weight
}

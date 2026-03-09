package testing

import (
	"context"
	"sync"
	"time"
)

// MockRedisClient is a simple in-memory Redis mock for testing
type MockRedisClient struct {
	data sync.Map // thread-safe string -> interface{} map
}

// NewMockRedisClient creates a new mock Redis client
func NewMockRedisClient() *MockRedisClient {
	return &MockRedisClient{
		data: sync.Map{},
	}
}

// Get retrieves a value from the mock store
func (m *MockRedisClient) Get(ctx context.Context, key string) *MockStringCmd {
	val, ok := m.data.Load(key)
	return &MockStringCmd{val: val, ok: ok}
}

// Set stores a value in the mock store with TTL
func (m *MockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *MockStatusCmd {
	m.data.Store(key, value)
	return &MockStatusCmd{val: "OK", err: nil}
}

// Del deletes a key from the mock store
func (m *MockRedisClient) Del(ctx context.Context, keys ...string) *MockIntCmd {
	count := 0
	for _, key := range keys {
		if _, ok := m.data.Load(key); ok {
			m.data.Delete(key)
			count++
		}
	}
	return &MockIntCmd{val: int64(count), err: nil}
}

// IncrBy increments an integer value
func (m *MockRedisClient) IncrBy(ctx context.Context, key string, increment int64) *MockIntCmd {
	val, _ := m.data.LoadOrStore(key, int64(0))
	newVal := val.(int64) + increment
	m.data.Store(key, newVal)
	return &MockIntCmd{val: newVal, err: nil}
}

// DecrBy decrements an integer value
func (m *MockRedisClient) DecrBy(ctx context.Context, key string, decrement int64) *MockIntCmd {
	val, _ := m.data.LoadOrStore(key, int64(0))
	newVal := val.(int64) - decrement
	m.data.Store(key, newVal)
	return &MockIntCmd{val: newVal, err: nil}
}

// Ping checks if the connection is alive
func (m *MockRedisClient) Ping(ctx context.Context) *MockStatusCmd {
	return &MockStatusCmd{val: "PONG", err: nil}
}

// Close closes the mock client
func (m *MockRedisClient) Close() error {
	return nil
}

// Flush clears all data
func (m *MockRedisClient) Flush() {
	m.data.Range(func(key, value interface{}) bool {
		m.data.Delete(key)
		return true
	})
}

// MockStringCmd represents a Redis string command result
type MockStringCmd struct {
	val interface{}
	ok  bool
	err error
}

// Bytes returns the value as bytes
func (c *MockStringCmd) Bytes() ([]byte, error) {
	if !c.ok {
		return nil, c.err
	}
	if b, ok := c.val.([]byte); ok {
		return b, nil
	}
	if s, ok := c.val.(string); ok {
		return []byte(s), nil
	}
	return nil, c.err
}

// Result returns the error
func (c *MockStringCmd) Result() (string, error) {
	if !c.ok {
		return "", c.err
	}
	if s, ok := c.val.(string); ok {
		return s, nil
	}
	return "", c.err
}

// MockIntCmd represents a Redis integer command result
type MockIntCmd struct {
	val int64
	err error
}

// Result returns the integer value and error
func (c *MockIntCmd) Result() (int64, error) {
	return c.val, c.err
}

// MockStatusCmd represents a Redis status command result
type MockStatusCmd struct {
	val string
	err error
}

// Result returns the status and error
func (c *MockStatusCmd) Result() (string, error) {
	return c.val, c.err
}

// Err returns the error
func (c *MockStatusCmd) Err() error {
	return c.err
}

// String returns the string representation
func (c *MockStatusCmd) String() string {
	return c.val
}

package forget

import (
	"io"
	"time"
)

// SingleSpace is equivalent to Cache but it doesn't use keyspaces.
type SingleSpace struct {
	cache *Cache
}

// NewSingleSpace is like New() but initializes a cache that doesn't use
// keyspaces.
func NewSingleSpace(o Options) *SingleSpace {
	return &SingleSpace{cache: New(o)}
}

// Get is like Cache.Get without keyspaces.
func (s *SingleSpace) Get(key string) (io.Reader, bool) {
	return s.cache.Get("", key)
}

// GetBytes is like Cache.GetBytes without keyspaces.
func (s *SingleSpace) GetBytes(key string) ([]byte, bool) {
	return s.cache.GetBytes("", key)
}

// Set is like Cache.Set without keyspaces.
func (s *SingleSpace) Set(key string, size int, ttl time.Duration) io.Writer {
	return s.cache.Set("", key, size, ttl)
}

// SetBytes is like Cache.SetBytes without keyspaces.
func (s *SingleSpace) SetBytes(key string, data []byte, ttl time.Duration) {
	s.cache.SetBytes("", key, data, ttl)
}

// Del is like Cache.Del without keyspaces.
func (s *SingleSpace) Del(key string) {
	s.cache.Del("", key)
}

// Status returns information about a single space cache.
func (s *SingleSpace) Status() Size {
	return s.cache.StatusOf("")
}

// Close releases the resources of the cache.
func (s *SingleSpace) Close() {
	s.cache.Close()
}

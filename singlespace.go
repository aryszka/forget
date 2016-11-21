package forget

import "time"

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
func (s *SingleSpace) Get(key string) ([]byte, bool) {
	return s.cache.Get("", key)
}

// Set is like Cache.Set without keyspaces.
func (s *SingleSpace) Set(key string, data []byte, ttl time.Duration) {
	s.cache.Set("", key, data, ttl)
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

package forget

import "time"

type SingleSpace struct {
	cache *Cache
}

func NewSingleSpace(maxSize int) *SingleSpace {
	return &SingleSpace{cache: New(maxSize)}
}

func (s *SingleSpace) Get(key string) ([]byte, bool) {
	return s.cache.Get("", key)
}

func (s *SingleSpace) Set(key string, data []byte, ttl time.Duration) {
	s.cache.Set("", key, data, ttl)
}

func (s *SingleSpace) Del(key string) {
	s.cache.Del("", key)
}

func (s *SingleSpace) Status() *Status {
	return s.cache.Status("")
}

func (s *SingleSpace) Close() {
	s.cache.Close()
}

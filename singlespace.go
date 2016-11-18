package forget

import "time"

type SingleSpace struct {
	cache *Cache
}

func NewSingleSpace(o Options) *SingleSpace {
	return &SingleSpace{cache: New(o)}
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

func (s *SingleSpace) Status() *KeyspaceStatus {
	return s.cache.StatusOf("")
}

func (s *SingleSpace) Close() {
	s.cache.Close()
}

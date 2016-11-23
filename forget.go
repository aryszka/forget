package forget

import "sync"

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache struct {
	mx    *sync.RWMutex
	cache *cache
}

// New initializes a cache.
func New() *Cache {
	return &Cache{
		mx:    &sync.RWMutex{},
		cache: newCache(),
	}
}

// Get retrieves an item from the cache with a key. If found, the second
// return argument will be true, otherwise false.
func (c *Cache) Get(key string) (interface{}, bool) {
	c.mx.Lock()
	defer c.mx.Unlock()
	d, ok := c.cache.get(key)
	return d, ok
}

// Set sets an item in the cache with a key.
func (c *Cache) Set(key string, data interface{}) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.cache.set(key, data)
}

// Del deletes an item from the cache with a key.
func (c *Cache) Del(key string) {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.cache.del(key)
}

// Close shuts down the cache and releases resource.
func (c *Cache) Close() {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.cache.close()
}

// binary data

// once possible, make an http comparison

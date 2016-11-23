package forget

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache struct {
	cache map[string]interface{}
}

// New initializes a cache.
func New() *Cache {
	return &Cache{cache: make(map[string]interface{})}
}

// Get retrieves an item from the cache with a key. If found, the second
// return argument will be true, otherwise false.
func (c *Cache) Get(key string) (interface{}, bool) {
	d, ok := c.cache[key]
	return d, ok
}

// Set sets an item in the cache with a key.
func (c *Cache) Set(key string, data interface{}) {
	c.cache[key] = data
}

// Del deletes an item from the cache with a key.
func (c *Cache) Del(key string) {
	delete(c.cache, key)
}

// concurrency

// once possible, make an http comparison

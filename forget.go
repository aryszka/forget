package forget

import (
	"bytes"
	"io"
	"sync"
)

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache struct {
	mx    *sync.Mutex
	cache *cache
}

// New initializes a cache.
func New() *Cache {
	return &Cache{
		mx:    &sync.Mutex{},
		cache: newCache(),
	}
}

// Get retrieves a reader to an item in the cache. The second return argument indicates if the item was
// found. Reading can start before writing to the item was finished. The reader blocks if the read reaches the point that
// the writer didn't pass yet. If the write finished, and the reader reaches the end of the item, EOF is
// returned. The reader returns ErrCacheClosed if the cache was closed and ErrItemDiscarded if
// the original item with the given keyspace and key is not available anymore.
func (c *Cache) Get(key string) (io.Reader, bool) {
	c.mx.Lock()
	defer c.mx.Unlock()

	if e, ok := c.cache.get(key); ok {
		return newIO(c.mx, e), true
	}

	return nil, false
}

// Set creates a cache item and returns a writer that can be used to store the assocated data.
// The writer returns ErrItemDiscarded if the item is not available anymore, and ErrWriteLimit if the item
// reaches the maximum item size of the cache.
func (c *Cache) Set(key string) (io.WriteCloser, bool) {
	c.mx.Lock()
	defer c.mx.Unlock()

	e, ok := c.cache.set(key)
	if !ok {
		return nil, false
	}

	return newIO(c.mx, e), true
}

// GetBytes retrieves an item from the cache with a key. If found, the second
// return argument will be true, otherwise false.
func (c *Cache) GetBytes(key string) ([]byte, bool) {
	r, ok := c.Get(key)
	if !ok {
		return nil, false
	}

	b := bytes.NewBuffer(nil)
	_, err := io.Copy(b, r)
	return b.Bytes(), err == nil // TODO: which errors can happen here
}

// SetBytes sets an item in the cache with a key.
func (c *Cache) SetBytes(key string, data []byte) bool {
	w, ok := c.Set(key)
	if !ok {
		return false
	}

	b := bytes.NewBuffer(data)
	io.Copy(w, b) // TODO: which errors can happen here
	w.Close()     // TODO: which errors can happen here
	return true
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

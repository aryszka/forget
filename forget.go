package forget

import (
	"bytes"
	"hash"
	"hash/fnv"
	"io"
	"sync"
)

// Options objects are used to pass in parameters to new Cache instances.
type Options struct {

	// MaxSize defines the maximum size of the memory.
	MaxSize int

	// SegmentSize defines the segment size in the memory.
	SegmentSize int

	hashing func() hash.Hash64
}

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache struct {
	mx      *sync.Mutex
	cache   *cache
	hashing func() hash.Hash64
}

// New initializes a cache.
func New(o Options) *Cache {
	if o.hashing == nil {
		o.hashing = fnv.New64a
	}

	return &Cache{
		mx:      &sync.Mutex{},
		cache:   newCache(o.MaxSize/o.SegmentSize, o.SegmentSize),
		hashing: o.hashing,
	}
}

func (c *Cache) hash(key string) uint64 {
	h := c.hashing()
	h.Write([]byte(key))
	return h.Sum64()
}

// Get retrieves a reader to an item in the cache. The second return argument indicates if the item was
// found. Reading can start before writing to the item was finished. The reader blocks if the read reaches the point that
// the writer didn't pass yet. If the write finished, and the reader reaches the end of the item, EOF is
// returned. The reader returns ErrCacheClosed if the cache was closed and ErrItemDiscarded if
// the original item with the given keyspace and key is not available anymore. The reader must be closed.
func (c *Cache) Get(key string) (io.ReadCloser, bool) {
	h := c.hash(key)

	c.mx.Lock()
	defer c.mx.Unlock()

	if e, ok := c.cache.get(id{hash: h, key: key}); ok {
		return newReader(c.mx, e), true
	}

	return nil, false
}

// GetKey checks if a key is in the cache.
func (c *Cache) GetKey(key string) bool {
	_, exists := c.Get(key)
	return exists
}

// GetBytes retrieves an item from the cache with a key. If found, the second
// return argument will be true, otherwise false.
func (c *Cache) GetBytes(key string) ([]byte, bool) {
	r, ok := c.Get(key)
	if !ok {
		return nil, false
	}

	defer r.Close()

	b := bytes.NewBuffer(nil)
	_, err := io.Copy(b, r)
	return b.Bytes(), err == nil // TODO: which errors can happen here
}

// Set creates a cache item and returns a writer that can be used to store the assocated data.
// The writer returns ErrItemDiscarded if the item is not available anymore, and ErrWriteLimit if the item
// reaches the maximum item size of the cache. The writer must be closed to indicate the end of data.
func (c *Cache) Set(key string) (io.WriteCloser, bool) {
	id := id{hash: c.hash(key), key: key}

	c.mx.Lock()
	defer c.mx.Unlock()

	e, ok := c.cache.set(id)
	if !ok {
		return nil, false
	}

	return newWriter(c.mx, c.cache, e), true
}

// SetKey sets only a key without data.
func (c *Cache) SetKey(key string) bool {
	// once key in the fixed block:
	//
	// if w, ok := c.Set(key); ok {
	// 	err := w.Close()
	// 	return err == nil
	// }
	//
	// return false

	w, _ := c.Set(key)
	w.Close()
	return true
}

// SetBytes sets an item in the cache with a key.
func (c *Cache) SetBytes(key string, data []byte) bool {
	w, ok := c.Set(key)
	if !ok {
		return false
	}

	defer w.Close()

	// TODO: which errors can happen here
	b := bytes.NewBuffer(data)
	if _, err := io.Copy(w, b); err == nil {
		return true
	}

	c.Del(key)
	return false
}

// Del deletes an item from the cache with a key.
func (c *Cache) Del(key string) {
	id := id{hash: c.hash(key), key: key}

	c.mx.Lock()
	defer c.mx.Unlock()

	c.cache.del(id)
}

// Close shuts down the cache and releases resource.
func (c *Cache) Close() {
	c.mx.Lock()
	defer c.mx.Unlock()
	c.cache.close()
}

// memory
// expiration
// make sure reader is closed everywhere
// overload handling: reader priority, writer priority, writer block
// refactor tests
// keyspaces
// verify no memory leak
// max procs

// once possible, make an http comparison

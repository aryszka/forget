package forget

import (
	"bytes"
	"hash"
	"hash/fnv"
	"io"
	"sync"
	"time"
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
	mx       *sync.RWMutex
	readCond *sync.Cond
	cache    *cache
	hashing  func() hash.Hash64
}

// SingleSpace is equivalent to Cache but it doesn't use keyspaces.
type SingleSpace struct {
	cache *Cache
}

// New initializes a cache.
func New(o Options) *Cache {
	if o.hashing == nil {
		o.hashing = fnv.New64a
	}

	return &Cache{
		mx:       &sync.RWMutex{},
		readCond: sync.NewCond(&sync.Mutex{}),
		cache:    newCache(o.MaxSize/o.SegmentSize, o.SegmentSize),
		hashing:  o.hashing,
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
func (c *Cache) Get(keyspace, key string) (io.ReadCloser, bool) {
	h := c.hash(key)

	c.mx.Lock()
	defer c.mx.Unlock()

	if e, ok := c.cache.get(id{hash: h, keyspace: keyspace, key: key}); ok {
		return newReader(c.mx, c.readCond, e, c.cache.segmentSize), true
	}

	return nil, false
}

// GetKey checks if a key is in the cache.
func (c *Cache) GetKey(keyspace, key string) bool {
	if r, exists := c.Get(keyspace, key); exists {
		r.Close()
		return true
	}

	return false
}

// GetBytes retrieves an item from the cache with a key. If found, the second
// return argument will be true, otherwise false.
//
// Equivalent to get and copy to end, so it blocks until write finished.
func (c *Cache) GetBytes(keyspace, key string) ([]byte, bool) {
	r, ok := c.Get(keyspace, key)
	if !ok {
		return nil, false
	}

	defer r.Close()

	b := bytes.NewBuffer(nil)
	_, err := io.Copy(b, r)
	return b.Bytes(), err == nil // TODO: which errors can happen here
}

func (c *Cache) setItem(id id, ttl time.Duration) (*entry, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	return c.cache.set(id, ttl)
}

// Set creates a cache item and returns a writer that can be used to store the assocated data.
// The writer returns ErrItemDiscarded if the item is not available anymore, and ErrWriteLimit if the item
// reaches the maximum item size of the cache. The writer must be closed to indicate the end of data.
func (c *Cache) Set(keyspace, key string, ttl time.Duration) (io.WriteCloser, bool) {
	if len(key) > c.cache.segmentCount*c.cache.segmentSize {
		return nil, false
	}

	id := id{hash: c.hash(key), keyspace: keyspace, key: key}
	for {
		if e, err := c.setItem(id, ttl); err == errAllocationForKeyFailed {
			c.readCond.L.Lock()
			c.readCond.Wait()
			c.readCond.L.Unlock()
		} else if err != nil {
			return nil, false
		} else {
			return newWriter(c.mx, c.readCond, c.cache, e), true
		}
	}
}

// SetKey sets only a key without data.
func (c *Cache) SetKey(keyspace, key string, ttl time.Duration) bool {
	if w, ok := c.Set(keyspace, key, ttl); ok {
		err := w.Close()
		return err == nil
	}

	return false
}

// SetBytes sets an item in the cache with a key.
func (c *Cache) SetBytes(keyspace, key string, data []byte, ttl time.Duration) bool {
	w, ok := c.Set(keyspace, key, ttl)
	if !ok {
		return false
	}

	defer w.Close()

	b := bytes.NewBuffer(data)
	_, err := io.Copy(w, b)
	return err == nil
}

// Del deletes an item from the cache with a key.
func (c *Cache) Del(keyspace, key string) {
	id := id{hash: c.hash(key), keyspace: keyspace, key: key}

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

// NewSingleSpace is like New() but initializes a cache that doesn't use
// keyspaces.
func NewSingleSpace(o Options) *SingleSpace {
	return &SingleSpace{cache: New(o)}
}

// Get is like Cache.Get without keyspaces.
func (s *SingleSpace) Get(key string) (io.ReadCloser, bool) {
	return s.cache.Get("", key)
}

// GetKey is like Cache.GetKey without keyspaces.
func (s *SingleSpace) GetKey(key string) bool {
	return s.cache.GetKey("", key)
}

// GetBytes is like Cache.GetBytes without keyspaces.
func (s *SingleSpace) GetBytes(key string) ([]byte, bool) {
	return s.cache.GetBytes("", key)
}

// Set is like Cache.Set without keyspaces.
func (s *SingleSpace) Set(key string, ttl time.Duration) (io.WriteCloser, bool) {
	return s.cache.Set("", key, ttl)
}

// SetKey is like Cache.SetKey without keyspaces.
func (s *SingleSpace) SetKey(key string, ttl time.Duration) bool {
	return s.cache.SetKey("", key, ttl)
}

// SetBytes is like Cache.SetBytes without keyspaces.
func (s *SingleSpace) SetBytes(key string, data []byte, ttl time.Duration) bool {
	return s.cache.SetBytes("", key, data, ttl)
}

// Del is like Cache.Del without keyspaces.
func (s *SingleSpace) Del(key string) {
	s.cache.Del("", key)
}

// Close shuts down the cache and releases resource.
func (s *SingleSpace) Close() { s.cache.Close() }

// verify no memory leak, whether all objects outside drop the internal references after closed
// max procs
// status, notifications
// refactor tests with documentation, examples
// fuzzy testing

// once possible, make an http comparison

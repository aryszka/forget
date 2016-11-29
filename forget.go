package forget

import (
	"bytes"
	"hash"
	"hash/fnv"
	"io"
	"runtime"
	"time"
)

const (
	// DefaultMaxSize is used when MaxSize is not specified in the initial Options.
	DefaultMaxSize = 1 << 30

	// DefaultSegmentSize is used when SegmentSize is not specified in the initial Options.
	DefaultSegmentSize = 1 << 15
)

// Options objects are used to pass in parameters to new Cache instances.
type Options struct {

	// MaxSize defines the maximum size of the memory.
	MaxSize int

	// SegmentSize defines the segment size in the memory.
	SegmentSize int

	Notify chan<- *Event

	NotifyMask EventType

	hashing  func() hash.Hash64
	maxProcs int
}

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache struct {
	options     Options
	maxItemSize int
	cache       []*cache
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

	if o.maxProcs <= 0 {
		o.maxProcs = runtime.NumCPU()
		if o.maxProcs > runtime.GOMAXPROCS(-1) {
			o.maxProcs = runtime.GOMAXPROCS(-1)
		}
	}

	if o.MaxSize <= 0 {
		o.MaxSize = DefaultMaxSize
	}

	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}

	if o.Notify == nil {
		o.NotifyMask = 0
	}

	maxInstanceSize := o.MaxSize / o.maxProcs
	maxInstanceSize -= maxInstanceSize % o.SegmentSize
	segmentCount := maxInstanceSize / o.SegmentSize
	if segmentCount == 0 {
		o.maxProcs = 1
		segmentCount = o.MaxSize / o.SegmentSize
		maxInstanceSize = segmentCount * o.SegmentSize
	}

	c := make([]*cache, o.maxProcs)
	n := newNotify(o.Notify, o.NotifyMask)
	for i := range c {
		c[i] = newCache(segmentCount, o.SegmentSize, n)
	}

	return &Cache{
		options:     o,
		maxItemSize: maxInstanceSize,
		cache:       c,
	}
}

func (c *Cache) hash(key string) uint64 {
	h := c.options.hashing()
	h.Write([]byte(key))
	return h.Sum64()
}

func (c *Cache) getCache(hash uint64) *cache {
	// TODO: >> 32 for the fnv last byte thing, but not sure about it, needs testing of distribution
	return c.cache[int(hash>>32)%len(c.cache)]
}

func (c *Cache) copy(to io.Writer, from io.Reader) (int64, error) {
	return io.CopyBuffer(to, from, make([]byte, c.options.SegmentSize))
}

// Get retrieves a reader to an item in the cache. The second return argument indicates if the item was
// found. Reading can start before writing to the item was finished. The reader blocks if the read reaches the point that
// the writer didn't pass yet. If the write finished, and the reader reaches the end of the item, EOF is
// returned. The reader returns ErrCacheClosed if the cache was closed and ErrItemDiscarded if
// the original item with the given keyspace and key is not available anymore. The reader must be closed.
func (c *Cache) Get(keyspace, key string) (io.ReadCloser, bool) {
	h := c.hash(key)
	ci := c.getCache(h)

	ci.mx.Lock()
	defer ci.mx.Unlock()

	if e, ok := ci.get(id{hash: h, keyspace: keyspace, key: key}); ok {
		ci.status.incReaders(keyspace)
		return newReader(ci, e, c.options.SegmentSize), true
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
	_, err := c.copy(b, r)
	return b.Bytes(), err == nil
}

func setItem(c *cache, id id, ttl time.Duration) (*entry, error) {
	c.mx.Lock()
	defer c.mx.Unlock()
	e, err := c.set(id, ttl)
	if err != nil {
		c.status.incWriters(id.keyspace)
	}

	return e, err
}

// Set creates a cache item and returns a writer that can be used to store the assocated data.
// The writer returns ErrItemDiscarded if the item is not available anymore, and ErrWriteLimit if the item
// reaches the maximum item size of the cache. The writer must be closed to indicate the end of data.
func (c *Cache) Set(keyspace, key string, ttl time.Duration) (io.WriteCloser, bool) {
	if len(key) > c.maxItemSize {
		return nil, false
	}

	h := c.hash(key)
	ci := c.getCache(h)
	id := id{hash: h, keyspace: keyspace, key: key}

	for {
		if e, err := setItem(ci, id, ttl); err == errAllocationFailed {
			ci.waitRead()
		} else if err != nil {
			return nil, false
		} else {
			return newWriter(ci, e), true
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
	_, err := c.copy(w, b)
	return err == nil
}

// Del deletes an item from the cache with a key.
func (c *Cache) Del(keyspace, key string) {
	h := c.hash(key)
	ci := c.getCache(h)
	id := id{hash: h, keyspace: keyspace, key: key}

	ci.mx.Lock()
	defer ci.mx.Unlock()
	ci.del(id)
}

// Status returns statistics about the cache state.
func (c *Cache) Status() *CacheStatus {
	s := make([]*InstanceStatus, 0, len(c.cache))
	for _, ci := range c.cache {
		s = append(s, ci.status)
	}

	return newCacheStatus(s)
}

// Close shuts down the cache and releases resources.
func (c *Cache) Close() {
	for _, ci := range c.cache {
		func() {
			ci.mx.Lock()
			defer ci.mx.Unlock()
			ci.close()
		}()
	}
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

// status, notifications
// refactor tests with documentation, examples, more stochastic io tests (buffer sizes, segment borders)
// - list for buckets
// - list of lists for lru round robin
// - what's the cost of having keyspaces in the notification statuses, what's the benefit?
// - what's the cost of having the keyspace status at all?
// - locking closer to where the section needs to protected
// - what happens when the key doesn't fit but under the max cache size
// - no blocking from notifications
// hash collision stats
// fuzzy testing
// scenario testing
// measure the right buffer size for the notify channel
// why the drop at 100k items
// expvar package

// once possible, make an http comparison

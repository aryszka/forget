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
	// DefaultCacheSize is used when CacheSize is not specified in the initial Options.
	DefaultCacheSize = 1 << 30

	// DefaultSegmentSize is used when SegmentSize is not specified in the initial Options.
	DefaultSegmentSize = 1 << 15
)

// Options objects are used to pass in parameters to new Cache instances.
type Options struct {

	// CacheSize defines the size of the cache.
	CacheSize int

	// SegmentSize defines the segment size in the memory.
	SegmentSize int

	// Notify is used by the cache to send notifications about internal events. When nil, no notifications are
	// sent. It is recommended to use a channel with a small buffer, e.g. 2.
	Notify chan<- *Event

	// NotifyMask can be used to select which event types should trigger a notification. The default is Normal,
	// meaning that evictions and allocation failures will trigger a notification.
	NotifyMask EventType

	hashing          func() hash.Hash64
	maxInstanceCount int
}

// Cache provides an in-memory cache for arbitrary binary data identified by keyspaces and keys. All methods of
// a Cache object are thread safe.
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
//
// Forget creates internally multiple independent cache instances, as many as the maximum of the reported CPU
// cores or the GOMAXPROCS value. These instances can be accessed in parallel without synchronization. This
// internal split of the cache affects the maximum size of a single item: ~ CacheSize / NumCPU.
func New(o Options) *Cache {
	if o.CacheSize <= 0 {
		o.CacheSize = DefaultCacheSize
	}

	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}

	if o.Notify == nil {
		o.NotifyMask = 0
	} else if o.NotifyMask == 0 {
		o.NotifyMask = Normal
	}

	if o.hashing == nil {
		o.hashing = fnv.New64a
	}

	instanceCount := o.maxInstanceCount
	if instanceCount <= 0 {
		instanceCount = runtime.NumCPU()
		if instanceCount > runtime.GOMAXPROCS(-1) {
			instanceCount = runtime.GOMAXPROCS(-1)
		}
	}

	instanceSize := o.CacheSize / instanceCount
	instanceSize -= instanceSize % o.SegmentSize
	segmentCount := instanceSize / o.SegmentSize
	if segmentCount == 0 {
		instanceCount = 1
		segmentCount = o.CacheSize / o.SegmentSize
		instanceSize = segmentCount * o.SegmentSize
	}

	n := newNotify(o.Notify, o.NotifyMask)
	c := make([]*cache, instanceCount)
	for i := range c {
		c[i] = newCache(segmentCount, o.SegmentSize, n)
	}

	return &Cache{
		options:     o,
		maxItemSize: instanceSize,
		cache:       c,
	}
}

func (c *Cache) hash(key string) uint64 {
	h := c.options.hashing()
	h.Write([]byte(key))
	return h.Sum64()
}

func (c *Cache) getCache(hash uint64) *cache {
	// take the cache instance based on the middle of the key hash
	return c.cache[int(hash>>32)%len(c.cache)]
}

// copies from a reader to a writer with a buffer of the same size as the used segments
func (c *Cache) copy(to io.Writer, from io.Reader) (int64, error) {
	return io.CopyBuffer(to, from, make([]byte, c.options.SegmentSize))
}

// Get retrieves a reader to an item in the cache. The second return argument indicates if the item was found.
// Reading can start before writing to the item was finished. The reader blocks if the read reaches the point
// that the writer didn't pass yet. If the write finished, and the reader reaches the end of the item, EOF is
// returned. The reader returns ErrCacheClosed if the cache was closed and ErrItemDiscarded if the original item
// with the given keyspace and key is not available anymore. The reader must be closed after the read was
// finished.
func (c *Cache) Get(keyspace, key string) (io.ReadCloser, bool) {
	h := c.hash(key)
	ci := c.getCache(h)
	return ci.get(h, keyspace, key)
}

// GetKey checks if a key is in the cache.
//
// It is equivalent to calling Get, and closing the reader without reading from it.
func (c *Cache) GetKey(keyspace, key string) bool {
	if r, exists := c.Get(keyspace, key); exists {
		r.Close()
		return true
	}

	return false
}

// GetBytes retrieves an item from the cache with a keyspace and key. If found, the second
// return argument will be true, otherwise false.
//
// It is equivalent to calling Get, copying the reader to the end and closing the reader.
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

// Set creates a cache item and returns a writer that can be used to store the associated data. The writer
// returns ErrItemDiscarded if the item is not available anymore, and ErrWriteLimit if the item reaches the
// maximum item size of the cache. The writer must be closed to indicate that no more data will be written to
// the item.
func (c *Cache) Set(keyspace, key string, ttl time.Duration) (io.WriteCloser, bool) {
	if len(key) > c.maxItemSize {
		return nil, false
	}

	h := c.hash(key)
	ci := c.getCache(h)
	return ci.set(h, keyspace, key, ttl)
}

// SetKey sets only a key without data.
//
// It is equivalent to calling Set, and closing the writer without writing any data.
func (c *Cache) SetKey(keyspace, key string, ttl time.Duration) bool {
	if w, ok := c.Set(keyspace, key, ttl); ok {
		err := w.Close()
		return err == nil
	}

	return false
}

// SetBytes sets an item in the cache with a keyspace and key.
//
// It is equivalent to calling Set, writing the complete data to the item and closing the writer.
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

// Del deletes an item from the cache with a keyspace and key.
func (c *Cache) Del(keyspace, key string) {
	h := c.hash(key)
	ci := c.getCache(h)
	ci.del(h, keyspace, key)
}

// Stats returns approximate statistics about the cache state.
func (c *Cache) Stats() *CacheStats {
	s := make([]*InstanceStats, 0, len(c.cache))
	for _, ci := range c.cache {
		s = append(s, ci.getStats())
	}

	return newCacheStats(s)
}

// Close shuts down the cache and releases resources.
func (c *Cache) Close() {
	for _, ci := range c.cache {
		ci.close()
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

// docs
// tests:
// - tests based on the documentation
// - tests based on the code
// - more combined io tests (buffer sizes, segment borders, event orders)
// - fuzzy testing
// - scenario testing
// - why the drop at 100k items
// - check stats, utilization
// refactor cond mutexes
// expvar package
// http package

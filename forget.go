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

	// DefaultChunkSize is used when ChunkSize is not specified in the initial Options.
	DefaultChunkSize = 1 << 15
)

// Options objects are used to pass in parameters to new Cache segments.
type Options struct {

	// CacheSize defines the size of the cache.
	CacheSize int

	// ChunkSize defines the chunk size in the memory.
	ChunkSize int

	// Notify is used by the cache to send notifications about internal events. When nil, no notifications are
	// sent. It is recommended to use a channel with a small buffer, e.g. 2.
	Notify chan<- *Event

	// NotifyMask can be used to select which event types should trigger a notification. The default is Normal,
	// meaning that evictions and allocation failures will trigger a notification.
	NotifyMask EventType

	hashing         func() hash.Hash64
	maxSegmentCount int
}

// Cache provides an in-memory cache for arbitrary binary data identified by keyspaces and keys. All methods of
// a Cache object are thread safe.
type Cache struct {
	options     Options
	maxItemSize int
	segments    []*segment
}

// Space is equivalent to Cache but it uses only a single keyspace.
type Space struct {
	cache *Cache
}

// New initializes a cache.
//
// Forget creates internally multiple independent cache segments, as many as the maximum of the reported CPU
// cores or the GOMAXPROCS value. These segments can be accessed in parallel without synchronization. This
// internal split of the cache affects the maximum size of a single item: ~ CacheSize / NumCPU.
func New(o Options) *Cache {
	if o.CacheSize <= 0 {
		o.CacheSize = DefaultCacheSize
	}

	if o.ChunkSize <= 0 {
		o.ChunkSize = DefaultChunkSize
	}

	if o.Notify == nil {
		o.NotifyMask = 0
	} else if o.NotifyMask == 0 {
		o.NotifyMask = Normal
	}

	if o.hashing == nil {
		o.hashing = fnv.New64a
	}

	segmentCount := o.maxSegmentCount
	if segmentCount <= 0 {
		segmentCount = runtime.NumCPU()
		if segmentCount > runtime.GOMAXPROCS(-1) {
			segmentCount = runtime.GOMAXPROCS(-1)
		}
	}

	segmentSize := o.CacheSize / segmentCount
	segmentSize -= segmentSize % o.ChunkSize
	chunkCount := segmentSize / o.ChunkSize
	if chunkCount == 0 {
		segmentCount = 1
		chunkCount = o.CacheSize / o.ChunkSize
		segmentSize = chunkCount * o.ChunkSize
	}

	n := newNotify(o.Notify, o.NotifyMask)
	segments := make([]*segment, segmentCount)
	for i := range segments {
		segments[i] = newCache(chunkCount, o.ChunkSize, n)
	}

	return &Cache{
		options:     o,
		maxItemSize: segmentSize,
		segments:    segments,
	}
}

func (c *Cache) hash(key string) uint64 {
	h := c.options.hashing()
	h.Write([]byte(key))
	return h.Sum64()
}

func (c *Cache) getSegment(hash uint64) *segment {
	// take the cache segment based on the middle of the key hash
	return c.segments[int(hash>>32)%len(c.segments)]
}

// copies from a reader to a writer with a buffer of the same size as the used chunks
func (c *Cache) copy(to io.Writer, from io.Reader) (int64, error) {
	return io.CopyBuffer(to, from, make([]byte, c.options.ChunkSize))
}

// Get retrieves a reader to an item in the cache. The second return argument indicates if the item was found.
// Reading can start before writing to the item was finished. The reader blocks if the read reaches the point
// that the writer didn't pass yet. If the write finished, and the reader reaches the end of the item, EOF is
// returned. The reader returns ErrCacheClosed if the cache was closed and ErrItemDiscarded if the original item
// with the given keyspace and key is not available anymore. The reader must be closed after the read was
// finished.
func (c *Cache) Get(keyspace, key string) (io.ReadCloser, bool) {
	h := c.hash(key)
	ci := c.getSegment(h)
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
	ci := c.getSegment(h)
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

// Delete deletes an item from the cache with a keyspace and key.
func (c *Cache) Delete(keyspace, key string) {
	h := c.hash(key)
	ci := c.getSegment(h)
	ci.del(h, keyspace, key)
}

// Stats returns approximate statistics about the cache state.
func (c *Cache) Stats() *CacheStats {
	s := make([]*segmentStats, 0, len(c.segments))
	for _, ci := range c.segments {
		s = append(s, ci.getStats())
	}

	return newCacheStats(s)
}

// Close shuts down the cache and releases resources.
func (c *Cache) Close() {
	for _, ci := range c.segments {
		ci.close()
	}
}

// NewSpace is like New() but initializes a cache that doesn't use
// keyspaces.
func NewSpace(o Options) *Space {
	return &Space{cache: New(o)}
}

// Get is like Cache.Get without keyspaces.
func (s *Space) Get(key string) (io.ReadCloser, bool) {
	return s.cache.Get("", key)
}

// GetKey is like Cache.GetKey without keyspaces.
func (s *Space) GetKey(key string) bool {
	return s.cache.GetKey("", key)
}

// GetBytes is like Cache.GetBytes without keyspaces.
func (s *Space) GetBytes(key string) ([]byte, bool) {
	return s.cache.GetBytes("", key)
}

// Set is like Cache.Set without keyspaces.
func (s *Space) Set(key string, ttl time.Duration) (io.WriteCloser, bool) {
	return s.cache.Set("", key, ttl)
}

// SetKey is like Cache.SetKey without keyspaces.
func (s *Space) SetKey(key string, ttl time.Duration) bool {
	return s.cache.SetKey("", key, ttl)
}

// SetBytes is like Cache.SetBytes without keyspaces.
func (s *Space) SetBytes(key string, data []byte, ttl time.Duration) bool {
	return s.cache.SetBytes("", key, data, ttl)
}

// Delete is like Cache.Delete without keyspaces.
func (s *Space) Delete(key string) {
	s.cache.Delete("", key)
}

// Close shuts down the cache and releases resource.
func (s *Space) Close() { s.cache.Close() }

// do segment stats need to be public?
// collisions to a list
// implement seeking
// docs
// - document write cancellable with delete for cancelling cache filling
// tests:
// - tests based on the documentation
// - tests based on the code
// - more combined io tests (buffer sizes, chunk borders, event orders)
// - fuzzy testing
// - scenario testing
// - why the drop at 100k items
// - check stats, utilization
// refactor cond mutexes
// expvar package
// http package

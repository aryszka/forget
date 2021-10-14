package forget

import (
	"bytes"
	"errors"
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

// Options objects are used to pass in parameters to new Cache instances.
type Options struct {

	// CacheSize defines the size of the cache.
	CacheSize int

	// ChunkSize defines the chunk size in the memory.
	ChunkSize int

	// Notify is used by the cache to send notifications about internal events. When nil, no notifications are
	// sent. It is recommended to use a channel with a small buffer, e.g. 2. Make sure that the channel is
	// continuously consumed when set. Be aware that when the channel is not nil, and the mask is 0, the
	// default mask is applied.
	Notify chan<- *Event

	// NotifyMask can be used to select which event types should trigger a notification. The default is Normal,
	// meaning that evictions and allocation failures will trigger a notification.
	NotifyMask EventType

	hashing         func() hash.Hash64
	maxSegmentCount int
}

// Cache provides an in-memory cache for arbitrary binary data identified by keys.
type Cache struct {
	spaces *CacheSpaces
}

// CacheSpaces is equivalent to Cache but it supports multiple keyspaces. Keyspaces are used to identify cache
// items in addition to the keys. Internally, when the cache is full, the cache tries to evict items first from
// the same keyspace as the item currently requiring more space.
type CacheSpaces struct {
	options     Options
	maxItemSize int
	segments    []*segment
}

var (
	// ErrItemDiscarded is returned by IO operations when an item has been discarded, e.g. evicted, deleted or
	// discarded due to the cache was closed.
	ErrItemDiscarded = errors.New("item discarded")

	// ErrWriteLimit is returned when writing to an item fills the available size.
	ErrWriteLimit = errors.New("write limit")

	// ErrReaderClosed is returned when reading from or closing a reader that was already closed before.
	ErrReaderClosed = errors.New("writer closed")

	// ErrWriterClosed is returned when writing to or closing a writer that was already closed before.
	ErrWriterClosed = errors.New("writer closed")

	// ErrInvalidSeekOffset is returned by Seek() when trying to seek to an invalid position.
	ErrInvalidSeekOffset = errors.New("invalid seek offset")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

// New initializes a cache.
//
// Forget creates internally multiple independent cache segments, as many as the maximum of the reported CPU
// cores or the GOMAXPROCS value. These segments can be accessed in parallel without synchronization cost. This
// internal split of the cache affects the maximum size of a single item: ~ CacheSize / NumCPU.
func New(o Options) *Cache {
	return &Cache{spaces: NewCacheSpaces(o)}
}

// Get retrieves a reader to an item in the cache. The second return argument indicates if the item was found.
// Reading can start before writing to the item was finished. The reader blocks if the read reaches the point
// that the writer didn't pass yet. If the write finished, and the reader reaches the end of the item, EOF is
// returned. The reader returns ErrCacheClosed if the cache was closed and ErrItemDiscarded if the original item
// with the given key is not available anymore. The reader must be closed after the read was finished.
func (c *Cache) Get(key string) (*Reader, bool) {
	return c.spaces.Get("", key)
}

// GetKey checks if a key is in the cache.
//
// It is equivalent to calling Get, and closing the reader without reading from it.
func (c *Cache) GetKey(key string) bool {
	return c.spaces.GetKey("", key)
}

// GetBytes retrieves an item from the cache with a key. If found, the second
// return argument will be true, otherwise false.
//
// It is equivalent to calling Get, copying the reader to the end and closing the reader. It is safe to modify
// the returned buffer.
func (c *Cache) GetBytes(key string) ([]byte, bool) {
	return c.spaces.GetBytes("", key)
}

// Set creates a cache item and returns a writer that can be used to store the associated data. The writer
// returns ErrItemDiscarded if the item is not available anymore, and ErrWriteLimit if the item reaches the
// maximum item size of the cache. The writer must be closed to indicate that no more data will be written to
// the item.
func (c *Cache) Set(key string, ttl time.Duration) (io.WriteCloser, bool) {
	return c.spaces.Set("", key, ttl)
}

// SetKey sets only a key without data.
//
// It is equivalent to calling Set, and closing the writer without writing any data.
func (c *Cache) SetKey(key string, ttl time.Duration) bool {
	return c.spaces.SetKey("", key, ttl)
}

// SetBytes sets an item in the cache with a key.
//
// It is equivalent to calling Set, writing the complete data to the item and closing the writer. It is safe to
// modify the buffer after SetBytes returned.
func (c *Cache) SetBytes(key string, data []byte, ttl time.Duration) bool {
	return c.spaces.SetBytes("", key, data, ttl)
}

// Delete deletes an item from the cache with a key.
func (c *Cache) Delete(key string) {
	c.spaces.Delete("", key)
}

// Stats returns approximate statistics about the cache state.
func (c *Cache) Stats() *CacheStats {
	return c.spaces.Stats()
}

// Close shuts down the cache and releases resources.
func (c *Cache) Close() { c.spaces.Close() }

// NewCacheSpaces is like New() but initializes a cache that supports keyspaces.
func NewCacheSpaces(o Options) *CacheSpaces {
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

	notify := newNotify(o.Notify, o.NotifyMask)

	if o.hashing == nil {
		o.hashing = fnv.New64a
	}

	segmentCount := o.maxSegmentCount

	if segmentCount <= 0 {
		segmentCount = runtime.NumCPU()
	}

	if segmentCount > runtime.GOMAXPROCS(-1) {
		segmentCount = runtime.GOMAXPROCS(-1)
	}

	segmentSize := o.CacheSize / segmentCount
	segmentSize -= segmentSize % o.ChunkSize

	chunkCount := segmentSize / o.ChunkSize
	if chunkCount == 0 {
		segmentSize = o.ChunkSize
		chunkCount = 1
	}

	segments := make([]*segment, segmentCount)
	for i := range segments {
		segments[i] = newSegment(chunkCount, o.ChunkSize, notify)
	}

	return &CacheSpaces{
		options:     o,
		maxItemSize: segmentSize,
		segments:    segments,
	}
}

func (c *CacheSpaces) hash(key string) uint64 {
	h := c.options.hashing()
	h.Write([]byte(key))
	return h.Sum64()
}

func (c *CacheSpaces) getSegment(hash uint64) *segment {
	// fnv: take the cache segment based on the middle of the key hash
	return c.segments[int(hash>>32)%len(c.segments)]
}

// copies from a reader to a writer with a buffer of the same size as the used chunks
func (c *CacheSpaces) copy(to io.Writer, from io.Reader) (int64, error) {
	return io.CopyBuffer(to, from, make([]byte, c.options.ChunkSize))
}

// Get is like Cache.Get but with keyspaces.
func (c *CacheSpaces) Get(keyspace, key string) (*Reader, bool) {
	h := c.hash(key)
	s := c.getSegment(h)
	return s.get(h, keyspace, key)
}

// GetKey is like Cache.GetKey but with keyspaces.
func (c *CacheSpaces) GetKey(keyspace, key string) bool {
	if r, exists := c.Get(keyspace, key); exists {
		r.Close()
		return true
	}

	return false
}

// GetBytes is like Cache.GetBytes but with keyspaces.
func (c *CacheSpaces) GetBytes(keyspace, key string) ([]byte, bool) {
	r, ok := c.Get(keyspace, key)
	if !ok {
		return nil, false
	}

	defer r.Close()

	b := bytes.NewBuffer(nil)
	_, err := c.copy(b, r)
	return b.Bytes(), err == nil
}

// Set is like Cache.Set but with keyspaces.
func (c *CacheSpaces) Set(keyspace, key string, ttl time.Duration) (*Writer, bool) {
	if len(key) > c.maxItemSize {
		return nil, false
	}

	h := c.hash(key)
	s := c.getSegment(h)
	return s.set(h, keyspace, key, ttl)
}

// SetKey is like Cache.SetKey but with keyspaces.
func (c *CacheSpaces) SetKey(keyspace, key string, ttl time.Duration) bool {
	if w, ok := c.Set(keyspace, key, ttl); ok {
		err := w.Close()
		return err == nil
	}

	return false
}

// SetBytes is like Cache.SetBytes but with keyspaces.
func (c *CacheSpaces) SetBytes(keyspace, key string, data []byte, ttl time.Duration) bool {
	w, ok := c.Set(keyspace, key, ttl)
	if !ok {
		return false
	}

	defer w.Close()

	b := bytes.NewBuffer(data)
	_, err := c.copy(w, b)
	return err == nil
}

// Delete is like Cache.Delete but with keyspaces.
func (c *CacheSpaces) Delete(keyspace, key string) {
	h := c.hash(key)
	s := c.getSegment(h)
	s.del(h, keyspace, key)
}

// Stats returns approximate statistics about the cache state.
func (c *CacheSpaces) Stats() *CacheStats {
	s := make([]*segmentStats, 0, len(c.segments))
	for _, si := range c.segments {
		s = append(s, si.getStats())
	}

	return newCacheStats(s)
}

// Close shuts down the cache and releases resources.
func (c *CacheSpaces) Close() {
	for _, s := range c.segments {
		s.close()
	}
}

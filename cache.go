package forget

import (
	"errors"
	"time"
)

// temporary structure to pass the key and its hash around during individual calls
type id struct {
	hash          uint64
	keyspace, key string
}

type cache struct {
	segmentCount, segmentSize int
	memory                    *memory
	lru, toDelete             *list
	hash                      [][]*entry
	closed                    bool
}

var (
	errAllocationForKeyFailed = errors.New("allocation for key failed")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

func newCache(segmentCount, segmentSize int) *cache {
	return &cache{
		segmentCount: segmentCount,
		segmentSize:  segmentSize,
		memory:       newMemory(segmentCount, segmentSize),
		lru:          new(list),
		toDelete:     new(list),
		hash:         make([][]*entry, segmentCount), // there cannot be more entries than segments
	}
}

func (c *cache) index(hash uint64) int {
	return int(hash % uint64(c.segmentCount))
}

func (c *cache) evictFromFor(l *list, e *entry) bool {
	current := l.first
	for current != nil {
		if current != e && c.deleteEntry(current.(*entry)) {
			return true
		}

		current = current.next()
	}

	return false
}

func (c *cache) evictFor(e *entry) bool {
	return c.evictFromFor(c.toDelete, e) || c.evictFromFor(c.lru, e)
}

func (c *cache) allocateFor(e *entry) bool {
	_, last := e.data()
	for {
		if s, ok := c.memory.allocate(); ok {
			if last != nil && s != last.next() {
				c.memory.move(s, last.next())
			}

			e.appendSegment(s)
			return true
		}

		if !c.evictFor(e) {
			return false
		}
	}
}

func (c *cache) writeKey(e *entry, key string) error {
	p := []byte(key)
	for len(p) > 0 {
		if !c.allocateFor(e) {
			return errAllocationForKeyFailed
		}

		n, _ := e.write(p)
		p = p[n:]
	}

	return nil
}

func (c *cache) lookup(id id) (*entry, bool) {
	for _, e := range c.hash[c.index(id.hash)] {
		if e.keyspace == id.keyspace && e.keyEquals(id.key) {
			return e, true
		}
	}

	return nil, false
}

func (c *cache) addLookup(id id, e *entry) {
	index := c.index(id.hash)
	c.hash[index] = append(c.hash[index], e)
}

func (c *cache) deleteLookup(e *entry) bool {
	index := c.index(e.hash)
	bucket := c.hash[index]
	for i, ei := range bucket {
		if ei == e {
			last := len(bucket) - 1
			bucket[i], bucket = bucket[last], bucket[:last]
			c.hash[index] = bucket
			return true
		}
	}

	return false
}

func (c *cache) touchEntry(e *entry) {
	c.lru.remove(e)
	c.lru.insert(e, nil)
}

func (c *cache) deleteEntry(e *entry) bool {
	if c.deleteLookup(e) {
		c.lru.remove(e)
		if e.reading > 0 {
			c.toDelete.insert(e, nil)
			return false
		}
	} else if e.reading == 0 {
		c.toDelete.remove(e)
	} else {
		return false
	}

	first, last := e.data()
	if first != nil {
		c.memory.free(first, last)
	}

	e.close()
	return true
}

func (c *cache) get(id id) (*entry, bool) {
	if c.closed {
		return nil, false
	}

	e, ok := c.lookup(id)
	if !ok {
		return nil, false
	}

	if e.expired() {
		c.deleteEntry(e)
		return nil, false
	}

	c.touchEntry(e)
	e.incReading()
	return e, ok
}

func (c *cache) set(id id, ttl time.Duration) (*entry, error) {
	if c.closed {
		return nil, ErrCacheClosed
	}

	c.del(id)
	e := newEntry(id.hash, id.keyspace, len(id.key), ttl)

	if err := c.writeKey(e, id.key); err != nil {
		return nil, err
	}

	c.lru.insert(e, nil)
	c.addLookup(id, e)

	return e, nil
}

func (c *cache) del(id id) {
	if c.closed {
		return
	}

	if e, ok := c.lookup(id); ok {
		c.deleteEntry(e)
	}
}

func (c *cache) closeAll(l *list) {
	e := l.first
	for e != nil {
		e.(*entry).close()
		e = e.next()
	}
}

func (c *cache) close() {
	if c.closed {
		return
	}

	c.closeAll(c.toDelete)
	c.closeAll(c.lru)

	c.memory = nil
	c.lru = nil
	c.toDelete = nil
	c.hash = nil
	c.closed = true
}

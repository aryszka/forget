package forget

import (
	"errors"
	"sync"
	"time"
)

// temporary structure to pass the keyspace, key and its hash around during individual calls
type id struct {
	hash          uint64
	keyspace, key string
}

type cache struct {
	segmentCount, segmentSize, lruOffset int
	mx                                   *sync.RWMutex
	readCond                             *sync.Cond
	memory                               *memory
	toDelete                             *list
	lru                                  map[string]*list
	allLRU                               []*list
	hash                                 [][]*entry
	closed                               bool
}

var (
	errAllocationFailed = errors.New("allocation for key failed")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

func newCache(segmentCount, segmentSize int) *cache {
	return &cache{
		segmentCount: segmentCount,
		segmentSize:  segmentSize,
		mx:           &sync.RWMutex{},
		readCond:     sync.NewCond(&sync.Mutex{}),
		memory:       newMemory(segmentCount, segmentSize),
		lru:          make(map[string]*list),
		toDelete:     new(list),
		hash:         make([][]*entry, segmentCount), // there cannot be more entries than segments
	}
}

func (c *cache) bucketIndex(hash uint64) int {
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
	if c.evictFromFor(c.toDelete, e) {
		return true
	}

	lru, ok := c.lru[e.keyspace]
	if ok && c.evictFromFor(lru, e) {
		return true
	}

	// round robin the rest
	var evicted bool
	for i := 0; i < len(c.allLRU); i++ {
		lruIndex := (i + c.lruOffset) % len(c.allLRU)
		current := c.allLRU[lruIndex]
		if current != lru && c.evictFromFor(current, e) {
			evicted = true
			break
		}
	}

	c.lruOffset++
	if c.lruOffset >= len(c.allLRU) {
		c.lruOffset = 0
	}

	return evicted
}

func (c *cache) allocateFor(e *entry) error {
	_, last := e.data()
	for {
		if s, ok := c.memory.allocate(); ok {
			if last != nil && s != last.next() {
				c.memory.move(s, last.next())
			}

			e.appendSegment(s)
			return nil
		}

		if !c.evictFor(e) {
			return errAllocationFailed
		}
	}
}

func (c *cache) writeKey(e *entry, key string) error {
	p := []byte(key)
	for len(p) > 0 {
		if err := c.allocateFor(e); err != nil {
			return err
		}

		n, _ := e.write(p)
		p = p[n:]
	}

	return nil
}

func (c *cache) lookup(id id) (*entry, bool) {
	for _, e := range c.hash[c.bucketIndex(id.hash)] {
		if e.keyspace == id.keyspace && e.keyEquals(id.key) {
			return e, true
		}
	}

	return nil, false
}

func (c *cache) addLookup(id id, e *entry) {
	index := c.bucketIndex(id.hash)
	c.hash[index] = append(c.hash[index], e)
}

func (c *cache) touchEntry(e *entry) {
	c.lru[e.keyspace].remove(e)
	c.lru[e.keyspace].insert(e, nil)
}

func (c *cache) removeKeyspaceLRU(lru *list, keyspace string) {
	delete(c.lru, keyspace)
	for i, current := range c.allLRU {
		if current == lru {
			last := len(c.allLRU) - 1
			c.allLRU[last], c.allLRU[i], c.allLRU = nil, c.allLRU[last], c.allLRU[:last]
			break
		}
	}
}

func (c *cache) keyspaceLRU(keyspace string) *list {
	lru := c.lru[keyspace]
	if lru == nil {
		lru = new(list)
		c.lru[keyspace] = lru
		c.allLRU = append(c.allLRU, lru)
	}

	return lru
}

func (c *cache) deleteLookup(e *entry) bool {
	index := c.bucketIndex(e.hash)
	bucket := c.hash[index]
	for i, ei := range bucket {
		if ei == e {
			last := len(bucket) - 1
			bucket[last], bucket[i], bucket = nil, bucket[last], bucket[:last]
			c.hash[index] = bucket
			return true
		}
	}

	// false means that it was already deleted from the lookup
	// but there were active readers at the time
	return false
}

func (c *cache) deleteEntry(e *entry) bool {
	if c.deleteLookup(e) {
		lru := c.lru[e.keyspace]
		lru.remove(e)
		if lru.empty() {
			c.removeKeyspaceLRU(lru, e.keyspace)
		}

		if e.reading > 0 {
			c.toDelete.insert(e, nil)
			return false
		}
	} else {
		if e.reading > 0 {
			return false
		}

		c.toDelete.remove(e)
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
	e.reading++
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

	lru := c.keyspaceLRU(e.keyspace)
	lru.insert(e, nil)
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

func (c *cache) waitRead() {
	c.readCond.L.Lock()
	c.readCond.Wait()
	c.readCond.L.Unlock()
}

func (c *cache) broadcastRead() {
	c.readCond.Broadcast()
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
	for _, lru := range c.lru {
		c.closeAll(lru)
	}

	c.memory = nil
	c.memory = nil
	c.toDelete = nil
	c.lru = nil
	c.hash = nil
	c.closed = true
}

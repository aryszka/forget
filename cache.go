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
	lruLookup                            map[string]*list
	lruList                              *list
	hash                                 [][]*entry
	closed                               bool
	status                               *InstanceStatus
}

var (
	errAllocationFailed = errors.New("allocation for key failed")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

func newCache(segmentCount, segmentSize int, notify *notify) *cache {
	return &cache{
		segmentCount: segmentCount,
		segmentSize:  segmentSize,
		mx:           &sync.RWMutex{},
		readCond:     sync.NewCond(&sync.Mutex{}),
		memory:       newMemory(segmentCount, segmentSize),
		lruLookup:    make(map[string]*list),
		lruList:      new(list),
		toDelete:     new(list),
		hash:         make([][]*entry, segmentCount), // there cannot be more entries than segments
		status:       newInstanceStatus(segmentSize, segmentCount*segmentSize, notify),
	}
}

func (c *cache) bucketIndex(hash uint64) int {
	return int(hash % uint64(c.segmentCount))
}

func (c *cache) evictFromFor(l *list, e *entry) bool {
	current := l.first
	for current != nil {
		if current != e && c.deleteEntry(current.(*entry)) {
			c.status.evict(e.keyspace, e.size)
			return true
		}

		current = current.next()
	}

	return false
}

func (c *cache) rotateLRUs(at node) {
	if at == nil || c.lruList.empty() {
		return
	}

	from := c.lruList.first
	c.lruList.removeRange(from, at)
	c.lruList.insertRange(from, at, nil)
}

func (c *cache) evictFor(e *entry) bool {
	if c.evictFromFor(c.toDelete, e) {
		return true
	}

	initial, ok := c.lruLookup[e.keyspace]
	if ok && c.evictFromFor(initial, e) {
		return true
	}

	// round robin the rest to evict one item
	lru, _ := c.lruList.first.(*list)
	for lru != nil {
		if lru != initial && c.evictFromFor(lru, e) {
			var rotateAt node = lru
			if lru.empty() {
				rotateAt = rotateAt.prev()
			}

			c.rotateLRUs(rotateAt)
			return true
		}

		lru, _ = lru.next().(*list)
	}

	return false
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
			c.status.allocFailed(e.keyspace)
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
	c.lruLookup[e.keyspace].remove(e)
	c.lruLookup[e.keyspace].insert(e, nil)
}

func (c *cache) removeKeyspaceLRU(lru *list, keyspace string) {
	delete(c.lruLookup, keyspace)
	c.lruList.remove(lru)
}

func (c *cache) keyspaceLRU(keyspace string) *list {
	lru := c.lruLookup[keyspace]
	if lru == nil {
		lru = new(list)
		c.lruLookup[keyspace] = lru
		c.lruList.insert(lru, nil)
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
		lru := c.lruLookup[e.keyspace]
		lru.remove(e)
		if lru.empty() {
			c.removeKeyspaceLRU(lru, e.keyspace)
		}

		if e.reading > 0 {
			c.toDelete.insert(e, nil)
			c.status.incReadersOnDeleted(e.keyspace)
			return false
		}
	} else {
		if e.reading > 0 {
			return false
		}

		c.toDelete.remove(e)
		c.status.decReadersOnDeleted(e.keyspace)
	}

	first, last := e.data()
	if first != nil {
		c.memory.free(first, last)
	}

	e.close()
	c.status.itemDeleted(e)
	return true
}

func (c *cache) get(id id) (*entry, bool) {
	if c.closed {
		return nil, false
	}

	e, ok := c.lookup(id)
	if !ok {
		c.status.miss(id.keyspace, id.key)
		return nil, false
	}

	if e.expired() {
		c.deleteEntry(e)
		c.status.expire(id.keyspace, id.key, e.size)
		return nil, false
	}

	c.touchEntry(e)
	e.reading++
	c.status.hit(id.keyspace, id.key)
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
	c.status.itemAdded(e)
	c.status.set(id.keyspace, id.key, e.keySize)

	return e, nil
}

func (c *cache) del(id id) {
	if c.closed {
		return
	}

	if e, ok := c.lookup(id); ok {
		c.deleteEntry(e)
		c.status.del(id.keyspace, id.key, e.size)
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
	lru := c.lruList.first
	for lru != nil {
		c.closeAll(lru.(*list))
		lru = lru.next()
	}

	c.memory = nil
	c.memory = nil
	c.toDelete = nil
	c.lruLookup = nil
	c.hash = nil
	c.closed = true
}

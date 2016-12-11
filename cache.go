package forget

import (
	"errors"
	"io"
	"sync"
	"time"
)

type cache struct {
	mx           *sync.RWMutex
	readDoneCond *sync.Cond
	memory       *memory
	deleteQueue  *list
	lruLookup    map[string]*list
	lruRotate    *list
	itemLookup   [][]*item
	closed       bool
	stats        *SegmentStats
}

var (
	errAllocationFailed = errors.New("allocation for key failed")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

func newCache(chunkCount, chunkSize int, notify *notify) *cache {
	return &cache{
		mx:           &sync.RWMutex{},
		readDoneCond: sync.NewCond(&sync.Mutex{}),
		memory:       newMemory(chunkCount, chunkSize),
		lruLookup:    make(map[string]*list),
		lruRotate:    &list{},
		deleteQueue:  &list{},
		itemLookup:   make([][]*item, chunkCount), // there cannot be more entries than chunks
		stats:        newSegmentStats(chunkSize, chunkCount*chunkSize, notify),
	}
}

// returns the index of the lookup bucket based on an item's hash
func (c *cache) bucketIndex(hash uint64) int {
	return int(hash % uint64(len(c.itemLookup)))
}

// finds an item based on the hash, keyspace and key
func (c *cache) lookup(hash uint64, keyspace, key string) (*item, bool) {
	for _, i := range c.itemLookup[c.bucketIndex(hash)] {
		if i.keyspace == keyspace && i.keyEquals(key) {
			return i, true
		}
	}

	return nil, false
}

// stores an item in the right lookup bucket
func (c *cache) addLookup(hash uint64, i *item) {
	index := c.bucketIndex(hash)
	c.itemLookup[index] = append(c.itemLookup[index], i)
	if len(c.itemLookup) > 1 {
		c.stats.incKeyCollisions(i.keyspace)
	}
}

func (c *cache) deleteLookup(i *item) {
	bi := c.bucketIndex(i.hash)
	bucket := c.itemLookup[bi]
	for bii, ii := range bucket {
		if ii == i {
			last := len(bucket) - 1
			bucket[last], bucket[bii], bucket = nil, bucket[last], bucket[:last]
			c.itemLookup[bi] = bucket
			if len(c.itemLookup) > 0 {
				c.stats.decKeyCollisions(i.keyspace)
			}

			return
		}
	}
}

// returns the LRU list for a keyspace. If it doesn't exist, creates it and stores it
func (c *cache) keyspaceLRU(keyspace string) *list {
	lru := c.lruLookup[keyspace]
	if lru == nil {
		lru = &list{}
		c.lruLookup[keyspace] = lru
		c.lruRotate.insert(lru, nil)
	}

	return lru
}

// removes an LRU list
func (c *cache) removeKeyspaceLRU(lru *list, keyspace string) {
	delete(c.lruLookup, keyspace)
	c.lruRotate.remove(lru)
}

func (c *cache) deleteFromLRU(i *item) {
	lru := c.lruLookup[i.keyspace]
	lru.remove(i)
	if lru.empty() {
		c.removeKeyspaceLRU(lru, i.keyspace)
	}
}

func canDelete(i *item) bool {
	return !i.writeComplete || i.readers == 0
}

func (c *cache) freeMemory(i *item) {
	first, last := i.data()
	if first != nil {
		c.memory.free(first, last)
	}
}

func (c *cache) deleteItem(i *item) bool {
	c.deleteLookup(i)
	c.deleteFromLRU(i)

	if canDelete(i) {
		c.freeMemory(i)
		i.close()
		return true
	}

	c.deleteQueue.insert(i, nil)
	return false
}

func canEvict(i *item) bool {
	return i.size > 0 && (!i.writeComplete || i.readers == 0)
}

func (c *cache) evictFromDeleted() bool {
	current := c.deleteQueue.first
	for current != nil {
		i := current.(*item)
		if !canEvict(i) {
			current = current.next()
			continue
		}

		c.deleteQueue.remove(i)
		c.freeMemory(i)
		i.close()

		c.stats.deleteItem(i)
		c.stats.decMarkedDeleted(i.keyspace)
		c.stats.notifyEvict(i.keyspace, i.size)

		return true
	}

	return false
}

// tries to evict one item from an lru list, other than the one in the args, with a size greater than 0 and has
// no active readers.
//
// TODO: this is not very self documenting. Current readers is checked but not writeComplete.
func (c *cache) evictFromLRU(lru *list, skip *item) (*item, bool) {
	current := lru.first
	for current != nil {
		i := current.(*item)
		if i == skip || !canEvict(i) {
			current = current.next()
			continue
		}

		c.deleteLookup(i)
		c.deleteFromLRU(i)
		c.freeMemory(i)
		i.close()

		return i, true
	}

	return nil, false
}

func (c *cache) evictFromOwnKeyspace(i *item) bool {
	keyspaceLRU, ok := c.lruLookup[i.keyspace]
	if !ok {
		return false
	}

	evictedItem, ok := c.evictFromLRU(keyspaceLRU, i)
	if !ok {
		return false
	}

	c.stats.deleteItem(evictedItem)
	c.stats.notifyEvict(evictedItem.keyspace, evictedItem.size)
	return true
}

// round-robin over the rest of the LRUs to evict one item
func (c *cache) evictFromOtherKeyspaces(i *item) bool {
	own := c.lruLookup[i.keyspace]
	lru, _ := c.lruRotate.first.(*list)
	for lru != nil {
		if lru == own {
			lru, _ = lru.next().(*list)
			continue
		}

		evictedItem, ok := c.evictFromLRU(lru, nil)
		if !ok {
			lru, _ = lru.next().(*list)
			continue
		}

		c.stats.deleteItem(evictedItem)
		c.stats.notifyEvict(evictedItem.keyspace, evictedItem.size)

		var rotateAt node = lru
		if lru.empty() {
			// if empty, it was removed, too
			rotateAt = rotateAt.prev()
		}

		c.lruRotate.rotate(rotateAt)
		return true
	}

	return false
}

// tries to evict one item, other than the one in the args. First tries from the temporary deleted items,
// checking if no reader is blocking them anymore. Next tries in the item's own keyspace. Last tries all other
// keyspaces in a round-robin fashion
func (c *cache) evictForItem(i *item) bool {
	if c.evictFromDeleted() {
		return true
	}

	if c.evictFromOwnKeyspace(i) {
		return true
	}

	return c.evictFromOtherKeyspaces(i)
}

// tries to allocate a single chunk in the free memory space. If it fails, tries to evict an item. When
// allocated, aligns the chunk with the item's existing chunks.
func (c *cache) allocateFor(i *item) error {
	s, ok := c.memory.allocate()
	if !ok {
		if c.evictForItem(i) {
			s, _ = c.memory.allocate()
		} else {
			c.stats.notifyAllocFailed(i.keyspace)
			return errAllocationFailed
		}
	}

	_, last := i.data()
	if last != nil && s != last.next() {
		c.memory.move(s, last.next())
	}

	i.appendChunk(s)
	return nil
}

// moves an item in its keyspace's LRU list to the end, meaning that it was the most recently used item in the
// keyspace
func (c *cache) touchItem(i *item) {
	c.lruLookup[i.keyspace].move(i, nil)
}

// tries to find an item based on its hash, key and keyspace. If found but expired, deletes it. When finds a
// valid item, it sets the item as the most recently used one in the LRU list of the keyspace
func (c *cache) get(hash uint64, keyspace, key string) (io.ReadCloser, bool) {
	c.mx.Lock()
	defer c.mx.Unlock()

	if c.closed {
		return nil, false
	}

	i, ok := c.lookup(hash, keyspace, key)
	if !ok {
		c.stats.notifyMiss(keyspace, key)
		return nil, false
	}

	if i.expired() {
		if c.deleteItem(i) {
			c.stats.deleteItem(i)
			c.stats.notifyExpireDelete(keyspace, key, i.size)
			return nil, false
		}

		c.stats.incMarkedDeleted(i.keyspace)
		c.stats.notifyExpire(keyspace, key)
		return nil, false
	}

	c.touchItem(i)
	i.readers++
	c.stats.notifyHit(keyspace, key)
	c.stats.incReaders(keyspace)
	return newReader(c, i), true
}

// allocates 0 or more chunks to fit the key with the item and saves the key bytes in memory
func (c *cache) writeKey(i *item, key string) error {
	p := []byte(key)
	for len(p) > 0 {
		if err := c.allocateFor(i); err != nil {
			return err
		}

		n, _ := i.write(p)
		p = p[n:]
	}

	return nil
}

// tries to create a new item. If one exists with the same keyspace and key, it deletes it. Stores the item in
// the lookup table and the LRU list of the keyspace. If memory cannot be allocated for the item key due to
// active readers holding too many existing items, it fails
func (c *cache) trySet(hash uint64, keyspace, key string, ttl time.Duration) (io.WriteCloser, error) {
	c.mx.Lock()
	defer c.mx.Unlock()

	if c.closed {
		return nil, ErrCacheClosed
	}

	if i, ok := c.lookup(hash, keyspace, key); ok {
		if c.deleteItem(i) {
			c.stats.deleteItem(i)
			c.stats.notifyDelete(keyspace, key, i.size)
		} else {
			c.stats.incMarkedDeleted(i.keyspace)
			c.stats.notifyDelete(keyspace, key, 0)
		}
	}

	i := newItem(hash, keyspace, len(key), ttl)
	if err := c.writeKey(i, key); err != nil {
		first, last := i.data()
		if first != nil {
			c.memory.free(first, last)
		}

		return nil, err
	}

	c.addLookup(hash, i)

	lru := c.keyspaceLRU(i.keyspace)
	lru.insert(i, nil)

	c.stats.addItem(i)
	c.stats.notifySet(keyspace, key, i.keySize)
	c.stats.incWriters(keyspace)

	return newWriter(c, i), nil
}

// creates a new item. If the item key cannot be stored due to active readers holding too many existing items,
// it blocks, and waits for the next reader to signal that it's done
func (c *cache) set(hash uint64, keyspace, key string, ttl time.Duration) (io.WriteCloser, bool) {
	for {
		if w, err := c.trySet(hash, keyspace, key, ttl); err == errAllocationFailed {
			// waiting to be able to store the key
			// condition checking happens during writing they key
			c.readDoneCond.L.Lock()
			c.readDoneCond.Wait()
			c.readDoneCond.L.Unlock()
		} else if err != nil {
			return nil, false
		} else {
			return w, true
		}
	}
}

// deletes an item if it can be found in the cache
func (c *cache) del(hash uint64, keyspace, key string) {
	c.mx.Lock()
	defer c.mx.Unlock()

	if c.closed {
		return
	}

	i, ok := c.lookup(hash, keyspace, key)
	if !ok {
		return
	}

	if c.deleteItem(i) {
		c.stats.deleteItem(i)
		c.stats.notifyDelete(keyspace, key, i.size)
		return
	}

	c.stats.incMarkedDeleted(i.keyspace)
	c.stats.notifyDelete(keyspace, key, 0)
}

func (c *cache) getStats() *SegmentStats {
	c.mx.Lock()
	defer c.mx.Unlock()
	return c.stats.clone()
}

// closes all items in a list
func closeAll(l *list) {
	i := l.first
	for i != nil {
		i.(*item).close()
		i = i.next()
	}
}

// closes the cache. It releases the allocated memory
func (c *cache) close() {
	c.mx.Lock()
	defer c.mx.Unlock()

	if c.closed {
		return
	}

	closeAll(c.deleteQueue)
	lru := c.lruRotate.first
	for lru != nil {
		closeAll(lru.(*list))
		lru = lru.next()
	}

	c.memory = nil
	c.deleteQueue = nil
	c.lruLookup = nil
	c.itemLookup = nil

	c.closed = true
}

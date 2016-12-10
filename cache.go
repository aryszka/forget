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
	stats        *InstanceStats
}

var (
	errAllocationFailed = errors.New("allocation for key failed")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

func newCache(segmentCount, segmentSize int, notify *notify) *cache {
	return &cache{
		mx:           &sync.RWMutex{},
		readDoneCond: sync.NewCond(&sync.Mutex{}),
		memory:       newMemory(segmentCount, segmentSize),
		lruLookup:    make(map[string]*list),
		lruRotate:    &list{},
		deleteQueue:  &list{},
		itemLookup:   make([][]*item, segmentCount), // there cannot be more entries than segments
		stats:        newInstanceStats(segmentSize, segmentCount*segmentSize, notify),
	}
}

// tries to evict one item from an lru list, other than the one in the args, with a size greater than 0 and has
// no active readers.
//
// TODO: this is not very self documenting. Current readers is checked but not writeComplete.
func (c *cache) evictForFrom(i *item, lru *list) bool {
	current := lru.first
	for current != nil {
		if currentItem := current.(*item); currentItem != i && currentItem.size > 0 &&
			currentItem.readers == 0 && c.deleteItem(currentItem) {

			c.stats.notifyEvict(currentItem.keyspace, currentItem.size)
			return true
		}

		current = current.next()
	}

	return false
}

// tries to evict one item, other than the one in the args. First tries from the temporary deleted items,
// checking if no reader is blocking them anymore. Next tries in the item's own keyspace. Last tries all other
// keyspaces in a round-robin fashion
func (c *cache) evictFor(i *item) bool {
	if c.evictForFrom(i, c.deleteQueue) {
		return true
	}

	keyspaceLRU, ok := c.lruLookup[i.keyspace]
	if ok && c.evictForFrom(i, keyspaceLRU) {
		return true
	}

	// round-robin over the rest to evict one item
	lru, _ := c.lruRotate.first.(*list)
	for lru != nil {
		if lru != keyspaceLRU && c.evictForFrom(i, lru) {
			var rotateAt node = lru
			if lru.empty() {
				// if empty, it was also removed
				rotateAt = rotateAt.prev()
			}

			c.lruRotate.rotate(rotateAt)
			return true
		}

		lru, _ = lru.next().(*list)
	}

	return false
}

// tries to allocate a single segment in the free memory space. If it fails, tries to evict an item. When
// allocated, aligns the segment with the item's existing segments.
func (c *cache) allocateFor(i *item) error {
	s, ok := c.memory.allocate()
	if !ok {
		if c.evictFor(i) {
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

	i.appendSegment(s)
	return nil
}

// allocates 0 or more segments to fit the key with the item and saves the key bytes in memory
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
}

// moves an item in its keyspace's LRU list to the end, meaning that it was the most recently used item in the
// keyspace
func (c *cache) touchItem(i *item) {
	c.lruLookup[i.keyspace].remove(i)
	c.lruLookup[i.keyspace].insert(i, nil)
}

// removes an LRU list
func (c *cache) removeKeyspaceLRU(lru *list, keyspace string) {
	delete(c.lruLookup, keyspace)
	c.lruRotate.remove(lru)
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

// deletes an item's lookup entry
func (c *cache) deleteLookup(i *item) bool {
	bi := c.bucketIndex(i.hash)
	bucket := c.itemLookup[bi]
	for bii, ii := range bucket {
		if ii == i {
			last := len(bucket) - 1
			bucket[last], bucket[bii], bucket = nil, bucket[last], bucket[:last]
			c.itemLookup[bi] = bucket
			return true
		}
	}

	// false means that it was already deleted from the lookup
	// but there were active readers at the time, to be found
	// in the delete queue
	return false
}

// deletes an item. If it has active readers, it preserves it in the queue of deleted items. If it can be
// completely deleted, releases the allocated memory for the item
//
// TODO: examine the different cases of delete
func (c *cache) deleteItem(i *item) bool {
	if c.deleteLookup(i) {
		lru := c.lruLookup[i.keyspace]
		lru.remove(i)
		if lru.empty() {
			c.removeKeyspaceLRU(lru, i.keyspace)
		}

		// TODO: this is not very self documenting. WriteComplete is only checked for allowing
		// cancelling cache fill
		if i.writeComplete && i.readers > 0 {
			c.deleteQueue.insert(i, nil)
			c.stats.incReadersOnDeleted(i.keyspace)
			return false
		}
	} else {
		if i.readers > 0 {
			return false
		}

		c.deleteQueue.remove(i)
		c.stats.decReadersOnDeleted(i.keyspace)
	}

	first, last := i.data()
	if first != nil {
		c.memory.free(first, last)
	}

	i.close()
	c.stats.deleteItem(i)
	return true
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
		c.deleteItem(i)
		c.stats.notifyExpire(keyspace, key, i.size)
		return nil, false
	}

	c.touchItem(i)
	i.readers++
	c.stats.notifyHit(keyspace, key)
	c.stats.incReaders(keyspace)
	return newReader(c, i), true
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

	c.deleteIfExists(hash, keyspace, key)

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

// deletes an item if it can be found in the lookup table
func (c *cache) deleteIfExists(hash uint64, keyspace, key string) {
	if i, ok := c.lookup(hash, keyspace, key); ok {
		c.deleteItem(i)
		c.stats.notifyDelete(keyspace, key, i.size)
	}
}

// deletes an item if it can be found in the cache
func (c *cache) del(hash uint64, keyspace, key string) {
	c.mx.Lock()
	defer c.mx.Unlock()

	if c.closed {
		return
	}

	c.deleteIfExists(hash, keyspace, key)
}

func (c *cache) getStats() *InstanceStats {
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

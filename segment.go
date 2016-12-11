package forget

import (
	"errors"
	"io"
	"sync"
	"time"
)

type segment struct {
	mx           *sync.RWMutex
	readDoneCond *sync.Cond
	memory       *memory
	deleteQueue  *list
	lruLookup    map[string]*list
	lruRotate    *list
	itemLookup   [][]*item
	closed       bool
	stats        *segmentStats
}

var (
	errAllocationFailed = errors.New("allocation for key failed")

	// ErrCacheClosed is returned when calling an operation on a closed cache.
	ErrCacheClosed = errors.New("cache closed")
)

func newCache(chunkCount, chunkSize int, notify *notify) *segment {
	return &segment{
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
func (s *segment) bucketIndex(hash uint64) int {
	return int(hash % uint64(len(s.itemLookup)))
}

// finds an item based on the hash, keyspace and key
func (s *segment) lookup(hash uint64, keyspace, key string) (*item, bool) {
	for _, i := range s.itemLookup[s.bucketIndex(hash)] {
		if i.keyspace == keyspace && i.keyEquals(key) {
			return i, true
		}
	}

	return nil, false
}

// stores an item in the right lookup bucket
func (s *segment) addLookup(hash uint64, i *item) {
	index := s.bucketIndex(hash)
	s.itemLookup[index] = append(s.itemLookup[index], i)
	if len(s.itemLookup) > 1 {
		s.stats.incKeyCollisions(i.keyspace)
	}
}

func (s *segment) deleteLookup(i *item) {
	bi := s.bucketIndex(i.hash)
	bucket := s.itemLookup[bi]
	for bii, ii := range bucket {
		if ii == i {
			last := len(bucket) - 1
			bucket[last], bucket[bii], bucket = nil, bucket[last], bucket[:last]
			s.itemLookup[bi] = bucket
			if len(s.itemLookup) > 0 {
				s.stats.decKeyCollisions(i.keyspace)
			}

			return
		}
	}
}

// returns the LRU list for a keyspace. If it doesn't exist, creates it and stores it
func (s *segment) keyspaceLRU(keyspace string) *list {
	lru := s.lruLookup[keyspace]
	if lru == nil {
		lru = &list{}
		s.lruLookup[keyspace] = lru
		s.lruRotate.insert(lru, nil)
	}

	return lru
}

// removes an LRU list
func (s *segment) removeKeyspaceLRU(lru *list, keyspace string) {
	delete(s.lruLookup, keyspace)
	s.lruRotate.remove(lru)
}

func (s *segment) deleteFromLRU(i *item) {
	lru := s.lruLookup[i.keyspace]
	lru.remove(i)
	if lru.empty() {
		s.removeKeyspaceLRU(lru, i.keyspace)
	}
}

func canDelete(i *item) bool {
	return !i.writeComplete || i.readers == 0
}

func (s *segment) freeMemory(i *item) {
	first, last := i.data()
	if first != nil {
		s.memory.free(first, last)
	}
}

func (s *segment) deleteItem(i *item) bool {
	s.deleteLookup(i)
	s.deleteFromLRU(i)

	if canDelete(i) {
		s.freeMemory(i)
		i.close()
		return true
	}

	s.deleteQueue.insert(i, nil)
	return false
}

func canEvict(i *item) bool {
	return i.size > 0 && (!i.writeComplete || i.readers == 0)
}

func (s *segment) evictFromDeleted() bool {
	current := s.deleteQueue.first
	for current != nil {
		i := current.(*item)
		if !canEvict(i) {
			current = current.next()
			continue
		}

		s.deleteQueue.remove(i)
		s.freeMemory(i)
		i.close()

		s.stats.deleteItem(i)
		s.stats.decMarkedDeleted(i.keyspace)
		s.stats.notifyEvict(i.keyspace, i.size)

		return true
	}

	return false
}

// tries to evict one item from an lru list, other than the one in the args, with a size greater than 0 and has
// no active readers.
//
// TODO: this is not very self documenting. Current readers is checked but not writeComplete.
func (s *segment) evictFromLRU(lru *list, skip *item) (*item, bool) {
	current := lru.first
	for current != nil {
		i := current.(*item)
		if i == skip || !canEvict(i) {
			current = current.next()
			continue
		}

		s.deleteLookup(i)
		s.deleteFromLRU(i)
		s.freeMemory(i)
		i.close()

		return i, true
	}

	return nil, false
}

func (s *segment) evictFromOwnKeyspace(i *item) bool {
	keyspaceLRU, ok := s.lruLookup[i.keyspace]
	if !ok {
		return false
	}

	evictedItem, ok := s.evictFromLRU(keyspaceLRU, i)
	if !ok {
		return false
	}

	s.stats.deleteItem(evictedItem)
	s.stats.notifyEvict(evictedItem.keyspace, evictedItem.size)
	return true
}

// round-robin over the rest of the LRUs to evict one item
func (s *segment) evictFromOtherKeyspaces(i *item) bool {
	own := s.lruLookup[i.keyspace]
	lru, _ := s.lruRotate.first.(*list)
	for lru != nil {
		if lru == own {
			lru, _ = lru.next().(*list)
			continue
		}

		evictedItem, ok := s.evictFromLRU(lru, nil)
		if !ok {
			lru, _ = lru.next().(*list)
			continue
		}

		s.stats.deleteItem(evictedItem)
		s.stats.notifyEvict(evictedItem.keyspace, evictedItem.size)

		var rotateAt node = lru
		if lru.empty() {
			// if empty, it was removed, too
			rotateAt = rotateAt.prev()
		}

		s.lruRotate.rotate(rotateAt)
		return true
	}

	return false
}

// tries to evict one item, other than the one in the args. First tries from the temporary deleted items,
// checking if no reader is blocking them anymore. Next tries in the item's own keyspace. Last tries all other
// keyspaces in a round-robin fashion
func (s *segment) evictForItem(i *item) bool {
	if s.evictFromDeleted() {
		return true
	}

	if s.evictFromOwnKeyspace(i) {
		return true
	}

	return s.evictFromOtherKeyspaces(i)
}

// tries to allocate a single chunk in the free memory space. If it fails, tries to evict an item. When
// allocated, aligns the chunk with the item's existing chunks.
func (s *segment) allocateFor(i *item) error {
	c, ok := s.memory.allocate()
	if !ok {
		if s.evictForItem(i) {
			c, _ = s.memory.allocate()
		} else {
			s.stats.notifyAllocFailed(i.keyspace)
			return errAllocationFailed
		}
	}

	_, last := i.data()
	if last != nil && c != last.next() {
		s.memory.move(c, last.next())
	}

	i.appendChunk(c)
	return nil
}

// moves an item in its keyspace's LRU list to the end, meaning that it was the most recently used item in the
// keyspace
func (s *segment) touchItem(i *item) {
	s.lruLookup[i.keyspace].move(i, nil)
}

// tries to find an item based on its hash, key and keyspace. If found but expired, deletes it. When finds a
// valid item, it sets the item as the most recently used one in the LRU list of the keyspace
func (s *segment) get(hash uint64, keyspace, key string) (io.ReadCloser, bool) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.closed {
		return nil, false
	}

	i, ok := s.lookup(hash, keyspace, key)
	if !ok {
		s.stats.notifyMiss(keyspace, key)
		return nil, false
	}

	if i.expired() {
		if s.deleteItem(i) {
			s.stats.deleteItem(i)
			s.stats.notifyExpireDelete(keyspace, key, i.size)
			return nil, false
		}

		s.stats.incMarkedDeleted(i.keyspace)
		s.stats.notifyExpire(keyspace, key)
		return nil, false
	}

	s.touchItem(i)
	i.readers++
	s.stats.notifyHit(keyspace, key)
	s.stats.incReaders(keyspace)
	return newReader(s, i), true
}

// allocates 0 or more chunks to fit the key with the item and saves the key bytes in memory
func (s *segment) writeKey(i *item, key string) error {
	p := []byte(key)
	for len(p) > 0 {
		if err := s.allocateFor(i); err != nil {
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
func (s *segment) trySet(hash uint64, keyspace, key string, ttl time.Duration) (io.WriteCloser, error) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.closed {
		return nil, ErrCacheClosed
	}

	if i, ok := s.lookup(hash, keyspace, key); ok {
		if s.deleteItem(i) {
			s.stats.deleteItem(i)
			s.stats.notifyDelete(keyspace, key, i.size)
		} else {
			s.stats.incMarkedDeleted(i.keyspace)
			s.stats.notifyDelete(keyspace, key, 0)
		}
	}

	i := newItem(hash, keyspace, len(key), ttl)
	if err := s.writeKey(i, key); err != nil {
		first, last := i.data()
		if first != nil {
			s.memory.free(first, last)
		}

		return nil, err
	}

	s.addLookup(hash, i)

	lru := s.keyspaceLRU(i.keyspace)
	lru.insert(i, nil)

	s.stats.addItem(i)
	s.stats.notifySet(keyspace, key, i.keySize)
	s.stats.incWriters(keyspace)

	return newWriter(s, i), nil
}

// creates a new item. If the item key cannot be stored due to active readers holding too many existing items,
// it blocks, and waits for the next reader to signal that it's done
func (s *segment) set(hash uint64, keyspace, key string, ttl time.Duration) (io.WriteCloser, bool) {
	for {
		if w, err := s.trySet(hash, keyspace, key, ttl); err == errAllocationFailed {
			// waiting to be able to store the key
			// condition checking happens during writing they key
			s.readDoneCond.L.Lock()
			s.readDoneCond.Wait()
			s.readDoneCond.L.Unlock()
		} else if err != nil {
			return nil, false
		} else {
			return w, true
		}
	}
}

// deletes an item if it can be found in the segment
func (s *segment) del(hash uint64, keyspace, key string) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.closed {
		return
	}

	i, ok := s.lookup(hash, keyspace, key)
	if !ok {
		return
	}

	if s.deleteItem(i) {
		s.stats.deleteItem(i)
		s.stats.notifyDelete(keyspace, key, i.size)
		return
	}

	s.stats.incMarkedDeleted(i.keyspace)
	s.stats.notifyDelete(keyspace, key, 0)
}

func (s *segment) getStats() *segmentStats {
	s.mx.Lock()
	defer s.mx.Unlock()
	return s.stats.clone()
}

// closes all items in a list
func closeAll(l *list) {
	i := l.first
	for i != nil {
		i.(*item).close()
		i = i.next()
	}
}

// closes the segment. It releases the allocated memory
func (s *segment) close() {
	s.mx.Lock()
	defer s.mx.Unlock()

	if s.closed {
		return
	}

	closeAll(s.deleteQueue)
	lru := s.lruRotate.first
	for lru != nil {
		closeAll(lru.(*list))
		lru = lru.next()
	}

	s.memory = nil
	s.deleteQueue = nil
	s.lruLookup = nil
	s.itemLookup = nil

	s.closed = true
}

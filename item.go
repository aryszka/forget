package forget

import (
	"sync"
	"time"
)

type item struct {
	hash                     uint64
	keyspace                 string
	keySize, size            int
	firstChunk, lastChunk    node
	chunkOffset              int
	expires                  time.Time
	readers                  int
	writeComplete, discarded bool
	writeCond                *sync.Cond
	prevItem, nextItem       node
}

func newItem(hash uint64, keyspace string, keySize int, ttl time.Duration) *item {
	return &item{
		hash:      hash,
		keyspace:  keyspace,
		keySize:   keySize,
		expires:   time.Now().Add(ttl),
		writeCond: sync.NewCond(&sync.Mutex{}),
	}
}

func (i *item) prev() node     { return i.prevItem }
func (i *item) next() node     { return i.nextItem }
func (i *item) setPrev(p node) { i.prevItem = p }
func (i *item) setNext(n node) { i.nextItem = n }

func (i *item) expired() bool {
	return i.expires.Before(time.Now())
}

func (i *item) data() (*chunk, *chunk) {
	if i.firstChunk == nil {
		return nil, nil
	}

	return i.firstChunk.(*chunk), i.lastChunk.(*chunk)
}

func (i *item) appendChunk(s *chunk) {
	if i.firstChunk == nil {
		i.firstChunk = s
	}

	i.lastChunk = s
	i.chunkOffset = 0
}

// checks if a key equals with the item's key. Keys are stored in the underlying chunks, if a key spans
// multiple chunks, keyEquals proceeds through the required number of chunks.
func (i *item) keyEquals(key string) bool {
	if len(key) != i.keySize {
		return false
	}

	p, s := []byte(key), i.firstChunk
	for len(p) > 0 && s != nil {
		ok, n := s.(*chunk).bytesEqual(0, p)
		if !ok {
			return false
		}

		p, s = p[n:], s.next()
	}

	return len(p) == 0
}

// writes from the last used chunk position of the last chunk, maximum to the end of the chunk.
func (i *item) write(p []byte) (int, error) {
	if i.discarded {
		return 0, ErrItemDiscarded
	}

	if i.lastChunk == nil {
		return 0, nil
	}

	n := i.lastChunk.(*chunk).write(i.chunkOffset, p)
	i.size += n
	i.chunkOffset += n

	return n, nil
}

// closes the item and releases the underlying chunks
func (i *item) close() {
	i.discarded = true
	i.firstChunk = nil
	i.lastChunk = nil
	i.prevItem = nil
	i.nextItem = nil
}

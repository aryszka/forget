package forget

import (
	"sync"
	"time"
)

type item struct {
	hash                      uint64
	keyspace                  string
	keySize, size             int
	firstSegment, lastSegment node
	segmentPosition           int
	expires                   time.Time
	readers                   int
	writeComplete, discarded  bool
	writeCond                 *sync.Cond
	prevItem, nextItem        node
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

func (i *item) data() (*segment, *segment) {
	if i.firstSegment == nil {
		return nil, nil
	}

	return i.firstSegment.(*segment), i.lastSegment.(*segment)
}

func (i *item) appendSegment(s *segment) {
	if i.firstSegment == nil {
		i.firstSegment = s
	}

	i.lastSegment = s
	i.segmentPosition = 0
}

// checks if a key equals with the item's key. Keys are stored in the underlying segments, if a key spans
// multiple segments, keyEquals proceeds through the required number of segments.
func (i *item) keyEquals(key string) bool {
	if len(key) != i.keySize {
		return false
	}

	p, s := []byte(key), i.firstSegment
	for len(p) > 0 && s != nil {
		ok, n := s.(*segment).bytesEqual(0, p)
		if !ok {
			return false
		}

		p, s = p[n:], s.next()
	}

	return len(p) == 0
}

// writes from the last used segment position of the last segment, maximum to the end of the segment.
func (i *item) write(p []byte) (int, error) {
	if i.discarded {
		return 0, ErrItemDiscarded
	}

	if i.lastSegment == nil {
		return 0, nil
	}

	n := i.lastSegment.(*segment).write(i.segmentPosition, p)
	i.size += n
	i.segmentPosition += n

	return n, nil
}

// closes the item and releases the underlying segments
func (i *item) close() {
	i.discarded = true
	i.firstSegment = nil
	i.lastSegment = nil
	i.prevItem = nil
	i.nextItem = nil
}

package forget

import (
	"errors"
	"io"
	"sync"
)

type entry struct {
	key                       string
	segmentSize, size         int // better store size here, because there is less entries than segments
	firstSegment, lastSegment node
	discarded, writeComplete  bool
	dataCond                  *sync.Cond
	prevEntry, nextEntry      node
}

var (
	// ErrItemDiscarded is returned by IO operations when an item has been discarded, e.g. evicted, deleted or the discarded
	// due to the cache was closed.
	ErrItemDiscarded = errors.New("item discarded")

	// // ErrItemWriteComplete is returned by write and close IO operations when an item's write was signalled to
	// // be complete by a previous call to Close().
	// ErrItemWriteComplete = errors.New("item write complete")
)

// entry doesn't hold a lock, but should be accessed in synchronized way
func newEntry(key string, segmentSize int) *entry {
	return &entry{
		key:         key,
		dataCond:    sync.NewCond(&sync.Mutex{}),
		segmentSize: segmentSize,
	}
}

func (e *entry) prev() node     { return e.prevEntry }
func (e *entry) next() node     { return e.nextEntry }
func (e *entry) setPrev(p node) { e.prevEntry = p }
func (e *entry) setNext(n node) { e.nextEntry = n }

func (e *entry) waitData() {
	e.dataCond.L.Lock()
	e.dataCond.Wait()
	e.dataCond.L.Unlock()
}

func (e *entry) broadcastData() {
	e.dataCond.Broadcast()
}

func (e *entry) data() (*segment, *segment) {
	if e.firstSegment == nil {
		return nil, nil
	}

	return e.firstSegment.(*segment), e.lastSegment.(*segment)
}

func (e *entry) appendSegment(s *segment) {
	if e.firstSegment == nil {
		e.firstSegment = s
	}

	e.lastSegment = s
}

func moveToPosition(segmentIndex, offset, segmentSize int) int {
	segmentOffset := offset - segmentIndex*segmentSize
	if segmentOffset < 0 {
		segmentOffset = 0
	}

	return segmentOffset
}

func (e *entry) read(offset int, p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded // TODO: decide on naming, entry or item
	}

	if offset >= e.size {
		if e.writeComplete {
			return 0, io.EOF
		}

		return 0, nil
	}

	if offset+len(p) > e.size {
		p = p[:e.size-offset]
	}

	currentSegment, segmentIndex, count := e.firstSegment, 0, 0
	for len(p) > 0 {
		segmentOffset := moveToPosition(segmentIndex, offset, e.segmentSize)
		n := currentSegment.(*segment).read(segmentOffset, p)
		p = p[n:]
		count += n
		currentSegment = currentSegment.next()
		segmentIndex++
	}

	return count, nil
}

func (e *entry) write(p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded // TODO: decide on naming, entry or item
	}

	if e.lastSegment == nil {
		return 0, nil
	}

	n := e.lastSegment.(*segment).write(e.size%e.segmentSize, p)
	e.size += n

	return n, nil
}

func (e *entry) closeWrite() error {
	e.writeComplete = true
	return nil
}

func (e *entry) close() {
	e.discarded = true
	e.firstSegment = nil
	e.lastSegment = nil
}

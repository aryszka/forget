package forget

import (
	"errors"
	"io"
	"sync"
)

type entry struct {
	segmentSize, size         int // better store size here, because there is less entries than segments
	firstSegment, lastSegment node
	discarded, writeComplete  bool
	dataCond                  *sync.Cond
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
func newEntry(segmentSize int) *entry {
	return &entry{
		dataCond:    sync.NewCond(&sync.Mutex{}),
		segmentSize: segmentSize,
	}
}

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

func moveToPosition(segmentIndex, offset, segmentSize int) (int, bool) {
	if (segmentIndex+1)*segmentSize <= offset {
		return 0, false
	}

	segmentOffset := offset - segmentIndex*segmentSize
	if segmentOffset < 0 {
		segmentOffset = 0
	}

	return segmentOffset, true
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
		if currentSegment == nil {
			if e.writeComplete {
				return count, io.EOF
			}

			return count, nil
		}

		segmentOffset, ok := moveToPosition(segmentIndex, offset, e.segmentSize)
		if !ok {
			currentSegment = currentSegment.next()
			segmentIndex++
			continue
		}

		n, err := currentSegment.(*segment).read(segmentOffset, p)
		p = p[n:]
		count += n

		if err != nil && err != io.EOF {
			return count, err
		}

		currentSegment = currentSegment.next()
		segmentIndex++

		// case of not reading as much as possible not considered
	}

	return count, nil
}

func (e *entry) write(p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded // TODO: decide on naming, entry or item
	}

	count := 0
	for len(p) > 0 {
		if e.lastSegment == nil {
			return count, nil
		}

		n, err := e.lastSegment.(*segment).write(e.size%e.segmentSize, p)
		p = p[n:]
		count += n
		e.size += n
		if err != nil {
			return count, err
		}

		if n == 0 {
			e.lastSegment = e.lastSegment.next()
		}
	}

	return count, nil

	/*
		currentSegment, segmentIndex, count := e.firstSegment, 0, 0
		for len(p) > 0 && currentSegment != nil {
			segmentOffset, ok := moveToPosition(segmentIndex, e.size, e.segmentSize)
			if !ok {
				if currentSegment == e.lastSegment {
					currentSegment = nil
				} else {
					currentSegment = currentSegment.next()
				}

				segmentIndex++
				continue
			}

			n, err := currentSegment.(*segment).write(segmentOffset, p)
			p = p[n:]
			count += n
			e.size += n

			if err != nil {
				return count, err
			}

			currentSegment = currentSegment.next()
			segmentIndex++

			// case of not written full despite enough data ignored
		}

		return count, nil
	*/
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

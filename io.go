package forget

import (
	"errors"
	"io"
	"sync"
)

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

type cio struct {
	mx       rwLocker
	readCond *sync.Cond
	entry    *entry
}

type reader struct {
	*cio
	currentSegment               node
	segmentSize, segmentPosition int
}

type writer struct {
	*cio
	cache *cache
	size  int
}

var (
	// ErrItemDiscarded is returned by IO operations when an item has been discarded, e.g. evicted, deleted or the discarded
	// due to the cache was closed.
	ErrItemDiscarded = errors.New("item discarded")

	// ErrWriteLimit is returned when writing to an item fills the available size.
	ErrWriteLimit = errors.New("write limit")

	// ErrWriterClosed is returned when writing to or closing a writer that was already closed before.
	ErrWriterClosed = errors.New("writer closed")
)

func newReader(mx rwLocker, readCond *sync.Cond, e *entry, segmentSize int) *reader {
	return &reader{
		cio:             &cio{mx: mx, readCond: readCond, entry: e},
		segmentSize:     segmentSize,
		currentSegment:  e.firstSegment,
		segmentPosition: e.keySize,
	}
}

func newWriter(mx rwLocker, readCond *sync.Cond, c *cache, e *entry) *writer {
	return &writer{
		cio:   &cio{mx: mx, readCond: readCond, entry: e},
		cache: c,
	}
}

func (r *reader) readOne(p []byte) (int, error) {
	r.mx.RLock()
	defer r.mx.RUnlock()

	if r.entry.discarded {
		return 0, ErrItemDiscarded
	}

	if r.currentSegment != r.entry.lastSegment && r.segmentPosition == r.segmentSize {
		r.currentSegment = r.currentSegment.next()
		r.segmentPosition = 0
	}

	if r.currentSegment == r.entry.lastSegment &&
		r.segmentPosition+len(p) > r.entry.segmentPosition {

		p = p[:r.entry.segmentPosition-r.segmentPosition]
		if len(p) == 0 {
			if r.entry.writeComplete {
				return 0, io.EOF
			}

			return 0, nil
		}
	}

	n := r.currentSegment.(*segment).read(r.segmentPosition, p)
	r.segmentPosition += n
	return n, nil
}

func (r *reader) Read(p []byte) (int, error) {
	var count int
	for len(p) > 0 {
		n, err := r.readOne(p)
		p = p[n:]
		count += n

		if err != nil || n == 0 && count > 0 {
			return count, err
		}

		if n == 0 {
			r.entry.waitWrite()
		}
	}

	return count, nil
}

func (r *reader) Close() error {
	r.mx.Lock()
	defer r.readCond.Broadcast()
	defer r.mx.Unlock()

	r.entry.decReading()
	r.entry = nil
	return nil
}

func (w *writer) writeOne(p []byte) (int, bool, error) {
	w.mx.Lock()
	defer w.entry.broadcastWrite()
	defer w.mx.Unlock()

	n, err := w.entry.write(p)

	var allocationFailed bool
	if n < len(p) && err == nil {
		allocationFailed = !w.cache.allocateFor(w.entry)
	}

	return n, allocationFailed, err
}

func (w *writer) waitReadCond() {
	w.readCond.L.Lock()
	w.readCond.Wait()
	w.readCond.L.Unlock()
}

func (w *writer) Write(p []byte) (int, error) {
	if w.entry == nil {
		return 0, ErrWriterClosed
	}

	if len(p) == 0 {
		return 0, nil
	}

	max := w.cache.segmentCount*w.cache.segmentSize - w.entry.keySize
	var count int
	for len(p) > 0 {
		n, allocationFailed, err := w.writeOne(p)
		p = p[n:]
		count += n
		w.size += n

		if err != nil {
			return count, err
		}

		if len(p) > 0 && w.size == max {
			return count, ErrWriteLimit
		}

		if allocationFailed {
			w.waitReadCond()
		}
	}

	return count, nil
}

func (w *writer) Close() error {
	if w.entry == nil {
		return ErrWriterClosed
	}

	w.mx.Lock()
	defer w.entry.broadcastWrite()
	defer w.mx.Unlock()

	err := w.entry.closeWrite()
	w.entry = nil
	w.cache = nil
	return err
}

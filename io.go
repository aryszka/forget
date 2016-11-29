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
	cache *cache
	entry *entry
}

type reader struct {
	*cio
	currentSegment               node
	segmentSize, segmentPosition int
}

type writer struct {
	*cio
	max, size int
}

var (
	// ErrItemDiscarded is returned by IO operations when an item has been discarded, e.g. evicted, deleted or the discarded
	// due to the cache was closed.
	ErrItemDiscarded = errors.New("item discarded")

	// ErrWriteLimit is returned when writing to an item fills the available size.
	ErrWriteLimit = errors.New("write limit")

	// ErrReaderClosed is returned when reading from or closing a reader that was already closed before.
	ErrReaderClosed = errors.New("writer closed")

	// ErrWriterClosed is returned when writing to or closing a writer that was already closed before.
	ErrWriterClosed = errors.New("writer closed")
)

func newReader(c *cache, e *entry, segmentSize int) *reader {
	return &reader{
		cio:             &cio{cache: c, entry: e},
		segmentSize:     segmentSize,
		currentSegment:  e.firstSegment,
		segmentPosition: e.keySize,
	}
}

func (r *reader) readOne(p []byte) (int, error) {
	r.cache.mx.RLock()
	defer r.cache.mx.RUnlock()

	if r.entry == nil {
		return 0, ErrReaderClosed
	}

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
	r.cache.mx.Lock()
	defer r.cache.broadcastRead()
	defer r.cache.mx.Unlock()

	if r.entry == nil {
		return ErrReaderClosed
	}

	r.cache.status.decReaders(r.entry.keyspace)

	r.entry.reading--
	r.entry = nil
	r.currentSegment = nil

	return nil
}

func newWriter(c *cache, e *entry) *writer {
	return &writer{
		cio: &cio{cache: c, entry: e},
		max: c.segmentCount*c.segmentSize - e.keySize,
	}
}

func (w *writer) writeOne(p []byte, unblock bool) (int, error) {
	w.cache.mx.Lock()
	defer w.entry.broadcastWrite()
	defer w.cache.mx.Unlock()

	// only for statistics
	if unblock {
		w.cache.status.decWritersBlocked(w.entry.keyspace)
	}

	if w.entry.writeComplete {
		return 0, ErrWriterClosed
	}

	n, err := w.entry.write(p)
	if n == len(p) || err != nil {
		return n, err
	}

	err = w.cache.allocateFor(w.entry)
	if err != nil {
		w.cache.status.incWritersBlocked(w.entry.keyspace)
	}

	return n, err
}

func (w *writer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	var (
		blocked bool
		count   int
	)
	for len(p) > 0 {
		n, err := w.writeOne(p, blocked)
		p = p[n:]
		count += n
		w.size += n

		if err != nil && err != errAllocationFailed {
			return count, err
		}

		if len(p) > 0 && w.size == w.max {
			return count, ErrWriteLimit
		}

		if err == errAllocationFailed {
			blocked = true
			w.cache.waitRead()
		}
	}

	return count, nil
}

func (w *writer) Close() error {
	defer w.entry.broadcastWrite()

	w.cache.mx.Lock()
	defer w.cache.mx.Unlock()

	if w.entry.writeComplete {
		return ErrWriterClosed
	}

	w.entry.writeComplete = true
	w.cache.status.decWriters(w.entry.keyspace)
	w.cache.status.writeComplete(w.entry.keyspace, w.entry.keySize, w.size)
	return nil
}

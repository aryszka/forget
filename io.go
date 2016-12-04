package forget

import (
	"errors"
	"io"
)

type cacheIO struct {
	cache *cache
	item  *item
}

type reader struct {
	*cacheIO
	currentSegment  node
	segmentPosition int
}

type writer struct {
	*cacheIO
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

func newReader(c *cache, i *item) *reader {
	return &reader{
		cacheIO:         &cacheIO{cache: c, item: i},
		currentSegment:  i.firstSegment,
		segmentPosition: i.keySize,
	}
}

func (r *reader) readOne(p []byte) (int, error) {
	r.cache.mx.RLock()
	defer r.cache.mx.RUnlock()

	if r.item == nil {
		return 0, ErrReaderClosed
	}

	if r.item.discarded {
		return 0, ErrItemDiscarded
	}

	if r.currentSegment != r.item.lastSegment &&
		r.segmentPosition == r.cache.memory.segmentSize {

		r.currentSegment = r.currentSegment.next()
		r.segmentPosition = 0
	}

	if r.currentSegment == r.item.lastSegment &&
		r.segmentPosition+len(p) > r.item.segmentPosition {

		p = p[:r.item.segmentPosition-r.segmentPosition]
		if len(p) == 0 {
			if r.item.writeComplete {
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

		// only block if there was nothing to read and no error
		if n == 0 {
			// here waiting only for a change, condition checking happens during the actual read
			r.item.writeCond.L.Lock()
			r.item.writeCond.Wait()
			r.item.writeCond.L.Unlock()
		}
	}

	return count, nil
}

func (r *reader) Close() error {
	defer r.cache.readDoneCond.Broadcast()

	r.cache.mx.Lock()
	defer r.cache.mx.Unlock()

	if r.item == nil {
		return ErrReaderClosed
	}

	r.cache.stats.decReaders(r.item.keyspace)

	r.item.readers--
	r.item = nil
	r.currentSegment = nil

	return nil
}

func newWriter(c *cache, i *item) *writer {
	return &writer{
		cacheIO: &cacheIO{cache: c, item: i},
		max:     c.memory.segmentCount*c.memory.segmentSize - i.keySize,
	}
}

func (w *writer) writeOne(p []byte, unblock bool) (int, error) {
	defer w.item.writeCond.Broadcast()

	w.cache.mx.Lock()
	defer w.cache.mx.Unlock()

	// only for statistics
	if unblock {
		w.cache.stats.decWritersBlocked(w.item.keyspace)
	}

	if w.item.writeComplete {
		return 0, ErrWriterClosed
	}

	n, err := w.item.write(p)
	if n == len(p) || err != nil {
		return n, err
	}

	// this assumes that Write continues until p is fully drained.
	err = w.cache.allocateFor(w.item)
	if err != nil {
		w.cache.stats.incWritersBlocked(w.item.keyspace)
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

			// here waiting only for a change, condition checking happens during the actual write
			w.cache.readDoneCond.L.Lock()
			w.cache.readDoneCond.Wait()
			w.cache.readDoneCond.L.Unlock()
		}
	}

	return count, nil
}

func (w *writer) Close() error {
	defer w.item.writeCond.Broadcast()

	w.cache.mx.Lock()
	defer w.cache.mx.Unlock()

	if w.item.writeComplete {
		return ErrWriterClosed
	}

	w.item.writeComplete = true
	w.cache.stats.decWriters(w.item.keyspace)
	w.cache.stats.notifyWriteComplete(w.item.keyspace, w.item.keySize, w.size)
	return nil
}

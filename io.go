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
	// ErrItemDiscarded is returned by IO operations when an item has been discarded, e.g. evicted, deleted or
	// the discarded due to the cache was closed.
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

// reads from the item's current underlying segment once at the current segment position. If reached the end of
// the segment, it steps first to the next segment and resets the segment position. If reached end of the item
// and the write to the item is complete, returns EOF, if reached the end but the item is still being written,
// returns nil
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

// reads from an item at the current position. If 0 bytes were read and the item is still being written, it
// blocks.
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
			// only block if there was nothing to read and no error.
			// here waiting only for a change, condition checking happens during the actual read
			r.item.writeCond.L.Lock()
			r.item.writeCond.Wait()
			r.item.writeCond.L.Unlock()
		}
	}

	return count, nil
}

// closes the reader and signals read done.
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

// writes to an item. If the writer was closed or the max item size was reached, returns an error. If the last
// segment of the item is full, tries to allocate a new segment. If allocation fails due to too many active
// readers, the write blocks until allocation becomes possible.
func (w *writer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	var (
		blocked bool
		count   int
	)

	for len(p) > 0 {
		err := func() error {
			defer w.item.writeCond.Broadcast()

			w.cache.mx.Lock()
			defer w.cache.mx.Unlock()

			// only for statistics
			if blocked {
				w.cache.stats.decWritersBlocked(w.item.keyspace)
			}

			if w.item.writeComplete {
				return ErrWriterClosed
			}

			n, err := w.item.write(p)
			p = p[n:]
			count += n
			w.size += n

			if len(p) == 0 || err != nil {
				return err
			}

			if len(p) > 0 && w.size == w.max {
				return ErrWriteLimit
			}

			err = w.cache.allocateFor(w.item)
			if err == errAllocationFailed {
				w.cache.stats.incWritersBlocked(w.item.keyspace)
			}

			return err
		}()

		if err == errAllocationFailed {
			blocked = true

			// here waiting only for a change, condition checking happens during the actual write
			w.cache.readDoneCond.L.Lock()
			w.cache.readDoneCond.Wait()
			w.cache.readDoneCond.L.Unlock()
		} else if err != nil {
			return count, err
		}
	}

	return count, nil
}

// closes the writer and signals write complete
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

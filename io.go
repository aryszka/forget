package forget

import (
	"errors"
	"io"
)

type cacheIO struct {
	segment *segment
	item    *item
}

type reader struct {
	*cacheIO
	currentChunk  node
	chunkPosition int
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

func newReader(s *segment, i *item) *reader {
	return &reader{
		cacheIO:       &cacheIO{segment: s, item: i},
		currentChunk:  i.firstChunk,
		chunkPosition: i.keySize,
	}
}

// reads from the item's current underlying chunk once at the current chunk position. If reached the end of
// the chunk, it steps first to the next chunk and resets the chunk position. If reached end of the item
// and the write to the item is complete, returns EOF, if reached the end but the item is still being written,
// returns nil
func (r *reader) readOne(p []byte) (int, error) {
	r.segment.mx.RLock()
	defer r.segment.mx.RUnlock()

	if r.item == nil {
		return 0, ErrReaderClosed
	}

	if r.item.discarded {
		return 0, ErrItemDiscarded
	}

	if r.currentChunk != r.item.lastChunk &&
		r.chunkPosition == r.segment.memory.chunkSize {

		r.currentChunk = r.currentChunk.next()
		r.chunkPosition = 0
	}

	if r.currentChunk == r.item.lastChunk &&
		r.chunkPosition+len(p) > r.item.chunkPosition {

		p = p[:r.item.chunkPosition-r.chunkPosition]
		if len(p) == 0 {
			if r.item.writeComplete {
				return 0, io.EOF
			}

			return 0, nil
		}
	}

	n := r.currentChunk.(*chunk).read(r.chunkPosition, p)
	r.chunkPosition += n
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
	defer r.segment.readDoneCond.Broadcast()

	r.segment.mx.Lock()
	defer r.segment.mx.Unlock()

	if r.item == nil {
		return ErrReaderClosed
	}

	r.item.readers--
	r.segment.stats.decReaders(r.item.keyspace)

	r.item = nil
	r.currentChunk = nil

	return nil
}

func newWriter(s *segment, i *item) *writer {
	return &writer{
		cacheIO: &cacheIO{segment: s, item: i},
		max:     s.memory.chunkCount*s.memory.chunkSize - i.keySize,
	}
}

// writes to an item. If the writer was closed or the max item size was reached, returns an error. If the last
// chunk of the item is full, tries to allocate a new chunk. If allocation fails due to too many active
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

			w.segment.mx.Lock()
			defer w.segment.mx.Unlock()

			// only for statistics
			if blocked {
				w.segment.stats.decWritersBlocked(w.item.keyspace)
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

			err = w.segment.allocateFor(w.item)
			if err == errAllocationFailed {
				w.segment.stats.incWritersBlocked(w.item.keyspace)
			}

			return err
		}()

		if err == errAllocationFailed {
			blocked = true

			// here waiting only for a change, condition checking happens during the actual write
			w.segment.readDoneCond.L.Lock()
			w.segment.readDoneCond.Wait()
			w.segment.readDoneCond.L.Unlock()
		} else if err != nil {
			return count, err
		}
	}

	return count, nil
}

// closes the writer and signals write complete
func (w *writer) Close() error {
	defer w.item.writeCond.Broadcast()

	w.segment.mx.Lock()
	defer w.segment.mx.Unlock()

	if w.item.writeComplete {
		return ErrWriterClosed
	}

	w.item.writeComplete = true
	w.segment.stats.decWriters(w.item.keyspace)
	w.segment.stats.notifyWriteComplete(w.item.keyspace, w.item.keySize, w.size)
	return nil
}

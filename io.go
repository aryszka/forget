package forget

import (
	"errors"
	"io"
	"sync"
)

type cio struct {
	mx    *sync.Mutex
	entry *entry
}

type reader struct {
	*cio
	currentSegment               node
	segmentSize, segmentPosition int
}

type writer struct {
	*cio
	cache *cache
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

func newReader(mx *sync.Mutex, e *entry, segmentSize int) *reader {
	return &reader{
		cio:             &cio{mx: mx, entry: e},
		segmentSize:     segmentSize,
		currentSegment:  e.firstSegment,
		segmentPosition: e.keySize,
	}
}

func newWriter(mx *sync.Mutex, c *cache, e *entry) *writer {
	return &writer{
		cio:   &cio{mx: mx, entry: e},
		cache: c,
	}
}

func (r *reader) Read(p []byte) (int, error) {
	var count int
	for {
		r.mx.Lock()

		if r.entry.discarded {
			r.mx.Unlock()
			return count, ErrItemDiscarded
		}

		if r.currentSegment == r.entry.lastSegment && len(p) > r.entry.segmentPosition {
			max := r.entry.segmentPosition - r.segmentPosition
			if max == 0 {
				if r.entry.writeComplete {
					r.mx.Unlock()
					return 0, io.EOF
				}

				r.mx.Unlock()
				r.entry.waitData()
				continue
			}

			p = p[:max]
		}

		if len(p) == 0 {
			r.mx.Unlock()
			return count, nil
		}

		if r.currentSegment == nil ||
			r.currentSegment == r.entry.lastSegment &&
				r.segmentPosition == r.entry.segmentPosition {

			if r.entry.writeComplete {
				r.mx.Unlock()
				return count, io.EOF
			}

			if count > 0 {
				r.mx.Unlock()
				return count, nil
			}

			r.mx.Unlock()
			r.entry.waitData()
			continue
		}

		n := r.currentSegment.(*segment).read(r.segmentPosition, p)
		p = p[n:]
		count += n
		r.segmentPosition += n

		if r.segmentPosition == r.segmentSize {
			if r.currentSegment == r.entry.lastSegment {
				r.currentSegment = nil
			} else {
				r.currentSegment = r.currentSegment.next()
				r.segmentPosition = 0
			}
		}

		r.mx.Unlock()
	}
}

func (r *reader) Close() error {
	r.entry = nil
	return nil
}

func (w *writer) Write(p []byte) (int, error) {
	if w.entry == nil {
		return 0, ErrWriterClosed
	}

	if len(p) == 0 {
		return 0, nil
	}

	w.mx.Lock()
	defer w.mx.Unlock()

	var count int
	for {
		n, err := w.entry.write(p)
		p = p[n:]
		count += n
		if len(p) == 0 || err != nil {
			return count, err
		}

		if !w.cache.allocateFor(w.entry) {
			return count, ErrWriteLimit
		}
	}
}

func (w *writer) Close() error {
	if w.entry == nil {
		return ErrWriterClosed
	}

	err := func() error {
		w.mx.Lock()
		defer w.mx.Unlock()
		return w.entry.closeWrite()
	}()

	w.entry.broadcastData()
	w.entry = nil
	w.cache = nil
	return err
}

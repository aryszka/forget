package forget

import (
	"errors"
	"sync"
)

type cio struct {
	mx     *sync.Mutex
	entry  *entry
	offset int
}

type reader struct {
	*cio
}

type writer struct {
	*cio
	cache *cache
}

var (
	ErrWriteLimit   = errors.New("write limit")
	ErrWriterClosed = errors.New("writer closed")
)

func newReader(mx *sync.Mutex, e *entry) *reader {
	return &reader{cio: &cio{mx: mx, entry: e}}
}

func newWriter(mx *sync.Mutex, c *cache, e *entry) *writer {
	return &writer{
		cio:   &cio{mx: mx, entry: e},
		cache: c,
	}
}

func (r *reader) read(p []byte) (int, error) {
	r.mx.Lock()
	defer r.mx.Unlock()
	return r.entry.read(r.offset, p)
}

func (r *reader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	n, err := r.read(p)
	for n == 0 && err == nil {
		r.entry.waitData()
		n, err = r.read(p)
	}

	r.offset += n
	return n, err
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
		w.offset += n
		p = p[n:]
		count += n
		if len(p) == 0 || err != nil {
			return count, err
		}

		if ok := w.cache.allocateFor(w.entry); !ok {
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

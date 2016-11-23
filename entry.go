package forget

import (
	"errors"
	"io"
	"sync"
)

type entry struct {
	data                     []byte
	discarded, writeComplete bool
	dataCond                 *sync.Cond
}

var (
	// ErrItemDiscarded is returned by IO operations when an item has been discarded, e.g. evicted, deleted or the discarded
	// due to the cache was closed.
	ErrItemDiscarded = errors.New("item discarded")

	// ErrItemWriteComplete is returned by write and close IO operations when an item's write was signalled to
	// be complete by a previous call to Close().
	ErrItemWriteComplete = errors.New("item write complete")
)

// entry doesn't hold a lock, but should be accessed in synchronized way
func newEntry() *entry {
	return &entry{dataCond: sync.NewCond(&sync.Mutex{})}
}

func (e *entry) waitData() {
	e.dataCond.L.Lock()
	e.dataCond.Wait()
	e.dataCond.L.Unlock()
}

func (e *entry) broadcastData() {
	e.dataCond.Broadcast()
}

func (e *entry) read(offset int, p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded // TODO: decide on naming, entry or item
	}

	if offset >= len(e.data) {
		if e.writeComplete {
			return 0, io.EOF
		}

		return 0, nil
	}

	return copy(p, e.data[offset:]), nil
}

func (e *entry) write(p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded // TODO: decide on naming, entry or item
	}

	if e.writeComplete {
		return 0, ErrItemWriteComplete
	}

	e.data = append(e.data, p...)
	return len(p), nil
}

func (e *entry) closeWrite() error {
	if e.writeComplete {
		return ErrItemWriteComplete
	}

	e.writeComplete = true
	return nil
}

func (e *entry) close() {
	e.discarded = true
	e.data = nil
}

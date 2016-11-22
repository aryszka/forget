package forget

import "errors"

type ioMessageType int

const (
	readMsg ioMessageType = iota
	writeMsg
)

type ioMessage struct {
	typ      ioMessageType
	entry    *entry
	offset   int
	buffer   []byte
	response chan<- ioMessage
	count    int
	err      error
	wait     chan<- struct{}
}

type rw struct {
	req    chan<- ioMessage
	entry  *entry
	offset int
	quit   chan struct{}
}

type discarded struct{}

var (
	// ErrCacheClosed is returned when the cache gets closed while
	// reading or writing from a cached item.
	ErrCacheClosed = errors.New("cache closed")

	// ErrItemDiscarded is returned when an item gets deleted,
	// overwritten or evicted from the cache while reading or writing to
	// it.
	ErrItemDiscarded = errors.New("item discarded")

	// ErrWriteLimit is returned when trying to write more data to a
	// cached item than the space that was allocated for it.
	ErrWriteLimit = errors.New("write limit")
)

var discardedIO = discarded{}

func (rw *rw) Read(p []byte) (int, error) {
	for {
		rsp := make(chan ioMessage)
		wait := make(chan struct{})

		select {
		case rw.req <- ioMessage{
			typ:      readMsg,
			entry:    rw.entry,
			offset:   rw.offset,
			buffer:   p,
			response: rsp,
			wait:     wait,
		}:
		case <-rw.quit:
			return 0, ErrCacheClosed
		}

		m := <-rsp
		rw.offset += m.count
		if m.count == 0 && m.err == nil {
			select {
			case <-wait:
			case <-rw.quit:
				return 0, ErrCacheClosed
			}

			continue
		}

		return m.count, m.err
	}
}

func (rw *rw) Write(p []byte) (int, error) {
	rsp := make(chan ioMessage)

	select {
	case rw.req <- ioMessage{typ: writeMsg, entry: rw.entry, offset: rw.offset, buffer: p, response: rsp}:
	case <-rw.quit:
		return 0, ErrCacheClosed
	}

	m := <-rsp
	rw.offset += m.count
	return m.count, m.err
}

func (d discarded) Read([]byte) (int, error)  { return 0, ErrItemDiscarded }
func (d discarded) Write([]byte) (int, error) { return 0, ErrItemDiscarded }

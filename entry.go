package forget

import "sync"

type entry struct {
	hash                      uint64
	keySize                   int
	segmentPosition           int
	firstSegment, lastSegment node
	discarded, writeComplete  bool
	dataCond                  *sync.Cond
	prevEntry, nextEntry      node
}

// entry doesn't hold a lock, but should be accessed in synchronized way
func newEntry(hash uint64, keySize int) *entry {
	return &entry{
		hash:     hash,
		keySize:  keySize,
		dataCond: sync.NewCond(&sync.Mutex{}),
	}
}

func (e *entry) prev() node     { return e.prevEntry }
func (e *entry) next() node     { return e.nextEntry }
func (e *entry) setPrev(p node) { e.prevEntry = p }
func (e *entry) setNext(n node) { e.nextEntry = n }

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
	e.segmentPosition = 0
}

func (e *entry) readKey() string {
	if e.firstSegment == nil {
		return ""
	}

	k := make([]byte, e.keySize)
	s := e.firstSegment
	p := k
	for len(p) > 0 {
		n := s.(*segment).read(0, p)
		p = p[n:]
		s = s.next()
	}

	return string(k)
}

func (e *entry) write(p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded // TODO: decide on naming, entry or item
	}

	if e.lastSegment == nil {
		return 0, nil
	}

	n := e.lastSegment.(*segment).write(e.segmentPosition, p)
	e.segmentPosition += n

	return n, nil
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

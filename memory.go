package forget

import "bytes"

type segment struct {
	data                     []byte
	prevSegment, nextSegment node
}

type memory struct {
	segments  *list
	firstFree node
}

func newSegment(size int) *segment {
	return &segment{data: make([]byte, size)}
}

func (s *segment) prev() node     { return s.prevSegment }
func (s *segment) next() node     { return s.nextSegment }
func (s *segment) setPrev(p node) { s.prevSegment = p }
func (s *segment) setNext(n node) { s.nextSegment = n }

func (s *segment) equals(offset int, p []byte) (bool, int) {
	available := len(s.data) - offset
	if len(p) > available {
		p = p[:available]
	}

	return bytes.Equal(p, s.data[offset:offset+len(p)]), len(p)
}

func (s *segment) read(offset int, p []byte) int {
	return copy(p, s.data[offset:])
}

func (s *segment) write(offset int, p []byte) int {
	return copy(s.data[offset:], p)
}

func newMemory(segmentCount, segmentSize int) *memory {
	m := &memory{segments: new(list)}
	for i := 0; i < segmentCount; i++ {
		m.segments.insert(newSegment(segmentSize), nil)
	}

	m.firstFree = m.segments.first
	return m
}

func (m *memory) allocate() (*segment, bool) {
	s, ok := m.firstFree.(*segment)
	if !ok {
		return nil, false
	}

	m.firstFree = s.next()
	return s, ok
}

func (m *memory) move(s *segment, before node) {
	m.segments.remove(s)
	m.segments.insert(s, before)
}

func (m *memory) free(from, to *segment) {
	m.segments.removeRange(from, to)
	m.segments.insertRange(from, to, m.firstFree)
	m.firstFree = from
}

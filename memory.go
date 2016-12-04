package forget

import "bytes"

type segment struct {
	data                     []byte
	prevSegment, nextSegment node
}

type memory struct {
	segmentCount, segmentSize int
	segments                  *list
	firstFree                 node
}

func newSegment(size int) *segment {
	return &segment{data: make([]byte, size)}
}

func (s *segment) prev() node     { return s.prevSegment }
func (s *segment) next() node     { return s.nextSegment }
func (s *segment) setPrev(p node) { s.prevSegment = p }
func (s *segment) setNext(n node) { s.nextSegment = n }

// checks if each byte in a slice equals to the bytes in the segment. If p contains more bytes than the segment
// after offset, those are ignored
func (s *segment) bytesEqual(offset int, p []byte) (bool, int) {
	available := len(s.data) - offset
	if len(p) > available {
		p = p[:available]
	}

	return bytes.Equal(p, s.data[offset:offset+len(p)]), len(p)
}

// reads bytes to p from offset in the segment. Whichever of p or the segment contains less bytes than the
// other, the rest of the other is ignored
func (s *segment) read(offset int, p []byte) int {
	return copy(p, s.data[offset:])
}

// reads bytes from p ot the segment with offset. Whichever of p or the segment contains less bytes than the
// other, the rest of the other is ignored
func (s *segment) write(offset int, p []byte) int {
	return copy(s.data[offset:], p)
}

// allocates all the memory used by a cache instance
func newMemory(segmentCount, segmentSize int) *memory {
	m := &memory{
		segmentCount: segmentCount,
		segmentSize:  segmentSize,
		segments:     new(list),
	}

	for i := 0; i < segmentCount; i++ {
		m.segments.insert(newSegment(segmentSize), nil)
	}

	m.firstFree = m.segments.first
	return m
}

// returns a free segment if any and moves the free segment marker to the next one
func (m *memory) allocate() (*segment, bool) {
	if s, ok := m.firstFree.(*segment); ok {
		m.firstFree = s.next()
		return s, true
	}

	return nil, false
}

// moves a segment to new position
func (m *memory) move(s *segment, before node) {
	m.segments.remove(s)
	m.segments.insert(s, before)
}

// moves a range of segments before the free segment marker and sets the free segment marker to the beginning of
// the range
func (m *memory) free(from, to *segment) {
	m.segments.removeRange(from, to)
	m.segments.insertRange(from, to, m.firstFree)
	m.firstFree = from
}

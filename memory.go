package forget

import "bytes"

type chunk struct {
	data                 []byte
	prevChunk, nextChunk node
}

type memory struct {
	chunkCount, chunkSize int
	chunks                *list
	firstFree             node
}

func newChunk(size int) *chunk {
	return &chunk{data: make([]byte, size)}
}

func (s *chunk) prev() node     { return s.prevChunk }
func (s *chunk) next() node     { return s.nextChunk }
func (s *chunk) setPrev(p node) { s.prevChunk = p }
func (s *chunk) setNext(n node) { s.nextChunk = n }

// checks if each byte in a slice equals to the bytes in the chunk. If p contains more bytes than the chunk
// after offset, those are ignored
func (s *chunk) bytesEqual(offset int, p []byte) (bool, int) {
	available := len(s.data) - offset
	if len(p) > available {
		p = p[:available]
	}

	return bytes.Equal(p, s.data[offset:offset+len(p)]), len(p)
}

// reads bytes to p from offset in the chunk. Whichever of p or the chunk contains less bytes than the
// other, the rest of the other is ignored
func (s *chunk) read(offset int, p []byte) int {
	return copy(p, s.data[offset:])
}

// reads bytes from p ot the chunk with offset. Whichever of p or the chunk contains less bytes than the
// other, the rest of the other is ignored
func (s *chunk) write(offset int, p []byte) int {
	return copy(s.data[offset:], p)
}

// allocates all the memory used by a cache segment
func newMemory(chunkCount, chunkSize int) *memory {
	m := &memory{
		chunkCount: chunkCount,
		chunkSize:  chunkSize,
		chunks:     new(list),
	}

	for i := 0; i < chunkCount; i++ {
		m.chunks.insert(newChunk(chunkSize), nil)
	}

	m.firstFree = m.chunks.first
	return m
}

// returns a free chunk if any and moves the free chunk marker to the next one
func (m *memory) allocate() (*chunk, bool) {
	if s, ok := m.firstFree.(*chunk); ok {
		m.firstFree = s.next()
		return s, true
	}

	return nil, false
}

// moves a chunk to new position
func (m *memory) move(s *chunk, before node) {
	m.chunks.move(s, before)
}

// moves a range of chunks before the free chunk marker and sets the free chunk marker to the beginning of
// the range
func (m *memory) free(from, to *chunk) {
	m.chunks.moveRange(from, to, m.firstFree)
	m.firstFree = from
}

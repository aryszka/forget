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

func (c *chunk) prev() node     { return c.prevChunk }
func (c *chunk) next() node     { return c.nextChunk }
func (c *chunk) setPrev(p node) { c.prevChunk = p }
func (c *chunk) setNext(n node) { c.nextChunk = n }

// checks if each byte in a slice equals to the bytes in the chunk. If p contains more bytes than the chunk
// after the offset, those are ignored
func (c *chunk) bytesEqual(offset int, p []byte) (bool, int) {
	available := len(c.data) - offset
	if len(p) > available {
		p = p[:available]
	}

	return bytes.Equal(p, c.data[offset:offset+len(p)]), len(p)
}

// reads bytes to p from the offset in the chunk. Whichever of p or the chunk contains less bytes than the
// other, the rest of the other is ignored
func (c *chunk) read(offset int, p []byte) int {
	return copy(p, c.data[offset:])
}

// writes bytes from p to the chunk with the offset. Whichever of p or the chunk contains less bytes than the
// other, the rest of the other is ignored
func (c *chunk) write(offset int, p []byte) int {
	return copy(c.data[offset:], p)
}

// preallocates all the memory used by a cache segment
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

// returns a free chunk if there is any and moves the free chunk marker to the next one
func (m *memory) allocate() (*chunk, bool) {
	if c, ok := m.firstFree.(*chunk); ok {
		m.firstFree = c.next()
		return c, true
	}

	return nil, false
}

// moves a chunk to a new position
func (m *memory) move(c *chunk, before node) {
	m.chunks.move(c, before)
}

// moves a range of chunks before the free chunk marker and sets the free chunk marker to the beginning of
// the range
func (m *memory) free(from, to *chunk) {
	m.chunks.moveRange(from, to, m.firstFree)
	m.firstFree = from
}

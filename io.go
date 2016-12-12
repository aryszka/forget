package forget

import (
	"errors"
	"io"
)

type cacheIO struct {
	segment *segment
	item    *item
}

// Reader objects are used to read data from a cache item.
type Reader struct {
	*cacheIO
	currentChunk            node
	chunkIndex, chunkOffset int
}

// Writer objects are used to fill cache items.
type Writer struct {
	*cacheIO
	max, size int
}

var errWaitingForWrite = errors.New("waiting for write")

// returns the chunk index and the chunk offset based on a global offset and the current chunk size
func (c *cacheIO) offsetToChunks(offset int) (int, int) {
	chunkIndex := offset / c.segment.memory.chunkSize
	chunkOffset := offset % c.segment.memory.chunkSize
	return chunkIndex, chunkOffset
}

// creates a reader and seeks to end of the key
func newReader(s *segment, i *item) *Reader {
	r := &Reader{cacheIO: &cacheIO{segment: s, item: i}}
	chunkIndex, chunkOffset := r.offsetToChunks(i.keySize)
	r.seekChunks(i.firstChunk, 0, chunkIndex, chunkOffset)
	return r
}

// reads from the item's current underlying current chunk once at the current chunk position. If reached the end
// of the chunk, it steps first to the next chunk and resets the chunk position. If reached the end of the item
// and the write to the item is complete, returns EOF. If reached the end, but the item is still being written,
// it returns nil
func (r *Reader) readOne(p []byte) (int, error) {
	r.segment.mx.RLock()
	defer r.segment.mx.RUnlock()

	if r.item == nil {
		return 0, ErrReaderClosed
	}

	if r.item.discarded {
		return 0, ErrItemDiscarded
	}

	if r.currentChunk != r.item.lastChunk && r.chunkOffset == r.segment.memory.chunkSize {
		r.currentChunk = r.currentChunk.next()
		r.chunkIndex++
		r.chunkOffset = 0
	}

	if r.currentChunk == r.item.lastChunk && r.chunkOffset+len(p) > r.item.chunkOffset {
		p = p[:r.item.chunkOffset-r.chunkOffset]
		if len(p) == 0 {
			if r.item.writeComplete {
				return 0, io.EOF
			}

			return 0, nil
		}
	}

	n := r.currentChunk.(*chunk).read(r.chunkOffset, p)
	r.chunkOffset += n
	return n, nil
}

// Read reads from an item at the current position. If 0 bytes were read and the item is still being written, it
// blocks.
func (r *Reader) Read(p []byte) (int, error) {
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
			// here waiting only for a change, while the
			// condition checking happens during the actual read
			r.item.writeCond.L.Lock()
			r.item.writeCond.Wait()
			r.item.writeCond.L.Unlock()
		}
	}

	return count, nil
}

// returns the current offset not counting the key
func (r *Reader) currentOffset() int64 {
	return int64(r.chunkIndex*r.segment.memory.chunkSize + r.chunkOffset - r.item.keySize)
}

// steps the current chunk by chunkIndex - fromIndex and sets the chunkOffset
func (r *Reader) seekChunks(from node, fromIndex, chunkIndex, chunkOffset int) {
	for fromIndex != chunkIndex {
		if fromIndex < chunkIndex {
			fromIndex++
			from = from.next()
		} else {
			fromIndex--
			from = from.prev()
		}
	}

	r.currentChunk = from
	r.chunkIndex = chunkIndex
	r.chunkOffset = chunkOffset
}

// seeks the item's current underlying current chunk and chunk index once. If reached the end of the item and
// the write to the item is complete, returns EOF. If reached the end, but the item is still being written, it
// returns errWaitingForWrite
func (r *Reader) seekOne(offset int64, whence int) (int64, error) {
	r.segment.mx.RLock()
	defer r.segment.mx.RUnlock()

	to := int(offset)

	switch whence {
	case io.SeekCurrent:
		to += r.chunkIndex*r.segment.memory.chunkSize + r.chunkOffset
	case io.SeekEnd:
		if !r.item.writeComplete {
			return r.currentOffset(), ErrInvalidSeekOffset
		}

		to += r.item.size
	default:
		to += r.item.keySize
	}

	if to < r.item.keySize {
		return r.currentOffset(), ErrInvalidSeekOffset
	}

	lastChunkIndex, lastOffset := r.offsetToChunks(r.item.size)
	if lastOffset == 0 {
		lastChunkIndex--
	}

	if to > r.item.size {
		r.seekChunks(r.item.lastChunk, lastChunkIndex, lastChunkIndex, lastOffset)
		if r.item.writeComplete {
			return r.currentOffset(), ErrInvalidSeekOffset
		}

		return r.currentOffset(), errWaitingForWrite
	}

	chunkIndex, chunkOffset := r.offsetToChunks(to)
	var (
		from      node
		fromIndex int
	)

	switch {
	case r.chunkIndex/2 > chunkIndex:
		from, fromIndex = r.item.firstChunk, 0
	case r.item.writeComplete && (r.chunkIndex+lastChunkIndex)/2 < chunkIndex:
		from, fromIndex = r.item.lastChunk, lastChunkIndex
	default:
		from, fromIndex = r.currentChunk, r.chunkIndex
	}

	r.seekChunks(from, fromIndex, chunkIndex, chunkOffset)
	return r.currentOffset(), nil
}

// Seek moves the current position of the reader by offset counted from whence, which can be io.SeekStart,
// io.SeekCurrent or io.SeekEnd. When the write to the item is incomplete, it is an error to use io.SeekEnd.
// When the targeted position is beyond the current size of the item, and the write is incomplete, Seek blocks,
// while if the write is complete, it returns an error.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	for {
		offset, err := r.seekOne(offset, whence)
		if err == errWaitingForWrite {
			// only block if waiting for new data.
			// here waiting only for a change, while the
			// condition checking happens during the actual seek
			r.item.writeCond.L.Lock()
			r.item.writeCond.Wait()
			r.item.writeCond.L.Unlock()
			continue
		}

		return offset, err
	}
}

// Close releases the reader. When there are items marked internally for delete, and no more readers are
// attached to them, those items will become available for delete at the time of the next cache eviction of the
// same segment.
func (r *Reader) Close() error {
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

func newWriter(s *segment, i *item) *Writer {
	return &Writer{
		cacheIO: &cacheIO{segment: s, item: i},
		max:     s.memory.chunkCount*s.memory.chunkSize - i.keySize,
	}
}

// Write fills a cache item. If the writer was closed or the max item size was reached, it returns an error. If
// the last chunk of the item is full, it tries to allocate a new chunk. If allocation fails due to too many
// active readers, the write blocks until allocation becomes possible.
func (w *Writer) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	var (
		count   int
		blocked bool
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

			// here waiting only for a change, while the
			// condition checking happens during the actual write
			w.segment.readDoneCond.L.Lock()
			w.segment.readDoneCond.Wait()
			w.segment.readDoneCond.L.Unlock()
		} else if err != nil {
			return count, err
		}
	}

	return count, nil
}

// Close releases the writer signaling write done internally. Readers, that attached to the same cache item and
// blocked by trying to read beyond the available item size, get unblocked at this point.
func (w *Writer) Close() error {
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

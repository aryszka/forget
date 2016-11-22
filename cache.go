package forget

import (
	"hash"
	"hash/fnv"
	"io"
	"time"
)

const (

	// DefaultMaxSize defines the maximum total size of items and their
	// keys stored in the cache if it is not specified in the
	// initialization options.
	DefaultMaxSize = 1 << 30

	// DefaultSegmentSize defines the used segment size if it is not
	// defined in the initialization options.
	DefaultSegmentSize = 1 << 15
)

type dataMode int

const (
	dataRead dataMode = iota
	dataWrite
)

type segment struct {
	data               []byte
	prevNode, nextNode node
}

type entry struct {
	hash                      uint64
	firstSegment, lastSegment node
	size                      Size
	expiration                time.Time
	prevNode, nextNode        node
	filled                    int
	discarded                 bool
	wait                      []chan<- struct{}
}

type keyspace struct {
	entries *list
	lookup  map[uint64][]*entry
	size    Size
}

type cache struct {
	maxSegments, segmentSize int
	size                     Size
	hashing                  hash.Hash64
	data                     *list
	firstFree                node
	spaces                   map[string]*keyspace
}

func (s *segment) prev() node     { return s.prevNode }
func (s *segment) next() node     { return s.nextNode }
func (s *segment) setPrev(n node) { s.prevNode = n }
func (s *segment) setNext(n node) { s.nextNode = n }

func (e *entry) prev() node     { return e.prevNode }
func (e *entry) next() node     { return e.nextNode }
func (e *entry) setPrev(n node) { e.prevNode = n }
func (e *entry) setNext(n node) { e.nextNode = n }

func initMemory(o Options) *list {
	segments := new(list)
	for i := 0; i < o.MaxSize; i += o.SegmentSize {
		segments.append(&segment{data: make([]byte, o.SegmentSize)})
	}

	return segments
}

func newCache(o Options) *cache {
	if o.MaxSize <= 0 {
		o.MaxSize = DefaultMaxSize
	}

	if o.SegmentSize <= 0 {
		o.SegmentSize = DefaultSegmentSize
	}

	if o.SegmentSize > o.MaxSize {
		o.SegmentSize = o.MaxSize
	}

	o.MaxSize -= o.MaxSize % o.SegmentSize

	if o.Hash == nil {
		o.Hash = fnv.New64a()
	}

	data := initMemory(o)
	return &cache{
		maxSegments: o.MaxSize / o.SegmentSize,
		segmentSize: o.SegmentSize,
		hashing:     o.Hash,
		data:        data,
		firstFree:   data.first,
		spaces:      make(map[string]*keyspace),
	}
}

func (c *cache) hash(key string) uint64 {
	c.hashing.Reset()
	c.hashing.Write([]byte(key))
	return c.hashing.Sum64()
}

func (c *cache) readWrite(e *entry, data []byte, offset int, mode dataMode) {
	var (
		index, copied int
		to, from      []byte
		current       node
		s             *segment
	)

	current = e.firstSegment
	for copied < len(data) {
		s = current.(*segment)
		if index+c.segmentSize >= offset {
			var segmentIndex int
			if index <= offset {
				segmentIndex = offset - index
			}

			switch mode {
			case dataWrite:
				to = s.data[segmentIndex:]
				from = data[copied:]
			default:
				to = data[copied:]
				from = s.data[segmentIndex:]
			}

			copied += copy(to, from)
		}

		current = current.next()
		index += c.segmentSize
	}
}

func (c *cache) readData(e *entry, offset, size int) []byte {
	data := make([]byte, size)
	c.readWrite(e, data, offset, dataRead)
	return data
}

func (c *cache) writeData(e *entry, offset int, data []byte) {
	c.readWrite(e, data, offset, dataWrite)
}

func (c *cache) lookup(s *keyspace, hash uint64, key string) (*entry, bool) {
	for _, e := range s.lookup[hash] {
		currentKey := c.readData(e, 0, len(key))
		if string(currentKey) == key {
			return e, true
		}
	}

	return nil, false
}

func (c *cache) deleteEntry(space *keyspace, e *entry) Size {
	e.discarded = true
	space.entries.remove(e)

	if e.size.Segments > 0 {
		if e.lastSegment.next() != c.firstFree {
			c.data.removeRange(e.firstSegment, e.lastSegment)
			c.data.insertRange(e.firstSegment, e.lastSegment, c.firstFree)
		}

		c.firstFree = e.firstSegment
	}

	hashEntries := space.lookup[e.hash]
	for i, ei := range hashEntries {
		if ei == e {
			last := len(hashEntries) - 1
			hashEntries[i], hashEntries[last], hashEntries = hashEntries[last], nil, hashEntries[:last]
		}
	}

	sizeChange := Size{}.sub(e.size)
	space.size = space.size.sub(e.size)
	c.size = c.size.sub(e.size)

	if len(hashEntries) > 0 {
		space.lookup[e.hash] = hashEntries
		return sizeChange
	}

	delete(space.lookup, e.hash)
	if len(space.lookup) == 0 {
		for k, s := range c.spaces {
			if s == space {
				delete(c.spaces, k)
				break
			}
		}
	}

	return sizeChange
}

func (c *cache) requiredSegments(key string, size int) int {
	l := len(key) + size
	n := l / c.segmentSize

	if l%c.segmentSize == 0 {
		return n
	}

	return n + 1
}

func (c *cache) allocate(kspace string, requiredSegments int) (node, node, map[string]int, Size) {
	if requiredSegments == 0 {
		return nil, nil, nil, Size{}
	}

	var sizeChange Size
	evicted := make(map[string]int)

	if space, ok := c.spaces[kspace]; ok {
		current := space.entries.first
		for c.maxSegments-c.size.Segments < requiredSegments && current != nil {
			sc := c.deleteEntry(space, current.(*entry))
			evicted[kspace] -= sc.Effective
			sizeChange = sizeChange.add(sc)
			current = current.next()
		}
	}

	if c.maxSegments-c.size.Segments < requiredSegments {
		spaces := make([]*keyspace, 0, len(c.spaces))
		keys := make(map[*keyspace]string)
		for k, s := range c.spaces {
			spaces = append(spaces, s)
			keys[s] = k
		}

		var counter int
		for c.maxSegments-c.size.Segments < requiredSegments {
			var s *keyspace
			for s == nil || s.entries.first == nil {
				s = spaces[counter]
				if s.entries.first == nil {
					spaces = append(spaces[:counter], spaces[counter+1:]...)
					counter %= len(spaces)
				}
			}

			sc := c.deleteEntry(s, s.entries.first.(*entry))
			evicted[keys[s]] -= sc.Effective
			sizeChange = sizeChange.add(sc)
			counter = (counter + 1) % len(spaces)
		}
	}

	first, last := c.firstFree, c.firstFree
	c.firstFree = last.next()
	requiredSegments--
	for requiredSegments > 0 {
		last, c.firstFree = c.firstFree, c.firstFree.next()
		requiredSegments--
	}

	return first, last, evicted, sizeChange
}

func (c *cache) read(e *entry, offset int, p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded
	}

	if e.filled < e.size.Effective && offset >= e.filled {
		return 0, nil
	}

	if offset >= e.size.Effective {
		return 0, io.EOF
	}

	maxCount := e.size.Effective - offset
	if len(p) > maxCount {
		p = p[:maxCount]
	}

	c.readWrite(e, p, offset, dataRead)
	return len(p), nil
}

func (c *cache) write(e *entry, offset int, p []byte) (int, error) {
	if e.discarded {
		return 0, ErrItemDiscarded
	}

	if offset >= e.size.Effective {
		return 0, ErrWriteLimit
	}

	var err error
	maxCount := e.size.Effective - offset
	if len(p) > maxCount {
		err = ErrWriteLimit
		p = p[:maxCount]
	}

	c.readWrite(e, p, offset, dataWrite)
	e.filled += len(p)

	for _, w := range e.wait {
		close(w)
	}

	return len(p), err
}

func (c *cache) get(kspace, key string) (*entry, bool, Size) {
	var (
		space      *keyspace
		exists     bool
		e          *entry
		sizeChange Size
	)

	if space, exists = c.spaces[kspace]; !exists {
		return nil, false, Size{}
	}

	if e, exists = c.lookup(space, c.hash(key), key); !exists {
		return nil, false, Size{}
	}

	if e.expiration.Before(time.Now()) {
		sizeChange = c.deleteEntry(space, e)
		return nil, false, sizeChange
	}

	space.entries.remove(e)
	space.entries.append(e)
	return e, true, Size{}
}

func (c *cache) set(kspace, key string, size int, ttl time.Duration) (*entry, bool, map[string]int, Size) {
	var (
		hash             uint64
		space            *keyspace
		exists           bool
		e                *entry
		requiredSegments int
		scAlloc          Size
		evicted          map[string]int
		sizeChange       Size
	)

	hash = c.hash(key)

	if space, exists = c.spaces[kspace]; exists {
		e, exists = c.lookup(space, hash, key)
	}

	requiredSegments = c.requiredSegments(key, size)
	if requiredSegments > c.maxSegments {
		if exists {
			sizeChange = c.deleteEntry(space, e)
		}

		return nil, false, nil, sizeChange
	}

	if exists {
		sizeChange = c.deleteEntry(space, e)
	}

	space = c.spaces[kspace]
	if space == nil {
		space = &keyspace{entries: new(list), lookup: make(map[uint64][]*entry)}
		c.spaces[kspace] = space
	}

	e = &entry{hash: hash}
	space.lookup[hash] = append(space.lookup[hash], e)
	e.firstSegment, e.lastSegment, evicted, scAlloc = c.allocate(kspace, requiredSegments)
	sizeChange = sizeChange.add(scAlloc)

	e.size = Size{
		Len:       1,
		Segments:  requiredSegments,
		Effective: len(key) + size,
	}
	sizeChange = sizeChange.add(e.size)
	space.size = space.size.add(e.size)
	c.size = c.size.add(e.size)

	e.expiration = time.Now().Add(ttl)
	c.writeData(e, 0, []byte(key))
	e.filled = len(key)
	space.entries.append(e)

	return e, true, evicted, sizeChange
}

func (c *cache) del(kspace, key string) Size {
	var (
		space  *keyspace
		exists bool
		e      *entry
	)

	if space, exists = c.spaces[kspace]; !exists {
		return Size{}
	}

	if e, exists = c.lookup(space, c.hash(key), key); !exists {
		return Size{}
	}

	return c.deleteEntry(space, e)
}

func (c *cache) getKeyspaceStatus(kspace string) Size {
	if space, ok := c.spaces[kspace]; ok {
		return space.size
	}

	return Size{}
}

func (c *cache) getStatus() *Status {
	s := &Status{
		Keyspaces: make(map[string]Size),
		Size:      c.size,
	}

	for k, space := range c.spaces {
		s.Keyspaces[k] = space.size
	}

	return s
}

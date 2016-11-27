package forget

// temporary structure to pass the key and its hash around during individual calls
type id struct {
	hash uint64
	key  string
}

type cache struct {
	segmentCount, segmentSize int
	memory                    *memory
	lru                       *list
	hash                      [][]*entry
	closed                    bool
}

func newCache(segmentCount, segmentSize int) *cache {
	return &cache{
		segmentCount: segmentCount,
		segmentSize:  segmentSize,
		memory:       newMemory(segmentCount, segmentSize),
		lru:          new(list),
		hash:         make([][]*entry, segmentCount), // there cannot be more entries than segments
	}
}

func (c *cache) evictFor(e *entry) bool {
	current := c.lru.first
	for current != nil {
		if current != e {
			c.deleteEntry(current.(*entry))
			return true
		}

		current = current.next()
	}

	return false
}

func (c *cache) allocateFor(e *entry) bool {
	_, last := e.data()
	for {
		if s, ok := c.memory.allocate(); ok {
			if last != nil {
			}
			if last != nil && s != last.next() {
				c.memory.move(s, last.next())
			}

			e.appendSegment(s)
			return true
		}

		if !c.evictFor(e) {
			return false
		}
	}
}

func (c *cache) writeKey(e *entry, key string) bool {
	p := []byte(key)
	for len(p) > 0 {
		if !c.allocateFor(e) {
			return false
		}

		n, _ := e.write(p)
		p = p[n:]
	}

	return true
}

func (c *cache) lookup(id id) (*entry, bool) {
	// optimization possiblity: if there is only a single entry in the bucket,
	// no need to compare the key
	for _, ei := range c.hash[id.hash%uint64(c.segmentCount)] {
		k := ei.readKey()
		if k == id.key {
			return ei, true
		}
	}

	return nil, false
}

func (c *cache) addLookup(id id, e *entry) {
	index := id.hash % uint64(c.segmentCount)
	c.hash[index] = append(c.hash[index], e)
}

func (c *cache) deleteLookup(e *entry) {
	index := e.hash % uint64(c.segmentCount)
	bucket := c.hash[index]
	for i, ei := range bucket {
		if ei == e {
			last := len(bucket) - 1
			bucket[i], bucket = bucket[last], bucket[:last]
			c.hash[index] = bucket
			return
		}
	}
}

func (c *cache) touchEntry(e *entry) {
	c.lru.remove(e)
	c.lru.insert(e, nil)
}

func (c *cache) deleteEntry(e *entry) {
	first, last := e.data()
	if first != nil {
		c.memory.free(first, last)
	}

	e.close()
	c.lru.remove(e)
	c.deleteLookup(e)
}

func (c *cache) get(id id) (*entry, bool) {
	if c.closed {
		return nil, false
	}

	e, ok := c.lookup(id)
	if !ok {
		return nil, false
	}

	c.touchEntry(e)
	return e, ok
}

func (c *cache) set(id id) (*entry, bool) {
	if c.closed {
		return nil, false
	}

	c.del(id)
	e := newEntry(id.hash, len(id.key))

	if !c.writeKey(e, id.key) {
		return nil, false
	}

	c.lru.insert(e, nil)
	c.addLookup(id, e)

	return e, true
}

func (c *cache) del(id id) {
	if c.closed {
		return
	}

	if e, ok := c.lookup(id); ok {
		c.deleteEntry(e)
	}
}

func (c *cache) close() {
	if c.closed {
		return
	}

	e := c.lru.first
	for e != nil {
		e.(*entry).close()
		e = e.next()
	}

	c.memory = nil
	c.lru = nil
	c.hash = nil
	c.closed = true
}

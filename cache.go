package forget

type cache struct {
	segmentSize int
	memory      *memory
	lru         *list
	lookup      map[string]*entry
}

func newCache(segmentCount, segmentSize int) *cache {
	return &cache{
		segmentSize: segmentSize,
		memory:      newMemory(segmentCount, segmentSize),
		lru:         new(list),
		lookup:      make(map[string]*entry),
	}
}

func (c *cache) deleteEntry(e *entry) {
	first, last := e.data()
	if first != nil {
		c.memory.free(first, last)
	}

	e.close()
	c.lru.remove(e)
	delete(c.lookup, e.key)
}

func (c *cache) get(key string) (*entry, bool) {
	e, ok := c.lookup[key]
	if !ok {
		return nil, false
	}

	c.lru.remove(e)
	c.lru.append(e)

	return e, ok
}

func (c *cache) set(key string) (*entry, bool) {
	if c.lookup == nil {
		return nil, false
	}

	c.del(key)

	e := newEntry(key, c.segmentSize)
	c.lru.append(e)
	c.lookup[key] = e
	return e, true
}

func (c *cache) del(key string) {
	if e, ok := c.lookup[key]; ok {
		c.deleteEntry(e)
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

func (c *cache) close() {
	for _, e := range c.lookup {
		e.close()
	}

	c.memory = nil
	c.lookup = nil
}

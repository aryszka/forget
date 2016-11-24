package forget

type cache struct {
	segmentSize int
	memory      *memory
	lookup      map[string]*entry
}

func newCache(segmentCount, segmentSize int) *cache {
	return &cache{
		segmentSize: segmentSize,
		memory:      newMemory(segmentCount, segmentSize),
		lookup:      make(map[string]*entry),
	}
}

func (c *cache) get(key string) (*entry, bool) {
	d, ok := c.lookup[key]
	return d, ok
}

func (c *cache) set(key string) (*entry, bool) {
	if c.lookup == nil {
		return nil, false
	}

	e := newEntry(c.segmentSize)
	c.lookup[key] = e
	return e, true
}

func (c *cache) del(key string) {
	e, ok := c.lookup[key]
	if !ok {
		return
	}

	first, last := e.data()
	if first != nil {
		c.memory.free(first, last)
	}

	e.close()
	delete(c.lookup, key)
}

func (c *cache) evictFor(e *entry) bool {
	return false
}

func (c *cache) allocateFor(e *entry) bool {
	_, after := e.data()
	for {
		if s, ok := c.memory.allocate(); ok {
			if after != nil {
				c.memory.move(s, after.next())
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

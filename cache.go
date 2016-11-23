package forget

type cache struct {
	data map[string]*entry
}

func newCache() *cache {
	return &cache{data: make(map[string]*entry)}
}

func (c *cache) get(key string) (*entry, bool) {
	d, ok := c.data[key]
	return d, ok
}

func (c *cache) set(key string) (*entry, bool) {
	if c.data == nil {
		return nil, false
	}

	e := newEntry()
	c.data[key] = e
	return e, true
}

func (c *cache) del(key string) {
	if e, ok := c.data[key]; ok {
		e.close()
	}

	delete(c.data, key)
}

func (c *cache) close() {
	for _, e := range c.data {
		e.close()
	}

	c.data = nil
}

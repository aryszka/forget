package forget

type cache struct {
	data map[string]interface{}
}

func newCache() *cache {
	return &cache{data: make(map[string]interface{})}
}

func (c *cache) get(key string) (interface{}, bool) {
	d, ok := c.data[key]
	return d, ok
}

func (c *cache) set(key string, data interface{}) {
	if c.data == nil {
		return
	}

	c.data[key] = data
}

func (c *cache) del(key string) {
	delete(c.data, key)
}

func (c *cache) close() {
	c.data = nil
}

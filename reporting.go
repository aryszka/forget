package forget

func (c *cache) keyspaces() []string {
	names := make([]string, 0, len(c.spaces))
	for k := range c.spaces {
		names = append(names, k)
	}

	return names
}

func (c *cache) keys(keyspace string) []string {
	s, ok := c.spaces[keyspace]
	if !ok {
		return nil
	}

	names := make([]string, 0, len(s.lookup))
	for k := range s.lookup {
		names = append(names, k)
	}

	return names
}

func (c *cache) allKeys() map[string][]string {
	names := make(map[string][]string)
	for k, s := range c.spaces {
		ns := make([]string, 0, len(s.lookup))
		for nsk := range s.lookup {
			ns = append(ns, nsk)
		}

		names[k] = ns
	}

	return names
}

func (c *cache) size(keyspace string) int {
	if space, ok := c.spaces[keyspace]; ok {
		return space.size
	}

	return 0
}

func (c *cache) len(keyspace string) int {
	if space, ok := c.spaces[keyspace]; ok {
		return len(space.lookup)
	}

	return 0
}

func (c *cache) totalSize() int {
	var s int
	for _, space := range c.spaces {
		s += space.size
	}

	return s
}

func (c *cache) totalLen() int {
	l := 0
	for _, s := range c.spaces {
		l += len(s.lookup)
	}

	return l
}

func (c *Cache) KeySpaces() []string {
	rsp := c.request(message{typ: keyspacesmsg})
	return rsp.names
}

func (c *Cache) Keys(keyspace string) []string {
	rsp := c.request(message{typ: keysmsg, keyspace: keyspace})
	return rsp.names
}

func (c *Cache) AllKeys() map[string][]string {
	rsp := c.request(message{typ: allkeysmsg})
	return rsp.allKeys
}

func (c *Cache) Size(keyspace string) int {
	rsp := c.request(message{typ: sizemsg, keyspace: keyspace})
	return rsp.size
}

func (c *Cache) Len(keyspace string) int {
	rsp := c.request(message{typ: lenmsg, keyspace: keyspace})
	return rsp.len
}

// TODO: handle key spaces
func (c *Cache) TotalSize() int {
	rsp := c.request(message{typ: totalsizemsg})
	return rsp.size
}

// TODO: handle key spaces
func (c *Cache) TotalLen() int {
	rsp := c.request(message{typ: totallenmsg})
	return rsp.len
}

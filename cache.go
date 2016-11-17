package forget

import "time"

type entry struct {
	keyspace, key          string
	data                   []byte
	expiration             time.Time
	lessRecent, moreRecent *entry
}

type keyspace struct {
	lookup   map[string]*entry
	lru, mru *entry
	size     int
}

type cache struct {
	maxSize int
	spaces  map[string]*keyspace
}

func (e *entry) size() int {
	return len(e.key) + len(e.data)
}

func (c *cache) remove(keyspace, key string) (*entry, bool) {
	space, ok := c.spaces[keyspace]
	if !ok {
		return nil, false
	}

	e, ok := space.lookup[key]
	if !ok {
		return nil, false
	}

	space.size -= e.size()
	delete(space.lookup, key)

	if len(space.lookup) == 0 {
		delete(c.spaces, keyspace)
		return e, true
	}

	if space.lru == e {
		space.lru, e.moreRecent.lessRecent = e.moreRecent, nil
	} else {
		e.lessRecent.moreRecent = e.moreRecent
	}

	if space.mru == e {
		space.mru, e.lessRecent.moreRecent = e.lessRecent, nil
	} else {
		e.moreRecent.lessRecent = e.lessRecent
	}

	return e, true
}

func (c *cache) evict(currentSpace string, size int) {
	if len(c.spaces) == 0 {
		return
	}

	var totalSize, counter int
	otherSpaces := make([]*keyspace, 0, len(c.spaces)-1)
	for k, space := range c.spaces {
		totalSize += space.size
		if k != currentSpace {
			otherSpaces = append(otherSpaces, space)
		}
	}

	for totalSize+size > c.maxSize {
		if space, ok := c.spaces[currentSpace]; ok {
			c.remove(space.lru.keyspace, space.lru.key)
			totalSize -= space.lru.size()
			continue
		}

		var space *keyspace
		for space == nil || len(space.lookup) == 0 {
			if space != nil {
				otherSpaces = append(otherSpaces[:counter], otherSpaces[counter+1:]...)
				counter %= len(otherSpaces)
			}

			space = otherSpaces[counter]
		}

		c.remove(space.lru.keyspace, space.lru.key)
		totalSize -= space.lru.size()
		counter++
		counter %= len(otherSpaces)
	}
}

func (c *cache) append(e *entry) {
	s := e.size()
	c.evict(e.keyspace, s)

	space, ok := c.spaces[e.keyspace]
	if !ok {
		space = &keyspace{lookup: make(map[string]*entry)}
		c.spaces[e.keyspace] = space
	}

	space.size += e.size()
	space.lookup[e.key] = e

	if space.lru == nil {
		space.lru, space.mru, e.lessRecent, e.moreRecent = e, e, nil, nil
	} else {
		space.mru.moreRecent, space.mru, e.lessRecent, e.moreRecent = e, e, space.mru, nil
	}
}

func (c *cache) get(keyspace, key string) ([]byte, bool) {
	e, ok := c.remove(keyspace, key)
	if !ok {
		return nil, false
	}

	if e.expiration.Before(time.Now()) {
		return nil, false
	}

	c.append(e)
	return e.data, true
}

func (c *cache) set(keyspace, key string, data []byte, ttl time.Duration) {
	e, ok := c.remove(keyspace, key)
	if !ok {
		e = &entry{keyspace: keyspace, key: key}
	}

	e.data = data
	if e.size() > c.maxSize {
		return
	}

	e.expiration = time.Now().Add(ttl)
	c.append(e)
}

func (c *cache) del(keyspace, key string) {
	c.remove(keyspace, key)
}

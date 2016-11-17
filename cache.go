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

func newCache(maxSize int) *cache {
	return &cache{
		maxSize: maxSize,
		spaces:  make(map[string]*keyspace),
	}
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

	var (
		totalSize, counter int
		cspace             *keyspace
	)

	otherSpaces := make([]*keyspace, 0, len(c.spaces)-1)
	for k, space := range c.spaces {
		totalSize += space.size

		if k == currentSpace {
			cspace = space
		} else {
			otherSpaces = append(otherSpaces, space)
		}
	}

	for totalSize+size > c.maxSize {
		if cspace != nil && len(cspace.lookup) > 0 {
			totalSize -= cspace.lru.size()
			c.remove(cspace.lru.keyspace, cspace.lru.key)
			continue
		}

		var other *keyspace
		for other == nil || len(other.lookup) == 0 {
			if other != nil {
				otherSpaces = append(otherSpaces[:counter], otherSpaces[counter+1:]...)
				counter %= len(otherSpaces)
			}

			other = otherSpaces[counter]
		}

		totalSize -= other.lru.size()
		c.remove(other.lru.keyspace, other.lru.key)
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

func getStatusOf(space *keyspace) *KeyspaceStatus {
	s := &KeyspaceStatus{}
	if space == nil {
		return s
	}

	s.Len = len(space.lookup)
	for _, e := range space.lookup {
		s.Size += e.size()
	}

	return s
}

func (c *cache) getKeyspaceStatus(keyspace string) *KeyspaceStatus {
	return getStatusOf(c.spaces[keyspace])
}

func (c *cache) getStatus() *Status {
	cs := &Status{Keyspaces: make(map[string]*KeyspaceStatus)}
	for k, space := range c.spaces {
		s := getStatusOf(space)
		cs.Keyspaces[k] = s
		cs.Len += s.Len
		cs.Size += s.Size
	}

	return cs
}

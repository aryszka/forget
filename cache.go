package forget

import "time"

type segment struct {
	index int
	previous, next *segment
}

type entry struct {
	keyspace, key          string
	firstSegment *segment
	size int
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

func newCache(o Options) *cache {
	o.MaxSize -= o.MaxSize % o.SegmentSize
	mem := make([][]byte, o.MaxSize / o.SegmentSize)
	for i := 0; i < len(mem); i++ {
		mem[i] = make([]byte, o.SegmentSize)
	}

	return &cache{
		maxSize: o.MaxSize,
		segmentSize: o.SegmentSize,
		spaces:  make(map[string]*keyspace),
		mem: mem,
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

func (c *cache) evict(currentSpace string, size int) (map[string][]string, int) {
	if len(c.spaces) == 0 {
		return nil, 0
	}

	var (
		totalSize, startSize, counter int
		cspace                        *keyspace
	)

	evicted := make(map[string][]string)
	otherSpaces := make([]*keyspace, 0, len(c.spaces)-1)

	for k, space := range c.spaces {
		totalSize += space.size

		if k == currentSpace {
			cspace = space
		} else {
			otherSpaces = append(otherSpaces, space)
		}
	}

	startSize = totalSize
	for totalSize+size > c.maxSize {
		if cspace != nil && len(cspace.lookup) > 0 {
			totalSize -= cspace.lru.size()
			evicted[cspace.lru.keyspace] = append(evicted[cspace.lru.keyspace], cspace.lru.key)
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
		evicted[other.lru.keyspace] = append(evicted[other.lru.keyspace], other.lru.key)
		c.remove(other.lru.keyspace, other.lru.key)
		counter++
		counter %= len(otherSpaces)
	}

	return evicted, totalSize - startSize
}

func (c *cache) append(e *entry) (map[string][]string, int) {
	s := e.size()
	evicted, sizeChange := c.evict(e.keyspace, s)

	space, ok := c.spaces[e.keyspace]
	if !ok {
		space = &keyspace{lookup: make(map[string]*entry)}
		c.spaces[e.keyspace] = space
	}

	space.size += s
	sizeChange += s
	space.lookup[e.key] = e

	if space.lru == nil {
		space.lru, space.mru, e.lessRecent, e.moreRecent = e, e, nil, nil
	} else {
		space.mru.moreRecent, space.mru, e.lessRecent, e.moreRecent = e, e, space.mru, nil
	}

	return evicted, sizeChange
}

func (c *cache) get(keyspace, key string) ([]byte, int, bool) {
	e, ok := c.remove(keyspace, key)
	if !ok {
		return nil, 0, false
	}

	if e.expiration.Before(time.Now()) {
		return nil, -e.size(), false
	}

	c.append(e)
	d := make([]byte, len(e.data))
	copy(d, e.data)
	return d, 0, true
}

func (c *cache) set(keyspace, key string, data []byte, ttl time.Duration) (map[string][]string, int) {
	var previousSize int
	e, ok := c.remove(keyspace, key)
	if ok {
		previousSize = e.size()
	} else {
		e = &entry{keyspace: keyspace, key: key}
	}

	e.data = make([]byte, len(data))
	copy(e.data, data)
	if e.size() > c.maxSize {
		return nil, -previousSize
	}

	e.expiration = time.Now().Add(ttl)
	evicted, sizeChange := c.append(e)
	return evicted, sizeChange - previousSize
}

func (c *cache) del(keyspace, key string) int {
	if e, ok := c.remove(keyspace, key); ok {
		return -e.size()
	}

	return 0
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

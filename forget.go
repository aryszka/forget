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
	size int
}

type cache struct {
	maxSize int
	spaces             map[string]*keyspace
}

type messageType int

const (
	getmsg messageType = iota
	setmsg
	delmsg
	keyspacesmsg
	keysmsg
	allkeysmsg
	sizemsg
	lenmsg
	totalsizemsg
	totallenmsg
)

type message struct {
	typ           messageType
	response      chan message
	ok            bool
	keyspace, key string
	data          []byte
	ttl           time.Duration
	names []string
	allKeys map[string][]string
	size, len     int
}

type Cache struct {
	cache        *cache
	req          chan message
	quit, closed chan struct{}
}

func (e *entry) size() int {
	return len(e.key) + len(e.data)
}

func (c *cache) remove(keyspace, key string) *entry {
	space, ok := c.spaces[keyspace]
	if !ok {
		return nil
	}

	e, ok := space.lookup[key]
	if !ok {
		return nil
	}

	space.size -= e.size()
	delete(space.lookup, key)

	if len(space.lookup) == 0 {
		delete(c.spaces, keyspace)
		return e
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

	return e
}

func (c *cache) evict(ks string, size int) {
	var (
		otherSpaces  []*keyspace
		counter int
	)

	totalSize := c.totalSize()
	for totalSize + size > c.maxSize {
		if space, ok := c.spaces[ks]; ok {
			c.remove(space.lru.keyspace, space.lru.key)
			totalSize -= space.lru.size()
			continue
		}

		if len(otherSpaces) == 0 {
			otherSpaces = make([]*keyspace, 0, len(c.spaces)-1)
			for k, s := range c.spaces {
				if k != ks {
					otherSpaces = append(otherSpaces, s)
				}
			}
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
	e := c.remove(keyspace, key)
	if e == nil {
		return nil, false
	}

	if e.expiration.Before(time.Now()) {
		return nil, false
	}

	c.append(e)
	return e.data, true
}

func (c *cache) set(keyspace, key string, data []byte, ttl time.Duration) {
	e := c.remove(keyspace, key)
	if e == nil {
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

func New(maxSize int) *Cache {
	c := &Cache{
		cache: &cache{
			maxSize:   maxSize,
			spaces:    make(map[string]*keyspace),
		},
		req:    make(chan message),
		quit:   make(chan struct{}),
		closed: make(chan struct{}),
	}

	go c.run()
	return c
}

func (c *Cache) run() {
	for {
		select {
		case req := <-c.req:
			var rsp message
			switch req.typ {
			case getmsg:
				rsp.data, rsp.ok = c.cache.get(req.keyspace, req.key)
			case setmsg:
				c.cache.set(req.keyspace, req.key, req.data, req.ttl)
			case delmsg:
				c.cache.del(req.keyspace, req.key)
			case keyspacesmsg:
				rsp.names = c.cache.keyspaces()
			case keysmsg:
				rsp.names = c.cache.keys(req.keyspace)
			case allkeysmsg:
				rsp.allKeys = c.cache.allKeys()
			case sizemsg:
				rsp.size = c.cache.size(req.keyspace)
			case lenmsg:
				rsp.len = c.cache.len(req.keyspace)
			case totalsizemsg:
				rsp.size = c.cache.totalSize()
			case totallenmsg:
				rsp.len = c.cache.totalLen()
			}

			req.response <- rsp
		case <-c.quit:
			close(c.closed)
			return
		}
	}
}

func (c *Cache) request(req message) message {
	req.response = make(chan message)

	select {
	case c.req <- req:
	case <-c.quit:
		return message{}
	}

	return <-req.response
}

// does not copy
func (c *Cache) Get(keyspace, key string) ([]byte, bool) {
	rsp := c.request(message{typ: getmsg, keyspace: keyspace, key: key})
	return rsp.data, rsp.ok
}

// does not copy
func (c *Cache) Set(keyspace, key string, data []byte, ttl time.Duration) {
	c.request(message{typ: setmsg, keyspace: keyspace, key: key, data: data, ttl: ttl})
}

func (c *Cache) Del(keyspace, key string) {
	c.request(message{typ: delmsg, keyspace: keyspace, key: key})
}

func (c *Cache) Close() {
	close(c.quit)
	<-c.closed
}

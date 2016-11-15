package forget

import "time"

type entry struct {
	keySpace, key          string
	data                   []byte
	expiration             time.Time
	lessRecent, moreRecent *entry
}

type keySpace struct {
	lookup   map[string]*entry
	lru, mru *entry
}

type cache struct {
	maxSize, available int
	spaces             map[string]*keySpace
}

type messageType int

const (
	getmsg messageType = iota
	setmsg
	delmsg
	sizemsg
	lenmsg
)

type message struct {
	typ           messageType
	response      chan message
	ok            bool
	keySpace, key string
	data          []byte
	ttl           time.Duration
	size, len     int
}

type Cache struct {
	cache *cache
	req   chan message
	quit  chan struct{}
}

type SingleSpace struct {
	cache *Cache
}

func (e *entry) size() int {
	return len(e.key) + len(e.data)
}

func (c *cache) remove(keySpace, key string) *entry {
	space, ok := c.spaces[keySpace]
	if !ok {
		return nil
	}

	e, ok := space.lookup[key]
	if !ok {
		return nil
	}

	c.available += e.size()
	delete(space.lookup, key)
	if len(space.lookup) == 0 {
		delete(c.spaces, keySpace)
	} else {
		if space.lru == e {
			space.lru = e.moreRecent
		} else {
			e.lessRecent.moreRecent = e.moreRecent
		}

		if space.mru == e {
			space.mru = e.lessRecent
		}
	}

	return e
}

func (c *cache) append(e *entry) {
	s := e.size()
	c.available -= s

	var (
		spaces  []*keySpace
		counter int
	)

	for c.available < 0 {
		if space, ok := c.spaces[e.keySpace]; ok {
			c.remove(space.lru.keySpace, space.lru.key)
		} else {
			if len(spaces) == 0 {
				spaces = make([]*keySpace, 0, len(c.spaces)-1)
				for k, s := range c.spaces {
					if k != e.keySpace {
						spaces = append(spaces, s)
					}
				}
			}

			var space *keySpace
			for space == nil || len(space.lookup) == 0 {
				if space != nil {
					spaces = append(spaces[:counter], spaces[counter+1:]...)
					counter %= len(spaces)
				}

				space = spaces[counter]
			}

			lru := space.lru
			c.remove(lru.keySpace, lru.key)
			counter++
			counter %= len(spaces)
		}
	}

	space, ok := c.spaces[e.keySpace]
	if !ok {
		space = &keySpace{lookup: make(map[string]*entry)}
		c.spaces[e.keySpace] = space
	}

	space.lookup[e.key] = e

	e.moreRecent = nil
	if space.lru == nil {
		space.lru, space.mru, e.lessRecent = e, e, nil
	} else {
		space.mru.moreRecent, space.mru, e.lessRecent = e, e, space.mru
	}
}

func (c *cache) get(keySpace, key string) ([]byte, bool) {
	e := c.remove(keySpace, key)
	if e == nil {
		return nil, false
	}

	if e.expiration.Before(time.Now()) {
		return nil, false
	}

	c.append(e)
	return e.data, true
}

func (c *cache) set(keySpace, key string, data []byte, ttl time.Duration) {
	e := c.remove(keySpace, key)
	if e == nil {
		e = &entry{keySpace: keySpace, key: key}
	}

	e.data = data
	if e.size() > c.maxSize {
		return
	}

	e.expiration = time.Now().Add(ttl)
	c.append(e)
}

func (c *cache) del(keySpace, key string) {
	c.remove(keySpace, key)
}

func (c *cache) size() int {
	return c.maxSize - c.available
}

func (c *cache) len() int {
	l := 0
	for _, s := range c.spaces {
		l += len(s.lookup)
	}

	return l
}

func New(maxSize int) *Cache {
	c := &Cache{
		cache: &cache{
			maxSize:   maxSize,
			available: maxSize,
			spaces:    make(map[string]*keySpace),
		},
		req:  make(chan message),
		quit: make(chan struct{}),
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
				rsp.data, rsp.ok = c.cache.get(req.keySpace, req.key)
				c.response(req, rsp)
			case setmsg:
				c.cache.set(req.keySpace, req.key, req.data, req.ttl)
				c.response(req, rsp)
			case delmsg:
				c.cache.del(req.keySpace, req.key)
				c.response(req, rsp)
			case sizemsg:
				rsp.size = c.cache.size()
				c.response(req, rsp)
			case lenmsg:
				rsp.len = c.cache.len()
				c.response(req, rsp)
			default:
				panic("invalid mesasge type")
			}
		case <-c.quit:
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

	select {
	case rsp := <-req.response:
		return rsp
	case <-c.quit:
		return message{}
	}
}

func (c *Cache) response(req, rsp message) {
	select {
	case <-c.quit:
		return
	default:
		req.response <- rsp
	}
}

func (c *Cache) Get(keySpace, key string) ([]byte, bool) {
	rsp := c.request(message{typ: getmsg, keySpace: keySpace, key: key})
	return rsp.data, rsp.ok
}

func (c *Cache) Set(keySpace, key string, data []byte, ttl time.Duration) {
	c.request(message{typ: setmsg, keySpace: keySpace, key: key, data: data, ttl: ttl})
}

func (c *Cache) Del(keySpace, key string) {
	c.request(message{typ: delmsg, keySpace: keySpace, key: key})
}

// TODO: handle key spaces
func (c *Cache) Size() int {
	rsp := c.request(message{typ: sizemsg})
	return rsp.size
}

// TODO: handle key spaces
func (c *Cache) Len() int {
	rsp := c.request(message{typ: lenmsg})
	return rsp.len
}

func (c *Cache) Close() {
	select {
	case <-c.quit:
	default:
		close(c.quit)
	}
}

func NewSingleSpace(maxSize int) *SingleSpace {
	return &SingleSpace{New(maxSize)}
}

func (s *SingleSpace) Get(key string) ([]byte, bool) {
	return s.cache.Get("", key)
}

func (s *SingleSpace) Set(key string, data []byte, ttl time.Duration) {
	s.cache.Set("", key, data, ttl)
}

func (s *SingleSpace) Del(key string) {
	s.cache.Del("", key)
}

// TODO: handle key spaces
func (s *SingleSpace) Size() int {
	return s.cache.Size()
}

// TODO: handle key spaces
func (s *SingleSpace) Len() int {
	return s.cache.Len()
}

func (s *SingleSpace) Close() {
	s.cache.Close()
}

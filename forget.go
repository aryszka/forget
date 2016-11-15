package forget

import "time"

type entry struct {
	key                    string
	data                   []byte
	expiration             time.Time
	lessRecent, moreRecent *entry
}

type cache struct {
	maxSize, available int
	lookup             map[string]*entry
	lru, mru           *entry
}

type message struct {
	response  chan message
	ok        bool
	key       string
	data      []byte
	ttl       time.Duration
	size, len int
}

type Forget struct {
	cache                    *cache
	get, set, del, size, len chan message
	quit                     chan struct{}
}

func (e *entry) size() int {
	return len(e.key) + len(e.data)
}

func (c *cache) remove(key string) *entry {
	e, ok := c.lookup[key]
	if !ok {
		return nil
	}

	c.available += e.size()
	delete(c.lookup, key)

	if c.lru == e {
		c.lru = e.moreRecent
	} else {
		e.lessRecent.moreRecent = e.moreRecent
	}

	if c.mru == e {
		c.mru = e.lessRecent
	}

	return e
}

func (c *cache) append(e *entry) {
	s := e.size()
	c.available -= s
	for c.available < 0 {
		c.del(c.lru.key)
	}

	c.lookup[e.key] = e

	e.moreRecent = nil
	if c.lru == nil {
		c.lru, c.mru, e.lessRecent = e, e, nil
	} else {
		c.mru.moreRecent, c.mru, e.lessRecent = e, e, c.mru
	}
}

func (c *cache) get(key string) ([]byte, bool) {
	e := c.remove(key)
	if e == nil {
		return nil, false
	}

	if e.expiration.Before(time.Now()) {
		return nil, false
	}

	c.append(e)
	return e.data, true
}

func (c *cache) set(key string, data []byte, ttl time.Duration) {
	e := c.remove(key)
	if e == nil {
		e = &entry{key: key}
	}

	e.data = data
	if e.size() > c.maxSize {
		return
	}

	e.expiration = time.Now().Add(ttl)
	c.append(e)
}

func (c *cache) del(key string) {
	c.remove(key)
}

func (c *cache) size() int {
	return c.maxSize - c.available
}

func (c *cache) len() int {
	return len(c.lookup)
}

func New(maxSize int) *Forget {
	f := &Forget{
		cache: &cache{
			maxSize:   maxSize,
			available: maxSize,
			lookup:    make(map[string]*entry),
		},
		get:  make(chan message),
		set:  make(chan message),
		del:  make(chan message),
		size: make(chan message),
		len:  make(chan message),
		quit: make(chan struct{}),
	}

	go f.run()
	return f
}

func (f *Forget) run() {
	for {
		var rsp message
		select {
		case req := <-f.get:
			rsp.data, rsp.ok = f.cache.get(req.key)
			f.response(req, rsp)
		case req := <-f.set:
			f.cache.set(req.key, req.data, req.ttl)
			f.response(req, rsp)
		case req := <-f.del:
			f.cache.del(req.key)
			f.response(req, rsp)
		case req := <-f.size:
			rsp.size = f.cache.size()
			f.response(req, rsp)
		case req := <-f.len:
			rsp.len = f.cache.len()
			f.response(req, rsp)
		case <-f.quit:
			return
		}
	}
}

func (f *Forget) request(c chan<- message, req message) message {
	req.response = make(chan message)

	select {
	case c <- req:
	case <-f.quit:
		return message{}
	}

	select {
	case rsp := <-req.response:
		return rsp
	case <-f.quit:
		return message{}
	}
}

func (f *Forget) response(req, rsp message) {
	select {
	case <-f.quit:
		return
	default:
		req.response <- rsp
	}
}

func (f *Forget) Get(key string) ([]byte, bool) {
	rsp := f.request(f.get, message{key: key})
	return rsp.data, rsp.ok

}

func (f *Forget) Set(key string, data []byte, ttl time.Duration) {
	f.request(f.set, message{key: key, data: data, ttl: ttl})
}

func (f *Forget) Del(key string) {
	f.request(f.del, message{key: key})
}

func (f *Forget) Size() int {
	rsp := f.request(f.size, message{})
	return rsp.size
}

func (f *Forget) Len() int {
	rsp := f.request(f.len, message{})
	return rsp.len
}

func (f *Forget) Close() {
	select {
	case <-f.quit:
	default:
		close(f.quit)
	}
}

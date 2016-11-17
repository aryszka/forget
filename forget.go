package forget

import "time"

type messageType int

const (
	getMsg messageType = iota
	setMsg
	delMsg
	statusMsg
	cacheStatusMsg
)

type message struct {
	typ           messageType
	response      chan message
	ok            bool
	keyspace, key string
	data          []byte
	ttl           time.Duration
	status        *Status
	cacheStatus   *CacheStatus
}

type Cache struct {
	cache        *cache
	req          chan message
	quit, closed chan struct{}
}

func New(maxSize int) *Cache {
	c := &Cache{
		cache: &cache{
			maxSize: maxSize,
			spaces:  make(map[string]*keyspace),
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
			case getMsg:
				rsp.data, rsp.ok = c.cache.get(req.keyspace, req.key)
			case setMsg:
				c.cache.set(req.keyspace, req.key, req.data, req.ttl)
			case delMsg:
				c.cache.del(req.keyspace, req.key)
			case statusMsg, cacheStatusMsg:
				rsp = requestStatus(c.cache, req)
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
	rsp := c.request(message{typ: getMsg, keyspace: keyspace, key: key})
	return rsp.data, rsp.ok
}

// does not copy
func (c *Cache) Set(keyspace, key string, data []byte, ttl time.Duration) {
	c.request(message{typ: setMsg, keyspace: keyspace, key: key, data: data, ttl: ttl})
}

func (c *Cache) Del(keyspace, key string) {
	c.request(message{typ: delMsg, keyspace: keyspace, key: key})
}

func (c *Cache) Status(keyspace string) *Status {
	rsp := c.request(message{typ: statusMsg, keyspace: keyspace})
	return rsp.status
}

func (c *Cache) CacheStatus() *CacheStatus {
	rsp := c.request(message{typ: cacheStatusMsg})
	return rsp.cacheStatus
}

func (c *Cache) Close() {
	close(c.quit)
	<-c.closed
}

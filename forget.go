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
	typ            messageType
	response       chan message
	ok             bool
	keyspace, key  string
	data           []byte
	ttl            time.Duration
	keyspaceStatus *KeyspaceStatus
	status         *Status
}

type KeyspaceStatus struct {
	Len, Size int
}

type Status struct {
	Keyspaces map[string]*KeyspaceStatus
	Len, Size int
}

type Cache struct {
	cache        *cache
	req          chan message
	quit, closed chan struct{}
}

func New(maxSize int) *Cache {
	c := &Cache{
		cache:  newCache(maxSize),
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
			case statusMsg:
				rsp.keyspaceStatus = c.cache.getKeyspaceStatus(req.keyspace)
			case cacheStatusMsg:
				rsp.status = c.cache.getStatus()
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

func (c *Cache) StatusOf(keyspace string) *KeyspaceStatus {
	rsp := c.request(message{typ: statusMsg, keyspace: keyspace})
	return rsp.keyspaceStatus
}

func (c *Cache) Status() *Status {
	rsp := c.request(message{typ: cacheStatusMsg})
	return rsp.status
}

func (c *Cache) Close() {
	select {
	case <-c.quit:
	default:
		close(c.quit)
		<-c.closed
	}
}

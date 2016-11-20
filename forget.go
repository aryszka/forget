package forget

import (
	"hash"
	"time"
)

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
	keyspaceStatus Size
	status         *Status
}

type Size struct {
	Len, Segments, Effective int
}

type Status struct {
	Keyspaces map[string]Size
	Size
}

type NotificationLevel int

const (
	Limited NotificationLevel = iota
	Moderate
	Verbose
)

type NotificationType int

const (
	Eviction NotificationType = iota
	Miss
	Hit
	SizeChange
)

type Notification struct {
	Type          NotificationType
	Status        *Status
	Keyspace, Key string
	Evicted       map[string]int
	SizeChange    Size
}

type Cache struct {
	cache             *cache
	req               chan message
	quit, closed      chan struct{}
	notificationLevel NotificationLevel
	notify            chan<- *Notification
}

type Options struct {
	MaxSize           int
	SegmentSize       int
	Hash              hash.Hash64
	Notify            chan<- *Notification
	NotificationLevel NotificationLevel
}

func (s Size) add(a Size) Size {
	s.Len += a.Len
	s.Segments += a.Segments
	s.Effective += a.Effective
	return s
}

func (s Size) sub(a Size) Size {
	s.Len -= a.Len
	s.Segments -= a.Segments
	s.Effective -= a.Effective
	return s
}

func (s Size) zero() bool {
	return s.Len == 0 && s.Segments == 0 && s.Effective == 0
}

// notifiy channel not closed, but also no more sent
func New(o Options) *Cache {
	c := &Cache{
		cache:             newCache(o),
		req:               make(chan message),
		quit:              make(chan struct{}),
		closed:            make(chan struct{}),
		notify:            o.Notify,
		notificationLevel: o.NotificationLevel,
	}

	go c.run()
	return c
}

func (c *Cache) sendNotification(n *Notification) {
	select {
	case c.notify <- n:
	case <-c.quit:
	}
}

func (c *Cache) notifyHitMiss(keyspace, key string, hit bool, sizeChange Size) {
	if c.notify == nil || c.notificationLevel < Moderate {
		return
	}

	if hit && c.notificationLevel < Verbose {
		return
	}

	n := &Notification{
		Keyspace:   keyspace,
		Key:        key,
		SizeChange: sizeChange,
		Status:     c.cache.getStatus(),
	}

	if hit {
		n.Type = Hit
	} else {
		n.Type = Miss
	}

	c.sendNotification(n)
}

func (c *Cache) notifySet(keyspace, key string, evicted map[string]int, sizeChange Size) {
	if c.notify == nil {
		return
	}

	if c.notificationLevel < Verbose && len(evicted) == 0 {
		return
	}

	if len(evicted) == 0 && sizeChange.zero() {
		return
	}

	n := &Notification{
		Keyspace:   keyspace,
		Key:        key,
		Evicted:    evicted,
		SizeChange: sizeChange,
		Status:     c.cache.getStatus(),
	}

	if len(evicted) > 0 {
		n.Type = Eviction
	} else {
		n.Type = SizeChange
	}

	c.sendNotification(n)
}

func (c *Cache) notifyDelete(keyspace, key string, sizeChange Size) {
	if c.notify == nil || c.notificationLevel < Verbose || sizeChange.zero() {
		return
	}

	c.sendNotification(&Notification{
		Type:       SizeChange,
		Status:     c.cache.getStatus(),
		Keyspace:   keyspace,
		Key:        key,
		SizeChange: sizeChange,
	})
}

func (c *Cache) run() {
	for {
		select {
		case req := <-c.req:
			var rsp message
			switch req.typ {
			case getMsg:
				var sizeChange Size
				rsp.data, rsp.ok, sizeChange = c.cache.get(req.keyspace, req.key)
				c.notifyHitMiss(req.keyspace, req.key, rsp.ok, sizeChange)
			case setMsg:
				evicted, sizeChange := c.cache.set(req.keyspace, req.key, req.data, req.ttl)
				c.notifySet(req.keyspace, req.key, evicted, sizeChange)
			case delMsg:
				sizeChange := c.cache.del(req.keyspace, req.key)
				c.notifyDelete(req.keyspace, req.key, sizeChange)
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

func (c *Cache) Get(keyspace, key string) ([]byte, bool) {
	rsp := c.request(message{typ: getMsg, keyspace: keyspace, key: key})
	return rsp.data, rsp.ok
}

func (c *Cache) Set(keyspace, key string, data []byte, ttl time.Duration) {
	c.request(message{typ: setMsg, keyspace: keyspace, key: key, data: data, ttl: ttl})
}

func (c *Cache) Del(keyspace, key string) {
	c.request(message{typ: delMsg, keyspace: keyspace, key: key})
}

func (c *Cache) StatusOf(keyspace string) Size {
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

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

// Size objects provide size information about the cache or individual
// keyspaces.
type Size struct {

	// Len contains the number of stored items either in the cache or a
	// keyspace.
	Len int

	// Segments contains the number of segments used by active data in
	// the cache or in a keyspace.
	Segments int

	// Effective contains the net size of the stored items, including
	// the keys and the payload, in the cache or in a keyspace.
	Effective int
}

// Status objects contain information about the cache.
type Status struct {

	// Keyspaces contain the found active keyspaces in the cache and
	// their size metrics.
	Keyspaces map[string]Size

	// Size contains total metrics about the cache.
	Size
}

// NotificationLevel is used to configure the detail level of the
// provided notifications.
type NotificationLevel int

const (
	// Limited notification level instructs the cache to send
	// notifications only on the events of cache eviction.
	Limited NotificationLevel = iota

	// Moderate notification level instructs the cache to send
	// notifications only on the evetns of cache eviction and cache
	// misses.
	Moderate

	// Verbose notification level instructs the cache to send
	// notifications on every event, including: cache hits and misses,
	// any size changes and cache eviction.
	Verbose
)

// NotificationType indicates the causing event of a notification.
type NotificationType int

const (

	// Eviction indicates that some active items were purged from the
	// cache.
	Eviction NotificationType = iota

	// Miss indicates that a Get() call didn't find a key in the cache.
	Miss

	// Hit indicates that a Get() call found a key in the cache.
	Hit

	// SizeChange indicates that there was a change in the total size of
	// the stored items.
	SizeChange
)

// Notification objects are sent by the cache about internal events,
// based on the configuration options. To receive notifications, pass in
// a channel with the options to New(). It is recommended to use a
// buffered channel.
type Notification struct {

	// Type indicates the reason of the notification.
	Type NotificationType

	// Status provides information about the utilization of the cache.
	Status *Status

	// Keyspace contains the keyspace name in which the change happened
	// that caused the notification.
	Keyspace string

	// Key contains the key whose related operation caused the
	// notification.
	Key string

	// Evicted contains the number of items evicted from the cache in
	// each keyspace.
	Evicted map[string]int

	// SizeChange contains the size difference of the cache before and
	// after the event causing the notification.
	SizeChange Size
}

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache struct {
	cache             *cache
	req               chan message
	quit, closed      chan struct{}
	notificationLevel NotificationLevel
	notify            chan<- *Notification
}

// Options are used to pass in initialization options to a cache
// instance.
type Options struct {

	// MaxSize tells the maximum (preallocated) size of the cache.
	MaxSize int

	// SegmentSize tells the size of the memory segments used by the
	// cache. (See the package documentation for the right choices.)
	SegmentSize int

	// Hash may define alternative hashing algorithms to hash the cache
	// keys. The default is FNV-1a. (Hash collisions don't cause missing
	// items.)
	Hash hash.Hash64

	// Notify, if set, is used to receive notifications about the
	// cache's internal state and events. The channel is not closed by
	// the cache when the cache is closed, but also no more
	// notifications are sent.
	Notify chan<- *Notification

	// NotificationLevel sets the detail level of the notifications
	// received from the cache.
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

// New initializes a new cache instance. Use Close() to release
// resources when the cache is not required anymore in a further running
// process.
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

// Get retrieves a cached item from the cache. The second return
// argument indicates if the item was found in the cache. It is safe to
// modify or received byte slice, it won't change the cached data.
func (c *Cache) Get(keyspace, key string) ([]byte, bool) {
	rsp := c.request(message{typ: getMsg, keyspace: keyspace, key: key})
	return rsp.data, rsp.ok
}

// Set sets a new item or overwrites an existing one in the cache. It is
// safe to modify or received byte slice, it won't change the cached data.
func (c *Cache) Set(keyspace, key string, data []byte, ttl time.Duration) {
	c.request(message{typ: setMsg, keyspace: keyspace, key: key, data: data, ttl: ttl})
}

// Del removes an item from the cache.
func (c *Cache) Del(keyspace, key string) {
	c.request(message{typ: delMsg, keyspace: keyspace, key: key})
}

// StatusOf returns information about a keyspace.
func (c *Cache) StatusOf(keyspace string) Size {
	rsp := c.request(message{typ: statusMsg, keyspace: keyspace})
	return rsp.keyspaceStatus
}

// Status returns information about the cache.
func (c *Cache) Status() *Status {
	rsp := c.request(message{typ: cacheStatusMsg})
	return rsp.status
}

// Close releases the resources of the cache.
func (c *Cache) Close() {
	select {
	case <-c.quit:
	default:
		close(c.quit)
		<-c.closed
	}
}

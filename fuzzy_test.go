package forget

import (
	"math/rand"
	"testing"
	"time"
)

const (
	testDuration      = 3600 * time.Millisecond
	procCount         = 12
	cacheSize         = 1 << 15
	segmentSize       = 180
	keyspaceCount     = 30
	keyCount          = 3000
	minKeySpaceLength = 3
	maxKeySpaceLength = 15
	minKeyLength      = 6
	maxKeyLength      = 30
	minDataLength     = 48
	maxDataLength     = 900
	minTTL            = time.Nanosecond
	maxTTL            = time.Minute
	minRoundLength    = 6
	maxRoundLength    = 18
	closeTryFreq      = testDuration / 32
)

type action func(*Cache)

var actionWeights = map[string]int{
	"get":            600,
	"set":            300,
	"del":            40,
	"keyspaceStatus": 30,
	"status":         30,
}

var (
	keyspaces, keys []string
	data            [][]byte
	actions         []action
)

func randomBytes(min, max int) []byte {
	l := min + rand.Intn(max-min)
	p := make([]byte, l)
	rand.Read(p)
	return p
}

func randomByteSlices(n, min, max int) [][]byte {
	b := make([][]byte, n)
	for i := 0; i < n; i++ {
		b[i] = randomBytes(min, max)
	}

	return b
}

func randomString(min, max int) string {
	return string(randomBytes(min, max))
}

func randomStrings(n, min, max int) []string {
	s := make([]string, n)
	for i := 0; i < n; i++ {
		s[i] = randomString(min, max)
	}

	return s
}

func init() {
	keyspaces = randomStrings(keyspaceCount, minKeySpaceLength, maxKeySpaceLength)
	keys = randomStrings(keyCount, minKeyLength, maxKeyLength)
	data = randomByteSlices(keyCount, minDataLength, maxDataLength)

	for name, w := range actionWeights {
		var a action
		switch name {
		case "get":
			a = testGet
		case "set":
			a = testSet
		case "del":
			a = testDel
		case "keyspaceStatus":
			a = testKeyspaceStatus
		case "status":
			a = testStatus
		}

		for i := 0; i < w; i++ {
			actions = append(actions, a)
		}
	}
}

func randomKeySpace() string {
	return keyspaces[rand.Intn(len(keyspaces))]
}

func randomKey() string {
	return keys[rand.Intn(len(keys))]
}

func randomData() []byte {
	return data[rand.Intn(len(data))]
}

func randomTTL() time.Duration {
	return minTTL + time.Duration(rand.Intn(int(maxTTL-minTTL)))
}

func randomAction() action {
	return actions[rand.Intn(len(actions))]
}

func randomRoundLength() int {
	return minRoundLength + rand.Intn(maxRoundLength-minRoundLength)
}

func testGet(c *Cache) {
	c.GetBytes(randomKeySpace(), randomKey())
}

func testSet(c *Cache) {
	c.SetBytes(randomKeySpace(), randomKey(), randomData(), randomTTL())
}

func testDel(c *Cache) {
	c.Del(randomKeySpace(), randomKey())
}

func testKeyspaceStatus(c *Cache) {
	c.StatusOf(randomKeySpace())
}

func testStatus(c *Cache) {
	c.Status()
}

func fuzzy(t *testing.T, quit <-chan struct{}, c chan *Cache) {
	for {
		if t.Failed() {
			return
		}

		a := make([]action, randomRoundLength())
		for i := range a {
			a[i] = randomAction()
		}

		cache := <-c

		for _, ai := range a {
			ai(cache)

			select {
			case <-quit:
				c <- cache
				return
			default:
			}
		}

		c <- cache
	}
}

func checkState(t *testing.T, c *Cache) {
	// var ts, tl int
	// for ks, s := range c.cache.spaces {
	// 	var (
	// 		ss, sl int
	// 		last   *entry
	// 	)

	// 	e := s.lru
	// 	for e != nil {
	// 		if e.keyspace != ks {
	// 			t.Error("inconsitent state: entry in invalid key space")
	// 			return
	// 		}

	// 		if s.lookup[e.key] != e {
	// 			t.Error("inconsistent state: activity list does not match lookup")
	// 			return
	// 		}

	// 		ss += len(e.data) + len(e.key)
	// 		sl++
	// 		if sl > len(s.lookup) {
	// 			t.Error("inconsistent state: activity list does not match lookup")
	// 			return
	// 		}

	// 		if e.moreRecent != nil && e.moreRecent.lessRecent != e {
	// 			t.Error("inconsistent state: broken activity list")
	// 			return
	// 		}

	// 		e, last = e.moreRecent, e
	// 	}

	// 	if last != s.mru {
	// 		t.Error("inconsistent state: lru does not match mru")
	// 		return
	// 	}

	// 	if sl != len(s.lookup) {
	// 		t.Error("inconsistent state: activity list does not match lookup")
	// 		return
	// 	}

	// 	ts += ss
	// 	tl += sl
	// }

	// cs := c.cache.getStatus()
	// if ts != cs.Size {
	// 	t.Error("inconsistent state: measured size does not match reported size")
	// 	return
	// }

	// if tl != cs.Len {
	// 	t.Error("inconsistent state: measured length does not match reported length")
	// 	return
	// }
}

func closer(t *testing.T, quit, err chan struct{}, c chan *Cache, nc chan<- *Notification) {
	for {
		select {
		case <-time.After(closeTryFreq):
		case <-quit:
		}

		select {
		case cache := <-c:
			select {
			case <-cache.closed:
			default:
				cache.Close()
				checkState(t, cache)
				if t.Failed() {
					close(err)
					return
				}

				cache = New(Options{Notify: nc, NotificationLevel: Moderate, MaxSize: cacheSize})
				c <- cache
			}
		case <-quit:
			return
		}
	}
}

func receiveNotifications(quit <-chan struct{}, nc <-chan *Notification) {
	for {
		select {
		case <-nc:
		case <-quit:
			return
		}
	}
}

func TestFuzzy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	quit := make(chan struct{})
	err := make(chan struct{})
	c := make(chan *Cache, procCount)
	nc := make(chan *Notification, 32)

	cache := New(Options{Notify: nc, NotificationLevel: Moderate, MaxSize: cacheSize})
	for i := 0; i < procCount-1; i++ {
		c <- cache
		go fuzzy(t, quit, c)
	}

	go closer(t, quit, err, c, nc)
	go receiveNotifications(quit, nc)

	select {
	case <-time.After(testDuration):
		close(quit)
	case <-err:
	}

	for {
		select {
		case cache := <-c:
			select {
			case <-cache.closed:
			default:
				cache.Close()
			}

			checkState(t, cache)
		default:
			return
		}
	}
}

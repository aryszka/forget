package forget

import (
	"math/rand"
	"testing"
	"time"
)

const (
	testDuration      = 3600 * time.Millisecond
	procCount         = 12
	cacheSize         = 1 << 20
	keySpaceCount     = 30
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
	closeChance       = 300
	closeFreq         = 150 * time.Millisecond
)

type action func(*Cache)

var actionWeights = map[string]int{
	"get":      600,
	"set":      300,
	"del":      40,
	"size":     30,
	"cacheLen": 30,
}

var (
	keySpaces, keys []string
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

func randomKeySpace() string {
	return keySpaces[rand.Intn(len(keySpaces))]
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

func init() {
	keySpaces = randomStrings(keySpaceCount, minKeySpaceLength, maxKeySpaceLength)
	keys = randomStrings(keyCount, minKeyLength, maxKeyLength)
	data = randomByteSlices(keyCount, minDataLength, maxDataLength)

	for name, w := range actionWeights {
		var a action
		switch name {
		case "get":
			a = get
		case "set":
			a = set
		case "del":
			a = del
		case "size":
			a = size
		case "cacheLen":
			a = cacheLen
		}

		for i := 0; i < w; i++ {
			actions = append(actions, a)
		}
	}
}

func get(c *Cache) {
	c.Get(randomKeySpace(), randomKey())
}

func set(c *Cache) {
	c.Set(randomKeySpace(), randomKey(), randomData(), randomTTL())
}

func del(c *Cache) {
	c.Del(randomKeySpace(), randomKey())
}

func size(c *Cache) {
	c.Size()
}

func cacheLen(c *Cache) {
	c.Len()
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

func checkState(t *testing.T, c *Cache, quit chan<- struct{}) {
	err := func(reason string) {
		t.Error()
		if quit != nil {
			close(quit)
		}
	}

	var ts, tl int
	for ks, s := range c.cache.spaces {
		var (
			ss, sl int
			last   *entry
		)

		e := s.lru
		for e != nil {
			if e.keySpace != ks {
				err("inconsitent state: entry in invalid key space")
				return
			}

			if s.lookup[e.key] != e {
				err("inconsistent state: activity list does not match lookup")
				return
			}

			ss += len(e.data) + len(e.key)
			sl++
			if sl > len(s.lookup) {
				err("inconsistent state: activity list does not match lookup")
				return
			}

			if e.moreRecent != nil && e.moreRecent.lessRecent != e {
				err("inconsistent state: broken activity list")
				return
			}

			e, last = e.moreRecent, e
		}

		if last != s.mru {
			err("inconsistent state: lru does not match mru")
			return
		}

		if sl != len(s.lookup) {
			err("inconsistent state: activity list does not match lookup")
			return
		}

		ts += ss
		tl += sl
	}

	if ts != c.cache.size() {
		err("inconsistent state: measured size does not match reported size")
		return
	}

	if tl != c.cache.len() {
		err("inconsistent state: measured length does not match reported length")
		return
	}
}

func closer(t *testing.T, quit chan struct{}, c chan *Cache) {
	for {
		select {
		case <-time.After(closeFreq):
		case <-quit:
		}

		select {
		case cache := <-c:
			select {
			case <-cache.closed:
			default:
				cache.Close()
				checkState(t, cache, quit)
				if t.Failed() {
					return
				}

				cache = New(cacheSize)
				c <- cache
			}
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
	cache := New(cacheSize)
	c := make(chan *Cache, procCount)
	for i := 0; i < procCount-1; i++ {
		c <- cache
		go fuzzy(t, quit, c)
	}

	go closer(t, quit, c)

	select {
	case <-time.After(testDuration):
		func() {
			defer func() {
				recover()
			}()
			close(quit)
		}()
	case <-quit:
	}

	for {
		select {
		case cache := <-c:
			select {
			case <-cache.closed:
			default:
				cache.Close()
			}

			checkState(t, cache, nil)
			if t.Failed() {
				return
			}
		default:
			return
		}
	}
}

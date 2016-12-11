package forget

import (
	"bytes"
	"io"
	"io/ioutil"
	"runtime"
	"sync"
	"testing"
	"time"
)

type (
	testKeyspace map[string][]byte
	testInit     map[string]testKeyspace
)

type opTest struct {
	msg           string
	init          testInit
	keyspace, key string
	data          []byte
	exists        bool
	check         testInit
}

var getTests = []opTest{{
	"empty",
	nil,
	"s1", "foo",
	nil,
	false,
	nil,
}, {
	"not found",
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	"s1", "qux",
	nil,
	false,
	nil,
}, {
	"not found in keyspace",
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	"s2", "bar",
	nil,
	false,
	nil,
}, {
	"found",
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	"s1", "bar",
	[]byte{2, 3, 1},
	true,
	nil,
}}

var setTests = []opTest{{
	"empty",
	nil,
	"s1", "foo",
	[]byte{1, 2, 3},
	true,
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
		},
	},
}, {
	"new",
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	"s2", "qux",
	[]byte{3, 2, 1},
	true,
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
			"qux": {3, 2, 1},
		},
	},
}, {
	"new, same key, different keyspace",
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	"s2", "foo",
	[]byte{3, 2, 1},
	true,
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
			"foo": {3, 2, 1},
		},
	},
}, {
	"overwrite",
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	"s1", "bar",
	[]byte{3, 2, 1},
	true,
	testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {3, 2, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
}}

var delTests = []opTest{{
	msg:   "empty",
	init:  nil,
	key:   "foo",
	check: nil,
}, {
	msg: "not found",
	init: testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	keyspace: "s1", key: "qux",
	check: testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
}, {
	msg: "not found in keyspace",
	init: testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	keyspace: "s2", key: "bar",
	check: testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
}, {
	msg: "found",
	init: testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
	keyspace: "s1", key: "bar",
	check: testInit{
		"s1": testKeyspace{
			"foo": {1, 2, 3},
		},
		"s2": testKeyspace{
			"baz": {3, 1, 2},
		},
	},
}}

func newTestCache() *CacheSpaces {
	return NewCacheSpaces(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
}

func initTestCache(init testInit) *CacheSpaces {
	c := newTestCache()
	for ks, s := range init {
		for k, v := range s {
			if !c.SetBytes(ks, k, v, time.Hour) {
				panic("failed to initialize test cache")
			}
		}
	}

	return c
}

func checkCacheWithContent(c *CacheSpaces, check testInit, withContent bool) bool {
	for ks, s := range check {
		for k, dk := range s {
			if d, ok := c.GetBytes(ks, k); !ok || withContent && !bytes.Equal(d, dk) {
				return false
			}
		}
	}

	return true
}

func checkCache(c *CacheSpaces, check testInit) bool {
	return checkCacheWithContent(c, check, true)
}

func TestGet(t *testing.T) {
	for _, ti := range getTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			r, found := c.Get(ti.keyspace, ti.key)
			if found != ti.exists {
				t.Error("invalid get result", found, ti.exists)
				return
			}

			if !found {
				return
			}

			defer r.Close()

			b := bytes.NewBuffer(nil)
			if n, err := io.Copy(b, r); int(n) != len(ti.data) || err != nil || !bytes.Equal(b.Bytes(), ti.data) {
				t.Error("failed to read item", n, err, b.Bytes(), ti.data)
				return
			}
		})
	}
}

func TestGetKey(t *testing.T) {
	for _, ti := range getTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			if found := c.GetKey(ti.keyspace, ti.key); found != ti.exists {
				t.Error("invalid key result", found, ti.exists)
			}
		})
	}
}

func TestGetBytes(t *testing.T) {
	for _, ti := range getTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			d, found := c.GetBytes(ti.keyspace, ti.key)
			if found != ti.exists {
				t.Error("invalid result")
				return
			}

			if found && !bytes.Equal(d, ti.data) {
				t.Error("invalid result data", d, ti.data)
				return
			}
		})
	}
}

func TestSet(t *testing.T) {
	for _, ti := range setTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			w, ok := c.Set(ti.keyspace, ti.key, time.Hour)
			if !ok {
				t.Error("failed to set item")
				return
			}

			b := bytes.NewBuffer(ti.data)
			if n, err := io.Copy(w, b); int(n) != len(ti.data) || err != nil {
				t.Error("failed to write item data")
				return
			}

			w.Close()

			if !checkCache(c, ti.check) {
				t.Error("invalid cache state")
			}
		})
	}
}

func TestSetKey(t *testing.T) {
	for _, ti := range setTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			if !c.SetKey(ti.keyspace, ti.key, time.Hour) {
				t.Error("failed to set key")
				return
			}

			if !checkCacheWithContent(c, ti.check, false) {
				t.Error("invalid cache state")
			}
		})
	}
}

func TestSetBytesOversized(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if c.SetBytes("s1", "foo", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, time.Hour) {
		t.Error("failed to fail")
	}
}

func TestSetBytes(t *testing.T) {
	for _, ti := range setTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			if !c.SetBytes(ti.keyspace, ti.key, ti.data, time.Hour) {
				t.Error("failed to set the item")
				return
			}

			if !checkCache(c, ti.check) {
				t.Error("invalid cache state")
			}
		})
	}
}

func TestDelete(t *testing.T) {
	for _, ti := range delTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			c.Delete(ti.keyspace, ti.key)
			if !checkCache(c, ti.check) {
				t.Error("failed to delete the key")
			}
		})
	}
}

func TestClose(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)

	c.Close()

	if c.GetKey("s1", "foo") {
		t.Error("failed to close cache")
		return
	}

	// crash test:
	func() {
		defer func() {
			if err := recover(); err != nil {
				t.Error(err)
			}
		}()

		c.SetBytes("s1", "bar", []byte{2, 3, 1}, time.Hour)
		if c.GetKey("s1", "bar") {
			t.Error("failed to close cache")
			return
		}

		c.Delete("s1", "foo")
		c.Close()
	}()
}

func TestReadZero(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set bytes")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to retrieve item from cache")
		return
	}

	defer r.Close()

	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Error("invalid result on reading zero bytes")
	}
}

func TestReadFromItemWithLongKey(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 9, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	c.SetBytes("s1", "12345", []byte{1, 2, 3}, time.Hour)
	if b, ok := c.GetBytes("s1", "12345"); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to read item data")
	}
}

func TestWriteZero(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if n, err := w.Write(nil); n != 0 || err != nil {
		t.Error("invalid result on writing zero bytes")
	}
}

func TestWaitForData(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	var wg sync.WaitGroup
	read := func() {
		if r, ok := c.Get("s1", "foo"); ok {
			defer r.Close()
			b := bytes.NewBuffer(nil)
			if _, err := io.Copy(b, r); err != nil || !bytes.Equal(b.Bytes(), []byte{1, 2, 3}) {
				t.Error("failed to read data", err, b.Bytes())
			}
		} else {
			t.Error("item not found")
		}

		wg.Done()
	}

	wg.Add(2)
	go read()
	go read()

	time.Sleep(12 * time.Millisecond)
	if _, err := w.Write([]byte{1, 2, 3}); err == nil {
		if err := w.Close(); err != nil {
			t.Error("close failed", err)
		}
	} else {
		t.Error("write failed", err)
	}

	wg.Wait()
}

func TestEntryReadIsNotDeleted(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to get reader")
		return
	}

	defer r.Close()

	c.Delete("s1", "foo")

	p := make([]byte, 3)
	if n, err := r.Read(p); n != 3 || err != nil {
		t.Error("failed to prevent deletion of entry being read", n, p, err)
	}
}

func TestWriteToDeletedEntry(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if _, err := w.Write([]byte{1, 2, 3}); err != nil {
		t.Error("failed to write to item")
		return
	}

	c.Delete("s1", "foo")

	if _, err := w.Write([]byte{4, 5, 6}); err != ErrItemDiscarded {
		t.Error("failed to get discarded error", err)
		return
	}
}

func TestWriteToCompleteEntry(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if _, err := w.Write([]byte{1, 2, 3}); err != nil {
		t.Error("failed to write to item")
		return
	}

	if err := w.Close(); err != nil {
		t.Error("failed to close writer")
		return
	}

	if _, err := w.Write([]byte{4, 5, 6}); err != ErrWriterClosed {
		t.Error("failed to get discarded error", err)
		return
	}
}

func TestCloseWriterTwice(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if _, err := w.Write([]byte{1, 2, 3}); err != nil {
		t.Error("failed to write to item")
		return
	}

	if err := w.Close(); err != nil {
		t.Error("failed to close writer")
		return
	}

	if err := w.Close(); err != ErrWriterClosed {
		t.Error("failed to get discarded error", err)
		return
	}
}

func TestEvict(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	if !c.SetBytes("s1", "bar", []byte{4, 5, 6, 1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	if c.GetKey("s1", "foo") {
		t.Error("failed to evict item")
		return
	}

	if !c.GetKey("s1", "bar") {
		t.Error("failed to set item")
		return
	}
}

func TestDoNotEvictCurrent(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	w, ok := c.Set("s1", "bar", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if !c.SetBytes("s1", "baz", []byte{4, 5, 6}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	if d, ok := c.GetBytes("s1", "baz"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("failed to set item")
	}

	if n, err := w.Write([]byte{7, 8, 9, 0, 1, 2}); n != 6 || err != nil {
		t.Error("failed to write data", n, err)
		return
	}

	if err := w.Close(); err != nil {
		t.Error("failed to close writer", err)
		return
	}

	if c.GetKey("s1", "foo") || c.GetKey("s1", "baz") {
		t.Error("failed to evict item")
		return
	}

	if d, ok := c.GetBytes("s1", "bar"); !ok || !bytes.Equal(d, []byte{7, 8, 9, 0, 1, 2}) {
		t.Error("data check failed", d)
		return
	}
}

func TestFailToEvict(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set initial item")
		return
	}

	w, ok := c.Set("s1", "bar", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if n, err := w.Write([]byte{7, 8, 9, 0, 1, 2, 3, 4, 6}); n != 3 || err != ErrWriteLimit {
		t.Error("failed to report write failure", n, err)
		return
	}
}

func TestTryReadBeyondAvailable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to get item")
		return
	}

	defer r.Close()

	p := make([]byte, 5)
	if n, err := r.Read(p); n != 3 || err != nil {
		t.Error("failed to read available")
	}

	done := make(chan struct{})
	go func() {
		if n, err := r.Read(p); n != 0 || err != io.EOF {
			t.Error("failed to finish read", n, err)
		}

		close(done)
	}()

	time.Sleep(12 * time.Millisecond)
	if err := w.Close(); err != nil {
		t.Error("failed to close writer")
	}

	<-done
}

func TestWriteAfterCacheClosed(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	c.Close()
	if _, err := w.Write([]byte{1, 2, 3, 4, 5, 6}); err != ErrItemDiscarded {
		t.Error("expected ErrItemDiscarded but got", err)
	}
}

func TestKeyTooLarge(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()
	if _, ok := c.Set("s1", "123456789012345", time.Hour); ok {
		t.Error("too large key was set")
	}

	if c.GetKey("s1", "123456789012345") {
		t.Error("too large key was set")
	}
}

func TestWriteAtChunkBoundary(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to create item")
		return
	}

	defer w.Close()

	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write to item", n, err)
		return
	}

	if n, err := w.Write([]byte{4, 5, 6}); n != 3 || err != nil {
		t.Error("failed to write to item", n, err)
		return
	}

	if err := w.Close(); err != nil {
		t.Error(err)
		return
	}

	if b, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(b, []byte{1, 2, 3, 4, 5, 6}) {
		t.Error("failed to read item", ok, b)
	}
}

func TestWriteToItemWithEmptyKey(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	w, ok := c.Set("s1", "", time.Hour)
	if !ok {
		t.Error("failed to create item")
		return
	}

	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write to item", n, err)
		return
	}

	if err := w.Close(); err != nil {
		t.Error(err)
		return
	}

	if b, ok := c.GetBytes("s1", ""); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to read item", ok, b)
	}
}

func TestAllocateAndInsert(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 24, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	w, ok := c.Set("s1", "foo", time.Hour)
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if !c.SetKey("s1", "bar", time.Hour) {
		t.Error("failed to set key")
		return
	}

	if _, err := w.Write([]byte{1, 2, 3, 4, 5, 6}); err != nil {
		t.Error(err)
	}
}

func TestGetEmptyItem(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetKey("s1", "", time.Hour) {
		t.Error("failed to set item with empty key")
	}

	if !c.GetKey("s1", "") {
		t.Error("failed to get item with empty key")
	}
}

func TestItemsWithDifferentKeys(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 24, ChunkSize: 6, maxSegmentCount: 1})
	c.SetKey("s1", "1", time.Hour)
	c.SetKey("s1", "123", time.Hour)
	c.SetKey("s1", "123456789", time.Hour)
	if !c.GetKey("s1", "1") || !c.GetKey("s1", "123") || !c.GetKey("s1", "123456789") {
		t.Error("failed to get/set keys of different size")
	}
}

func TestExpiration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := NewCacheSpaces(Options{CacheSize: 24, ChunkSize: 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetKey("s1", "foo", 3*time.Millisecond) {
		t.Error("failed to set item")
		return
	}

	time.Sleep(12 * time.Millisecond)
	if c.GetKey("s1", "foo") {
		t.Error("failed to expire item")
	}
}

func TestDoNotEvictWhenReading(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set initial item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to retrieve initial item")
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)
		wg.Done()
	}()
	go func() {
		c.SetBytes("s1", "baz", []byte{7, 8, 9}, time.Hour)
		wg.Done()
	}()

	time.Sleep(12 * time.Millisecond)

	if err := r.Close(); err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func TestTooLargeKey(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if c.SetKey("s1", "123456789", time.Hour) {
		t.Error("unexpectedly set too large key")
	}
}

func TestReadFromClosedCache(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 24, ChunkSize: 6, maxSegmentCount: 1})
	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to retrieve item")
		return
	}

	c.Close()

	p := make([]byte, 3)
	if _, err := r.Read(p); err != ErrItemDiscarded {
		t.Error("failed to fail", err)
	}
}

func TestBlockWriterUntilSpaceAvailable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := NewCacheSpaces(Options{CacheSize: 9, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to retrieve item")
		return
	}

	w, ok := c.Set("s1", "bar", time.Hour)
	done := make(chan struct{})
	go func() {
		if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
			t.Error("failed to write after unblocked")
		}

		w.Close()
		close(done)
	}()

	time.Sleep(12 * time.Millisecond)

	r.Close()
	<-done
}

func TestSingleSpaceGet(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set initial key")
		return
	}

	r, ok := c.Get("foo")
	if !ok {
		t.Error("failed to retrieve reader")
		return
	}

	defer r.Close()

	b := bytes.NewBuffer(nil)
	if n, err := io.Copy(b, r); n != 3 || err != nil || !bytes.Equal(b.Bytes(), []byte{1, 2, 3}) {
		t.Error("failed to read from item")
	}
}

func TestSingleSpaceGetKey(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set initial key")
		return
	}

	if !c.GetKey("foo") {
		t.Error("failed to retrieve reader")
	}
}

func TestSingleSpaceGetBytes(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set initial key")
		return
	}

	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("failed to retrieve reader")
	}
}

func TestSingleSpaceSet(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	w, ok := c.Set("foo", time.Hour)
	if !ok {
		t.Error("falied to set item")
	}

	b := bytes.NewBuffer([]byte{1, 2, 3})
	if n, err := io.Copy(w, b); n != 3 || err != nil {
		t.Error("failed to write item data")
		return
	}

	w.Close()

	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("failed to verify item data")
	}
}

func TestSingleSpaceSetKey(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetKey("foo", time.Hour) {
		t.Error("falied to set item")
	}

	if !c.GetKey("foo") {
		t.Error("failed to verify key")
	}
}

func TestSingleSpaceSetBytes(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("falied to set item")
	}

	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("failed to verify item data")
	}
}

func TestSingleSpaceDelete(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set initial key")
		return
	}

	c.Delete("foo")

	if c.GetKey("foo") {
		t.Error("failed to delete item")
	}
}

func TestSingleSpaceStats(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("bar", []byte{4, 5, 6}, time.Hour)

	s := c.Stats()
	if s.AvailableMemory != (1<<9)-2*(1<<6) {
		t.Error("invalid stats")
	}
}

func TestSingleSpaceClose(t *testing.T) {
	c := New(Options{CacheSize: 1 << 9, ChunkSize: 1 << 6, maxSegmentCount: 1})
	defer c.Close()

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)

	c.Close()

	if c.GetKey("foo") {
		t.Error("failed to close cache")
		return
	}

	// crash test:
	func() {
		defer func() {
			if err := recover(); err != nil {
				t.Error(err)
			}
		}()

		c.SetBytes("bar", []byte{2, 3, 1}, time.Hour)
		if c.GetKey("bar") {
			t.Error("failed to close cache")
			return
		}

		c.Delete("foo")
		c.Close()
	}()
}

func TestEvictFirstFromOwnKeyspace(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetKey("s1", "foo", time.Hour) {
		t.Error("failed to set item")
		return
	}

	if !c.SetKey("s2", "bar", time.Hour) {
		t.Error("failed to set item")
		return
	}

	if !c.GetKey("s1", "foo") {
		t.Error("failed to touch item")
		return
	}

	if !c.SetKey("s1", "baz", time.Hour) {
		t.Error("failed to set new item")
		return
	}

	if c.GetKey("s1", "foo") || !c.GetKey("s1", "baz") || !c.GetKey("s2", "bar") {
		t.Error("invalid eviction order")
	}
}

func TestEvictFromOtherKeyspace(t *testing.T) {
	t.Run("first keyspace available", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		for ks, k := range map[string]string{
			"s1": "foo",
			"s2": "bar",
			"s3": "baz",
		} {
			if !c.SetKey(ks, k, time.Hour) {
				t.Error("failed to set item")
				return
			}
		}
	})

	t.Run("move to next keyspace when needed", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetKey("s1", "foo", time.Hour)
		c.SetKey("s2", "bar", time.Hour)

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.SetKey("s3", "baz", time.Hour)

		if !c.GetKey("s1", "foo") || c.GetKey("s2", "bar") || !c.GetKey("s3", "baz") {
			t.Error("failed to evict the right item")
		}
	})
}

func TestEvictFromOtherKeyspaceRoundRobin(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 18, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	for ks, k := range map[string][]string{
		"s1": {"foo", "qux"},
		"s2": {"bar", "fo2"},
		"s3": {"baz", "ba2"},
		"s4": {"fo3"},
		"s5": {"ba3"},
		"s6": {"qu3"},
	} {
		for _, ki := range k {
			if !c.SetKey(ks, ki, time.Hour) {
				t.Error("failed to set item")
				return
			}
		}
	}

	c.Delete("s1", "qux")
	c.Delete("s2", "fo2")
	c.Delete("s3", "ba2")
	c.Delete("s4", "fo3")

	if !c.SetKey("s5", "foobarbazquxquux", time.Hour) {
		t.Error("failed to set item")
		return
	}
}

func TestUseMaxProcsDefault(t *testing.T) {
	c := NewCacheSpaces(Options{CacheSize: 1 << 12, ChunkSize: 1 << 6})
	defer c.Close()

	items := map[string][]string{
		"s1": {"foo", "bar", "baz", "qux", "quux"},
		"s2": {"foo", "bar", "baz", "qux", "quux"},
		"s3": {"foo", "bar", "baz", "qux", "quux"},
	}

	for ks, k := range items {
		for _, ki := range k {
			if !c.SetKey(ks, ki, time.Hour) {
				t.Error("failed to set item")
				return
			}
		}
	}

	for ks, k := range items {
		for _, ki := range k {
			if !c.GetKey(ks, ki) {
				t.Error("failed to get item", ks, ki)
				return
			}
		}
	}
}

func TestCloseReader(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	if !c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour) {
		t.Error("failed to set item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to retrieve reader")
		return
	}

	if err := r.Close(); err != nil {
		t.Error("failed to close reader", err)
		return
	}

	if _, err := r.Read(make([]byte, 3)); err != ErrReaderClosed {
		t.Error("failed to return the right error", err)
		return
	}

	if err := r.Close(); err != ErrReaderClosed {
		t.Error("failed to return the right error", err)
		return
	}
}

func TestNoMoreSegmentsThanMaxProcs(t *testing.T) {
	if runtime.NumCPU() < 2 {
		t.Skip()
	}

	gmp := runtime.GOMAXPROCS(-1)
	defer runtime.GOMAXPROCS(gmp)

	runtime.GOMAXPROCS(runtime.NumCPU() / 2)

	c := NewCacheSpaces(Options{})
	defer c.Close()

	if len(c.segments) != runtime.GOMAXPROCS(-1) {
		t.Error("failed to set the segment count to GOMAXPROCS")
	}
}

func TestStats(t *testing.T) {
	c := initTestCache(testInit{
		"s1": testKeyspace{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		"s2": testKeyspace{
			"baz": []byte{7, 8, 9},
			"qux": []byte{0, 1, 2},
		},
	})
	defer c.Close()

	s := c.Stats()
	if s.AvailableMemory != (1<<9)-4*(1<<6) {
		t.Error("invalid stats")
	}
}

func TestNotifications(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	receive := func(t *testing.T, n <-chan *Event, expect EventType) {
		if e := <-n; e.Type != expect {
			t.Errorf("failed to receive the expected event. Got: %s, expected: %s.\n", e.Type, expect)
		}
	}

	run := func(msg string, expect EventType, f ...func(c *CacheSpaces)) {
		t.Run(msg, func(t *testing.T) {
			n := make(chan *Event, 4)

			c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, Notify: n, NotifyMask: All})
			defer c.Close()

			for _, fi := range f[:len(f)-1] {
				fi(c)
				<-n
			}

			f[len(f)-1](c)
			receive(t, n, expect)
		})
	}

	noop := func(*CacheSpaces) {}

	run("miss", Miss, func(c *CacheSpaces) {
		c.GetKey("s1", "foo")
	})

	run("hit", Hit, func(c *CacheSpaces) {
		c.SetKey("s1", "foo", time.Hour)
	}, noop, func(c *CacheSpaces) {
		c.GetKey("s1", "foo")
	})

	run("set", Set, func(c *CacheSpaces) {
		c.SetKey("s1", "foo", time.Hour)
	})

	run("delete", Delete, func(c *CacheSpaces) {
		c.SetKey("s1", "foo", time.Hour)
	}, noop, func(c *CacheSpaces) {
		c.Delete("s1", "foo")
	})

	run("expire", Expire|Delete|Miss, func(c *CacheSpaces) {
		c.SetKey("s1", "foo", 3*time.Millisecond)
	}, noop, func(c *CacheSpaces) {
		time.Sleep(12 * time.Millisecond)
		c.GetKey("s1", "foo")
	})

	run("evict", Evict|Delete, func(c *CacheSpaces) {
		c.SetKey("s1", "foo", time.Hour)
	}, noop, func(c *CacheSpaces) {
		c.SetKey("s1", "barbaz", time.Hour)
	})
}

func TestEventTypeString(t *testing.T) {
	all, allPlus := All, (All<<1)+1
	if all.String() != allPlus.String() {
		t.Error("failed to stringify event type", all, allPlus)
	}
}

func TestSetWhileAllBusy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetKey("s1", "foo", time.Hour) {
		t.Error("failed to set key")
		return
	}

	if !c.SetKey("s1", "bar", time.Hour) {
		t.Error("failed to set key")
		return
	}

	r1, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to get key")
		return
	}

	defer r1.Close()

	r2, ok := c.Get("s1", "bar")
	if !ok {
		t.Error("failed to get key")
		return
	}

	defer r2.Close()

	setComplete := make(chan struct{})

	go func() {
		c.Set("s1", "baz", time.Hour)
		close(setComplete)
	}()

	select {
	case <-setComplete:
		t.Error("set unexpectedly completed")
	case <-time.After(12 * time.Millisecond):
	}

	r2.Close()

	select {
	case <-setComplete:
	case <-time.After(12 * time.Millisecond):
		t.Error("failed to complete set")
	}
}

func TestNotifyMaskDefaultsToNormal(t *testing.T) {
	n := make(chan *Event, 1)
	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, Notify: n, maxSegmentCount: 1})
	defer c.Close()
	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("s1", "bar", []byte{1, 2, 3}, time.Hour)
	e := <-n
	if !e.Type.Is(Evict) {
		t.Error("failed to set mask")
	}
}

func TestReleaseMemoryOnFailedKeyWrite(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
	defer c.Close()

	if !c.SetKey("s1", "foo", time.Hour) {
		t.Error("failed to set inital item")
		return
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to get initial item")
		return
	}

	done := make(chan struct{})
	go func() {
		if !c.SetKey("s1", "123456", time.Hour) {
			t.Error("failed to set item")
		}

		close(done)
	}()

	select {
	case <-done:
		t.Error("too large key write succeeded")
	case <-time.After(12 * time.Millisecond):
	}

	r.Close()

	select {
	case <-done:
	case <-time.After(12 * time.Millisecond):
		t.Error("timeout")
	}
}

func TestDeleteCases(t *testing.T) {
	t.Run("no read, no write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.Delete("s1", "foo")
		if c.GetKey("s1", "foo") {
			t.Error("failed to delete")
		}
	})

	t.Run("no read, write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		c.Delete("s1", "foo")
		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error(err)
		}
	})

	t.Run("read, no write -> marked for delete", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.Delete("s1", "foo")
		if c.GetKey("s1", "foo") {
			t.Error("failed to mark item for delete")
			return
		}

		if b, err := ioutil.ReadAll(r); err != nil || !bytes.Equal(b, []byte{1, 2, 3}) {
			t.Error("failed to read data", err, b)
			return
		}
	})

	t.Run("read, write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.Delete("s1", "foo")
		if c.GetKey("s1", "foo") {
			t.Error("failed to delete item")
			return
		}

		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error("failed to delete item")
		}

		if _, err := r.Read(make([]byte, 3)); err != ErrItemDiscarded {
			t.Error("failed to delete item")
		}
	})
}

func TestExpireCases(t *testing.T) {
	t.Run("no read, no write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, -time.Millisecond)
		if c.GetKey("s1", "foo") {
			t.Error("failed to expire item")
		}
	})

	t.Run("no read, write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", -time.Millisecond)
		defer w.Close()

		if c.GetKey("s1", "foo") {
			t.Error("failed to expire item")
		}

		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error("failed to expire item")
		}
	})

	t.Run("read, no write -> marked for delete", func(t *testing.T) {
		if testing.Short() {
			t.Skip()
		}

		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, 12*time.Millisecond)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		time.Sleep(36 * time.Millisecond)

		if c.GetKey("s1", "foo") {
			t.Error("failed to mark deleted")
		}

		if b, err := ioutil.ReadAll(r); err != nil || !bytes.Equal(b, []byte{1, 2, 3}) {
			t.Error("failed to read data", err, b)
			return
		}
	})

	t.Run("read, write -> deleted", func(t *testing.T) {
		if testing.Short() {
			t.Skip()
		}

		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", 12*time.Millisecond)
		defer w.Close()

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		time.Sleep(36 * time.Millisecond)

		if c.GetKey("s1", "foo") {
			t.Error("failed to mark deleted")
		}

		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error("failed to delete item")
		}

		if _, err := r.Read(make([]byte, 3)); err != ErrItemDiscarded {
			t.Error("failed to delete item")
		}
	})
}

func TestResetCases(t *testing.T) {
	t.Run("no read, no write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.SetBytes("s1", "foo", []byte{4, 5, 6}, time.Hour)
		if b, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(b, []byte{4, 5, 6}) {
			t.Error("failed to reset item")
		}
	})

	t.Run("no read, write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		c.SetBytes("s1", "foo", []byte{4, 5, 6}, time.Hour)
		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error("failed to reset item")
		}

		if b, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(b, []byte{4, 5, 6}) {
			t.Error("failed to reset item")
		}
	})

	t.Run("read, no write -> marked for delete", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.SetBytes("s1", "foo", []byte{4, 5, 6}, time.Hour)
		if b, err := ioutil.ReadAll(r); err != nil || !bytes.Equal(b, []byte{1, 2, 3}) {
			t.Error("failed to mark for delete")
		}

		if b, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(b, []byte{4, 5, 6}) {
			t.Error("failed to reset item")
		}
	})

	t.Run("read, write -> deleted", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.SetBytes("s1", "foo", []byte{4, 5, 6}, time.Hour)

		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error("failed to delete item")
		}

		if _, err := r.Read(make([]byte, 3)); err != ErrItemDiscarded {
			t.Error("failed to delete item")
		}

		if b, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(b, []byte{4, 5, 6}) {
			t.Error("faled to reset item")
		}
	})
}

func TestEvictCases(t *testing.T) {
	t.Run("no read, no write, no content -> not deleted", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "", nil, time.Hour)
		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.GetKey("s1", "foo") // ensure "" is next in LRU
		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)

		if !c.GetKey("s1", "") {
			t.Error("empty item evicted")
		}

		if c.GetKey("s1", "foo") {
			t.Error("failed to evict item")
		}

		if b, ok := c.GetBytes("s1", "bar"); !ok || !bytes.Equal(b, []byte{4, 5, 6}) {
			t.Error("failed to set item")
		}
	})

	t.Run("no read, no write -> deleted", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)
		c.GetKey("s1", "bar") // ensure "foo" is next in LRU
		c.SetBytes("s1", "baz", []byte{7, 8, 9}, time.Hour)

		if c.GetKey("s1", "foo") {
			t.Error("failed to evict item")
		}

		if !c.GetKey("s1", "bar") {
			t.Error("failed to keep item")
		}

		if b, ok := c.GetBytes("s1", "baz"); !ok || !bytes.Equal(b, []byte{7, 8, 9}) {
			t.Error("failed to set item")
		}
	})

	t.Run("no read, write -> deleted", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)
		c.GetKey("s1", "bar") // ensure "foo" is next in LRU
		c.SetBytes("s1", "baz", []byte{7, 8, 9}, time.Hour)

		if c.GetKey("s1", "foo") {
			t.Error("failed to evict item")
		}

		if !c.GetKey("s1", "bar") {
			t.Error("failed to keep item")
		}

		if b, ok := c.GetBytes("s1", "baz"); !ok || !bytes.Equal(b, []byte{7, 8, 9}) {
			t.Error("failed to set item")
		}

		if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
			t.Error("failed to evict item")
		}
	})

	t.Run("read, no write -> not deleted", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)
		c.GetKey("s1", "bar") // ensure "foo" is next in LRU
		c.SetBytes("s1", "baz", []byte{7, 8, 9}, time.Hour)

		if b, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
			t.Error("failed to keep item")
		}

		if c.GetKey("s1", "bar") {
			t.Error("failed to evict item")
		}

		if b, ok := c.GetBytes("s1", "baz"); !ok || !bytes.Equal(b, []byte{7, 8, 9}) {
			t.Error("failed to set item")
		}
	})

	t.Run("read, write -> deleted", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)
		c.GetKey("s1", "bar") // ensure "foo" is next in LRU
		c.SetBytes("s1", "baz", []byte{7, 8, 9}, time.Hour)

		if c.GetKey("s1", "foo") {
			t.Error("failed to evict item")
		}

		if !c.GetKey("s1", "bar") {
			t.Error("failed to keep item")
		}

		if b, ok := c.GetBytes("s1", "baz"); !ok || !bytes.Equal(b, []byte{7, 8, 9}) {
			t.Error("failed to set item")
		}
	})
}

func TestEvictFromDeleted(t *testing.T) {
	t.Run("when read done", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 6, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")

		c.Delete("s1", "foo")
		if c.GetKey("s1", "foo") {
			t.Error("failed to delete item")
		}

		r.Close()

		c.SetBytes("s1", "bar", []byte{4, 5, 6}, time.Hour)
		if !c.GetKey("s1", "bar") {
			t.Error("failed to create item")
		}
	})

	t.Run("skip that is not done", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 12, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.SetBytes("s1", "bar", []byte{1, 2, 3}, time.Hour)

		rfoo, _ := c.Get("s1", "foo")
		defer rfoo.Close()

		rbar, _ := c.Get("s1", "bar")

		c.Delete("s1", "foo")
		c.Delete("s1", "bar")

		if c.GetKey("s1", "foo") {
			t.Error("failed to delete item")
		}

		if c.GetKey("s1", "bar") {
			t.Error("failed to delete item")
		}

		rbar.Close()

		c.SetBytes("s1", "baz", []byte{4, 5, 6}, time.Hour)
		if !c.GetKey("s1", "baz") {
			t.Error("failed to create item")
		}
	})
}

func TestSeek(t *testing.T) {
	t.Run("seek and read", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3, 4, 5, 6}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		if offset, err := r.Seek(2, io.SeekStart); err != nil || offset != 2 {
			t.Error("seek failed", offset, err)
			return
		}

		p := make([]byte, 3)
		if n, err := r.Read(p); err != nil || n != 3 || !bytes.Equal(p, []byte{3, 4, 5}) {
			t.Error("read failed", n, err, p)
		}
	})

	t.Run("seek and read to end", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		if offset, err := r.Seek(2, io.SeekStart); err != nil || offset != 2 {
			t.Error("seek failed", offset, err)
			return
		}

		p := make([]byte, 3)
		if n, err := r.Read(p); err != io.EOF || n != 1 || p[0] != 3 {
			t.Error("read failed", n, err)
		}
	})

	t.Run("seek from start", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 15, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		r.Read(make([]byte, 10))

		if offset, err := r.Seek(1, io.SeekStart); err != nil || offset != 1 {
			t.Error("seek failed", offset, err)
			return
		}

		p := make([]byte, 3)
		if n, err := r.Read(p); err != nil || n != 3 || !bytes.Equal(p, []byte{2, 3, 4}) {
			t.Error("read failed", n, err, p)
		}
	})

	t.Run("seek from current", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3, 4, 5, 6}, time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		r.Read(make([]byte, 2))

		if offset, err := r.Seek(2, io.SeekCurrent); err != nil || offset != 4 {
			t.Error("seek failed", offset, err)
			return
		}

		p := make([]byte, 3)
		if n, err := r.Read(p); err != io.EOF || n != 2 || !bytes.Equal(p[:n], []byte{5, 6}) {
			t.Error("read failed", n, err, p)
		}
	})

	t.Run("seek from end", func(t *testing.T) {
		c := NewCacheSpaces(Options{CacheSize: 18, ChunkSize: 3, maxSegmentCount: 1})
		defer c.Close()

		c.SetBytes("s1", "foo", []byte("123456789012435"), time.Hour)
		r, _ := c.Get("s1", "foo")
		defer r.Close()

		if offset, err := r.Seek(4, io.SeekEnd); err != nil || offset != 11 {
			t.Error("seek failed", offset, err)
			return
		}

		p := make([]byte, 3)
		if n, err := r.Read(p); err != nil || n != 3 || string(p) != "243" {
			t.Error("read failed", n, err, string(p))
		}
	})

	t.Run("seek from end when write incomplete", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		w.Write([]byte{1, 2, 3})

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		if _, err := r.Seek(2, io.SeekEnd); err != ErrInvalidSeekOffset {
			t.Error("seek failed to fail", err)
			return
		}
	})

	t.Run("seek before 0", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")
		r.Read(make([]byte, 1))
		defer r.Close()

		if _, err := r.Seek(-2, io.SeekCurrent); err != ErrInvalidSeekOffset {
			t.Error("seek failed to fail", err)
			return
		}
	})

	t.Run("seek beyond end", func(t *testing.T) {
		c := newTestCache()
		defer c.Close()

		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
		r, _ := c.Get("s1", "foo")
		r.Read(make([]byte, 2))
		defer r.Close()

		if _, err := r.Seek(2, io.SeekCurrent); err != ErrInvalidSeekOffset {
			t.Error("seek failed to fail", err)
			return
		}
	})

	t.Run("seek beyond end when write incomplete", func(t *testing.T) {
		if testing.Short() {
			t.Skip()
		}

		c := newTestCache()
		defer c.Close()

		w, _ := c.Set("s1", "foo", time.Hour)
		defer w.Close()

		w.Write([]byte{1, 2, 3})

		r, _ := c.Get("s1", "foo")
		defer r.Close()

		done := make(chan struct{})
		go func() {
			if _, err := r.Seek(5, io.SeekStart); err != nil {
				t.Error("failed to seek", err)
			}

			close(done)
		}()

		select {
		case <-done:
			t.Error("failed to block seek")
		case <-time.After(3 * time.Millisecond):
		}

		w.Write([]byte{4, 5, 6})

		select {
		case <-done:
		case <-time.After(3 * time.Millisecond):
			t.Error("seek timeout")
		}

		p := make([]byte, 3)
		if n, err := r.Read(p); err != nil || n != 1 || p[0] != 6 {
			t.Error("failed to read", err, n, p[:n])
		}
	})
}

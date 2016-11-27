package forget

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

type opTest struct {
	msg    string
	init   map[string][]byte
	key    string
	data   []byte
	exists bool
	check  map[string][]byte
}

var getTests = []opTest{{
	"empty",
	nil,
	"foo",
	nil,
	false,
	nil,
}, {
	"not found",
	map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
	"qux",
	nil,
	false,
	nil,
}, {
	"found",
	map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
	"bar",
	[]byte{2, 3, 1},
	true,
	nil,
}}

var setTests = []opTest{{
	"empty",
	nil,
	"foo",
	[]byte{1, 2, 3},
	true,
	map[string][]byte{
		"foo": {1, 2, 3},
	},
}, {
	"new",
	map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
	"qux",
	[]byte{3, 2, 1},
	true,
	map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
		"qux": {3, 2, 1},
	},
}, {
	"overwrite",
	map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
	"bar",
	[]byte{3, 2, 1},
	true,
	map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {3, 2, 1},
		"baz": {3, 1, 2},
	},
}}

var delTests = []opTest{{
	msg:   "empty",
	init:  nil,
	key:   "foo",
	check: nil,
}, {
	msg: "not found",
	init: map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
	key: "qux",
	check: map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
}, {
	msg: "found",
	init: map[string][]byte{
		"foo": {1, 2, 3},
		"bar": {2, 3, 1},
		"baz": {3, 1, 2},
	},
	key: "bar",
	check: map[string][]byte{
		"foo": {1, 2, 3},
		"baz": {3, 1, 2},
	},
}}

func newTestCache() *Cache {
	return New(Options{MaxSize: 1 << 9, SegmentSize: 1 << 6})
}

func initTestCache(init map[string][]byte) *Cache {
	c := newTestCache()
	for k, v := range init {
		if !c.SetBytes(k, v) {
			panic("failed to initialize test cache")
		}
	}

	return c
}

func checkCacheWithContent(c *Cache, check map[string][]byte, withContent bool) bool {
	for k, dk := range check {
		if d, ok := c.GetBytes(k); !ok || withContent && !bytes.Equal(d, dk) {
			return false
		}
	}

	return true
}

func checkCache(c *Cache, check map[string][]byte) bool {
	return checkCacheWithContent(c, check, true)
}

func TestGet(t *testing.T) {
	for _, ti := range getTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			r, found := c.Get(ti.key)
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

			if found := c.GetKey(ti.key); found != ti.exists {
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

			d, found := c.GetBytes(ti.key)
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

			w, ok := c.Set(ti.key)
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

			if !c.SetKey(ti.key) {
				t.Error("failed to set key")
				return
			}

			if !checkCacheWithContent(c, ti.check, false) {
				t.Error("invalid cache state")
			}
		})
	}
}

func TestSetBytesFails(t *testing.T) {
	c := New(Options{MaxSize: 6, SegmentSize: 3})
	defer c.Close()

	if c.SetBytes("foo", []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Error("failed to fail")
	}
}

func TestSetBytes(t *testing.T) {
	for _, ti := range setTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			if !c.SetBytes(ti.key, ti.data) {
				t.Error("failed to set the item")
				return
			}

			if !checkCache(c, ti.check) {
				t.Error("invalid cache state")
			}
		})
	}
}

func TestDel(t *testing.T) {
	for _, ti := range delTests {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			c.Del(ti.key)
			if !checkCache(c, ti.check) {
				t.Error("failed to delete the key")
			}
		})
	}
}

func TestClose(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	c.SetBytes("foo", []byte{1, 2, 3})

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

		c.SetBytes("bar", []byte{2, 3, 1})
		if c.GetKey("bar") {
			t.Error("failed to close cache")
			return
		}

		c.Del("foo")
		c.Close()
	}()
}

func TestReadZero(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}) {
		t.Error("failed to set bytes")
		return
	}

	r, ok := c.Get("foo")
	if !ok {
		t.Error("failed to retrieve item from cache")
		return
	}

	defer r.Close()

	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Error("invalid result on reading zero bytes")
	}
}

func TestWriteZero(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("foo")
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
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("foo")
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	var wg sync.WaitGroup
	read := func() {
		if r, ok := c.Get("foo"); ok {
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

func TestReadFromDeletedEntry(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}) {
		t.Error("failed to set item")
		return
	}

	r, ok := c.Get("foo")
	if !ok {
		t.Error("failed to get reader")
		return
	}

	defer r.Close()

	c.Del("foo")

	p := make([]byte, 3)
	if _, err := r.Read(p); err != ErrItemDiscarded {
		t.Error("failed to get discarded error", err)
	}
}

func TestWriteToDeletedEntry(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("foo")
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if _, err := w.Write([]byte{1, 2, 3}); err != nil {
		t.Error("failed to write to item")
		return
	}

	c.Del("foo")

	if _, err := w.Write([]byte{4, 5, 6}); err != ErrItemDiscarded {
		t.Error("failed to get discarded error", err)
		return
	}
}

func TestWriteToCompleteEntry(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("foo")
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

func TestCloseWriteTwice(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("foo")
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
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}) {
		t.Error("failed to set item")
		return
	}

	if !c.SetBytes("bar", []byte{4, 5, 6, 1, 2, 3}) {
		t.Error("failed to set item")
		return
	}

	if c.GetKey("foo") {
		t.Error("failed to evict item")
		return
	}

	if !c.GetKey("bar") {
		t.Error("failed to set item")
		return
	}
}

func TestDoNotEvictCurrent(t *testing.T) {
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}) {
		t.Error("failed to set item")
		return
	}

	w, ok := c.Set("bar")
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if !c.SetBytes("baz", []byte{4, 5, 6}) {
		t.Error("failed to set item")
		return
	}

	if d, ok := c.GetBytes("baz"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
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

	if c.GetKey("foo") || c.GetKey("baz") {
		t.Error("failed to evict item")
		return
	}

	if d, ok := c.GetBytes("bar"); !ok || !bytes.Equal(d, []byte{7, 8, 9, 0, 1, 2}) {
		t.Error("data check failed", d)
		return
	}
}

func TestFailToEvict(t *testing.T) {
	c := New(Options{MaxSize: 6, SegmentSize: 3})
	defer c.Close()

	if !c.SetBytes("foo", []byte{1, 2, 3}) {
		t.Error("failed to set initial item")
		return
	}

	w, ok := c.Set("bar")
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
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()

	w, ok := c.Set("foo")
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write item")
		return
	}

	r, ok := c.Get("foo")
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
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	w, ok := c.Set("foo")
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
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()
	if _, ok := c.Set("123456789012345"); ok {
		t.Error("too large key was set")
	}

	if c.GetKey("123456789012345") {
		t.Error("too large key was set")
	}
}

func TestWriteAtSegmentBoundary(t *testing.T) {
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()

	w, ok := c.Set("foo")
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

	if b, ok := c.GetBytes("foo"); !ok || !bytes.Equal(b, []byte{1, 2, 3, 4, 5, 6}) {
		t.Error("failed to read item", ok, b)
	}
}

func TestWriteToItemWithEmptyKey(t *testing.T) {
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()

	w, ok := c.Set("")
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

	if b, ok := c.GetBytes(""); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to read item", ok, b)
	}
}

func TestAllocateAndInsert(t *testing.T) {
	c := New(Options{MaxSize: 24, SegmentSize: 6})
	defer c.Close()

	w, ok := c.Set("foo")
	if !ok {
		t.Error("failed to set item")
		return
	}

	defer w.Close()

	if !c.SetKey("bar") {
		t.Error("failed to set key")
		return
	}

	if _, err := w.Write([]byte{1, 2, 3, 4, 5, 6}); err != nil {
		t.Error(err)
	}
}

func TestGetEmptyItem(t *testing.T) {
	c := New(Options{MaxSize: 12, SegmentSize: 6})
	defer c.Close()

	if !c.SetKey("") {
		t.Error("failed to set item with empty key")
	}

	if !c.GetKey("") {
		t.Error("failed to get item with empty key")
	}
}

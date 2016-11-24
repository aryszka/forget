package forget

import (
	"bytes"
	"io"
	"sync"
	"testing"
	"time"
)

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

func checkCache(c *Cache, check map[string][]byte) bool {
	for k, dk := range check {
		if d, ok := c.GetBytes(k); !ok || !bytes.Equal(d, dk) {
			return false
		}
	}

	return true
}

func TestGet(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init map[string][]byte
		key  string
		data []byte
		ok   bool
	}{{
		"empty",
		nil,
		"foo",
		nil,
		false,
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
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			d, ok := c.GetBytes(ti.key)
			if ok != ti.ok {
				t.Error("invalid result")
				return
			}

			if ok && !bytes.Equal(d, ti.data) {
				t.Error("invalid result data", d, ti.data)
				return
			}
		})
	}
}

func TestSet(t *testing.T) {
	for _, ti := range []struct {
		msg   string
		init  map[string][]byte
		key   string
		data  []byte
		check map[string][]byte
	}{{
		"empty",
		nil,
		"foo",
		[]byte{1, 2, 3},
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
		map[string][]byte{
			"foo": {1, 2, 3},
			"bar": {3, 2, 1},
			"baz": {3, 1, 2},
		},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := initTestCache(ti.init)
			defer c.Close()

			c.SetBytes(ti.key, ti.data)
			if !checkCache(c, ti.check) {
				t.Error("failed to set the key")
			}
		})
	}
}

func TestDel(t *testing.T) {
	for _, ti := range []struct {
		msg   string
		init  map[string][]byte
		key   string
		check map[string][]byte
	}{{
		"empty",
		nil,
		"foo",
		nil,
	}, {
		"not found",
		map[string][]byte{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
			"baz": {3, 1, 2},
		},
		"qux",
		map[string][]byte{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
			"baz": {3, 1, 2},
		},
	}, {
		"found",
		map[string][]byte{
			"foo": {1, 2, 3},
			"bar": {2, 3, 1},
			"baz": {3, 1, 2},
		},
		"bar",
		map[string][]byte{
			"foo": {1, 2, 3},
			"baz": {3, 1, 2},
		},
	}} {
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

	if _, ok := c.GetBytes("foo"); ok {
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
		if _, ok := c.GetBytes("bar"); ok {
			t.Error("failed to close cache")
			return
		}

		c.Del("foo")
		c.Close()
	}()
}

func TestWaitForData(t *testing.T) {
	c := newTestCache()
	defer c.Close()

	w, ok := c.Set("foo")
	if !ok {
		t.Error("failed to set item")
		return
	}

	var wg sync.WaitGroup
	read := func() {
		if r, ok := c.Get("foo"); ok {
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

package forget

import (
	"bytes"
	"testing"
	"time"
)

func TestSingleSpaceGet(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	if _, ok := c.Get("foo"); ok {
		t.Error("unexpected cache item")
		return
	}

	c.Set("foo", []byte{1, 2, 3}, time.Hour)
	c.Set("bar", []byte{4, 5, 6}, time.Hour)

	if d, ok := c.Get("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	if d, ok := c.Get("bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	if _, ok := c.Get("baz"); ok {
		t.Error("unexpected cache item")
		return
	}
}

func TestSingleSpaceSet(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	c.Set("foo", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.Get("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	c.Set("foo", []byte{4, 5, 6}, time.Hour)
	if d, ok := c.Get("foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Set("bar", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.Get("foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
}

func TestSingleSpaceDelete(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	c.Del("foo")

	c.Set("foo", []byte{1, 2, 3}, time.Hour)
	c.Set("bar", []byte{4, 5, 6}, time.Hour)

	c.Del("baz")
	if d, ok := c.Get("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("foo")
	if _, ok := c.Get("foo"); ok {
		t.Error("unexpected cache item")
		return
	}
	if d, ok := c.Get("bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
}

func TestSingleSpaceStatus(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	s := c.Status()
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	c.Set("foo", []byte{1, 2, 3}, time.Hour)
	c.Set("bar", []byte{3, 4, 5}, time.Hour)
	c.Set("baz", []byte{7, 8, 9}, time.Hour)
	c.Set("qux", []byte{0, 1, 2}, time.Hour)
	c.Set("quux", []byte{0, 1, 2}, time.Hour)

	s = c.Status()
	if s.Len != 5 || s.Segments != 5 || s.Effective != 31 {
		t.Error("unexpected status")
		return
	}
}

func TestSingleSpaceClose(t *testing.T) {
	c := NewSingleSpace(Options{})
	c.Set("foo", []byte{1, 2, 3}, time.Hour)

	c.Close()
	c.Set("bar", []byte{1, 2, 3}, time.Hour)
	c.Get("foo")
	c.Get("bar")
	c.Del("foo")
	c.Close()
	c.Close()
}

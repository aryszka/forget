package forget

import (
	"bytes"
	"io"
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

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("bar", []byte{4, 5, 6}, time.Hour)

	b := bytes.NewBuffer(nil)
	if r, ok := c.Get("foo"); !ok {
		t.Error("invalid cache item")
		return
	} else if n, err := io.Copy(b, r); n != 3 || err != nil || !bytes.Equal(b.Bytes(), []byte{1, 2, 3}) {
		t.Error("invalid cache item data")
		return
	}

	b = bytes.NewBuffer(nil)
	if r, ok := c.Get("bar"); !ok {
		t.Error("invalid cache item")
		return
	} else if n, err := io.Copy(b, r); n != 3 || err != nil || !bytes.Equal(b.Bytes(), []byte{4, 5, 6}) {
		t.Error("invalid cache item data")
		return
	}

	if _, ok := c.Get("baz"); ok {
		t.Error("unexpected cache item")
		return
	}
}

func TestSingleSpaceGetBytes(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	if _, ok := c.GetBytes("foo"); ok {
		t.Error("unexpected cache item")
		return
	}

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("bar", []byte{4, 5, 6}, time.Hour)

	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	if d, ok := c.GetBytes("bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	if _, ok := c.GetBytes("baz"); ok {
		t.Error("unexpected cache item")
		return
	}
}

func TestSingleSpaceSet(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	w := c.Set("foo", 3, time.Hour)
	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write cache item")
		return
	}
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	w = c.Set("foo", 3, time.Hour)
	if n, err := w.Write([]byte{4, 5, 6}); n != 3 || err != nil {
		t.Error("failed to overwrite cache item")
		return
	}
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	w = c.Set("bar", 3, time.Hour)
	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write new cache item")
		return
	}
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
}

func TestSingleSpaceSetBytes(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	c.SetBytes("foo", []byte{4, 5, 6}, time.Hour)
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.SetBytes("bar", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
}

func TestSingleSpaceDelete(t *testing.T) {
	c := NewSingleSpace(Options{})
	defer c.Close()

	c.Del("foo")

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("bar", []byte{4, 5, 6}, time.Hour)

	c.Del("baz")
	if d, ok := c.GetBytes("foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("foo")
	if _, ok := c.GetBytes("foo"); ok {
		t.Error("unexpected cache item")
		return
	}
	if d, ok := c.GetBytes("bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
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

	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("bar", []byte{3, 4, 5}, time.Hour)
	c.SetBytes("baz", []byte{7, 8, 9}, time.Hour)
	c.SetBytes("qux", []byte{0, 1, 2}, time.Hour)
	c.SetBytes("quux", []byte{0, 1, 2}, time.Hour)

	s = c.Status()
	if s.Len != 5 || s.Segments != 5 || s.Effective != 31 {
		t.Error("unexpected status")
		return
	}
}

func TestSingleSpaceClose(t *testing.T) {
	c := NewSingleSpace(Options{})
	c.SetBytes("foo", []byte{1, 2, 3}, time.Hour)

	c.Close()
	c.SetBytes("bar", []byte{1, 2, 3}, time.Hour)
	c.GetBytes("foo")
	c.GetBytes("bar")
	c.Del("foo")
	c.Close()
	c.Close()
}

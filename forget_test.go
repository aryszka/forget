package forget

import (
	"bytes"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	c := New(1 << 9)
	defer c.Close()

	if _, ok := c.Get("s1", "foo"); ok {
		t.Error("unexpected cache item")
		return
	}

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s2", "bar", []byte{4, 5, 6}, time.Hour)

	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	if _, ok := c.Get("s2", "foo"); ok {
		t.Error("unexpected cache item")
		return
	}

	if d, ok := c.Get("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	if _, ok := c.Get("s1", "bar"); ok {
		t.Error("unexpected cache item")
		return
	}
}

func TestSet(t *testing.T) {
	c := New(1 << 9)
	defer c.Close()

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	c.Set("s1", "foo", []byte{4, 5, 6}, time.Hour)
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Set("s1", "bar", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("s1", "bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	c.Set("s2", "foo", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("s1", "bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("s2", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
}

func TestDelete(t *testing.T) {
	c := New(1 << 9)
	defer c.Close()

	c.Del("s1", "foo")

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s2", "bar", []byte{4, 5, 6}, time.Hour)

	c.Del("s1", "bar")
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("s2", "foo")
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("s2", "foo")
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.Get("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("s1", "foo")
	if _, ok := c.Get("s1", "foo"); ok {
		t.Error("unexpected cache item")
		return
	}
	if d, ok := c.Get("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
}

func TestKeyspaceStatus(t *testing.T) {
	c := New(1 << 9)
	defer c.Close()

	s := c.StatusOf("s1")
	if s == nil || s.Len != 0 || s.Size != 0 {
		t.Error("unexpected status")
		return
	}

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.Set("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.Set("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.Set("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.StatusOf("s1")
	if s == nil || s.Len != 2 || s.Size != 12 {
		t.Error("unexpected status")
		return
	}
	s = c.StatusOf("s2")
	if s == nil || s.Len != 3 || s.Size != 19 {
		t.Error("unexpected status")
		return
	}
	s = c.StatusOf("s3")
	if s == nil || s.Len != 0 || s.Size != 0 {
		t.Error("unexpected status")
		return
	}
}

func TestStatus(t *testing.T) {
	c := New(1 << 9)
	defer c.Close()

	s := c.Status()
	if s == nil || len(s.Keyspaces) != 0 || s.Len != 0 || s.Size != 0 {
		t.Error("unexpected status")
		return
	}

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.Set("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.Set("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.Set("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.Status()
	if s == nil || len(s.Keyspaces) != 2 || s.Len != 5 || s.Size != 31 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s1"].Len != 2 || s.Keyspaces["s1"].Size != 12 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s2"].Len != 3 || s.Keyspaces["s2"].Size != 19 {
		t.Error("unexpected status")
		return
	}
}

func TestClose(t *testing.T) {
	c := New(1 << 9)
	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)

	c.Close()
	c.Set("s2", "bar", []byte{1, 2, 3}, time.Hour)
	c.Get("s1", "foo")
	c.Get("s2", "bar")
	c.Del("s1", "foo")
	c.Close()
	c.Close()
}

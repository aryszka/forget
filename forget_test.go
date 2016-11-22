package forget

import (
	"bytes"
	"testing"
	"time"
)

func TestGet(t *testing.T) {
	c := New(Options{})
	defer c.Close()

	if _, ok := c.GetBytes("s1", "foo"); ok {
		t.Error("unexpected cache item")
		return
	}

	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("s2", "bar", []byte{4, 5, 6}, time.Hour)

	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item", ok, d)
		return
	}

	if _, ok := c.GetBytes("s2", "foo"); ok {
		t.Error("unexpected cache item")
		return
	}

	if d, ok := c.GetBytes("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	if _, ok := c.GetBytes("s1", "bar"); ok {
		t.Error("unexpected cache item")
		return
	}
}

func TestSet(t *testing.T) {
	c := New(Options{})
	defer c.Close()

	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	c.SetBytes("s1", "foo", []byte{4, 5, 6}, time.Hour)
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.SetBytes("s1", "bar", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("s1", "bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}

	c.SetBytes("s2", "foo", []byte{1, 2, 3}, time.Hour)
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("s1", "bar"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("s2", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
}

func TestDelete(t *testing.T) {
	c := New(Options{})
	defer c.Close()

	c.Del("s1", "foo")

	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("s2", "bar", []byte{4, 5, 6}, time.Hour)

	c.Del("s1", "bar")
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("s2", "foo")
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("s2", "foo")
	if d, ok := c.GetBytes("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("invalid cache item")
		return
	}
	if d, ok := c.GetBytes("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}

	c.Del("s1", "foo")
	if _, ok := c.GetBytes("s1", "foo"); ok {
		t.Error("unexpected cache item")
		return
	}
	if d, ok := c.GetBytes("s2", "bar"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("invalid cache item")
		return
	}
}

func TestKeyspaceStatus(t *testing.T) {
	c := New(Options{})
	defer c.Close()

	s := c.StatusOf("s1")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.SetBytes("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.SetBytes("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.SetBytes("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.StatusOf("s1")
	if s.Len != 2 || s.Segments != 2 || s.Effective != 12 {
		t.Error("unexpected status")
		return
	}
	s = c.StatusOf("s2")
	if s.Len != 3 || s.Segments != 3 || s.Effective != 19 {
		t.Error("unexpected status")
		return
	}
	s = c.StatusOf("s3")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}
}

func TestStatus(t *testing.T) {
	c := New(Options{})
	defer c.Close()

	s := c.Status()
	if len(s.Keyspaces) != 0 || s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.SetBytes("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.SetBytes("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.SetBytes("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.SetBytes("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.Status()
	if len(s.Keyspaces) != 2 || s.Len != 5 || s.Segments != 5 || s.Effective != 31 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s1"].Len != 2 || s.Keyspaces["s1"].Segments != 2 || s.Keyspaces["s1"].Effective != 12 {
		t.Error("unexpected status", s)
		return
	}

	if s.Keyspaces["s2"].Len != 3 || s.Keyspaces["s2"].Segments != 3 || s.Keyspaces["s2"].Effective != 19 {
		t.Error("unexpected status")
		return
	}
}

func TestClose(t *testing.T) {
	c := New(Options{})
	c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)

	c.Close()
	c.SetBytes("s2", "bar", []byte{1, 2, 3}, time.Hour)
	c.GetBytes("s1", "foo")
	c.GetBytes("s2", "bar")
	c.Del("s1", "foo")
	c.Close()
	c.Close()
}

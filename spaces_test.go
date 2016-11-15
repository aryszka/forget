package forget

import (
	"bytes"
	"testing"
	"time"
)

func TestSpaces(t *testing.T) {
	c := New(1 << 9)

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	if _, ok := c.Get("s2", "foo"); ok {
		t.Error("item returned from the wrong space")
	}
	if _, ok := c.Get("s1", "foo"); !ok {
		t.Error("failed to return item from the space")
	}

	c.Del("s2", "foo")
	if _, ok := c.Get("s1", "foo"); !ok {
		t.Error("item deleted from the wrong space")
	}

	c.Del("s1", "foo")
	if _, ok := c.Get("s1", "foo"); ok {
		t.Error("failed to delete item from the space")
	}

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s2", "foo", []byte{4, 5, 6}, time.Hour)
	if d, ok := c.Get("s1", "foo"); !ok || !bytes.Equal(d, []byte{1, 2, 3}) {
		t.Error("failed to get item from the right space")
	}
	if d, ok := c.Get("s2", "foo"); !ok || !bytes.Equal(d, []byte{4, 5, 6}) {
		t.Error("failed to get item from the right space")
	}
}

func TestEvictFromOwnSpace(t *testing.T) {
	c := New(15)

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s2", "foo", []byte{4, 5, 6}, time.Hour)
	c.Set("s1", "bar", []byte{7, 8, 9}, time.Hour)

	if _, ok := c.Get("s1", "foo"); ok {
		t.Error("failed to evict from the right space")
	}
	if _, ok := c.Get("s2", "foo"); !ok {
		t.Error("failed to evict from the right space")
	}
	if _, ok := c.Get("s1", "bar"); !ok {
		t.Error("failed to set the new entry in the right space")
	}
}

func TestEvictFromOtherSpace(t *testing.T) {
	c := New(9)

	c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.Set("s2", "foo", []byte{7, 8, 9}, time.Hour)

	if _, ok := c.Get("s1", "foo"); ok {
		t.Error("failed to evict from the right space")
	}
	if _, ok := c.Get("s2", "foo"); !ok {
		t.Error("failed to evict from the right space")
	}
}

func TestEvictFromManySpaces(t *testing.T) {
	c := New(36)
	init := map[string][]string{
		"s1": []string{"foo", "bar"},
		"s2": []string{"foo", "bar", "baz"},
		"s3": []string{"foo"},
	}
	for s, keys := range init {
		for _, k := range keys {
			c.Set(s, k, make([]byte, 3), time.Hour)
		}
	}

	c.Set("s3", "qux", make([]byte, 33), time.Hour)
	for s, keys := range init {
		for _, k := range keys {
			if _, ok := c.Get(s, k); ok {
				t.Error("failed to evict item", s, k)
			}
		}
	}

	if _, ok := c.Get("s3", "qux"); !ok {
		t.Error("failed to set item")
	}
}

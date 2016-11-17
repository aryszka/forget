package forget

import (
	"bytes"
	"testing"
	"time"
)

type testInit map[string]map[string][]byte

type testDataItem struct {
	space string
	key   string
	ok    bool
	data  []byte
}

func createTestCacheWithSize(d testInit, maxSize int) *cache {
	c := newCache(maxSize)
	for ks, s := range d {
		for k, dk := range s {
			c.set(ks, k, dk, time.Hour)
		}
	}

	return c
}

func createTestCache(d testInit) *cache {
	return createTestCacheWithSize(d, 1<<9)
}

func checkData(t *testing.T, items []testDataItem, c *cache) {
	for _, i := range items {
		if d, ok := c.get(i.space, i.key); ok != i.ok {
			t.Error("unexpected response status", i.space, i.key, ok, i.ok)
			return
		} else if !bytes.Equal(d, i.data) {
			t.Error("invalid response data", i.space, i.key, d, i.data)
		}
	}
}

func TestCacheGet(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init testInit
		testDataItem
	}{{
		"empty cache",
		nil,
		testDataItem{
			space: "s1",
			key:   "foo",
		},
	}, {
		"not found key",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		testDataItem{
			space: "s1",
			key:   "baz",
		},
	}, {
		"key found",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		testDataItem{
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCache(ti.init)
			d, ok := c.get(ti.space, ti.key)

			if ok != ti.ok {
				t.Error("unexpected response status", ok, ti.ok)
				return
			}

			if !bytes.Equal(d, ti.data) {
				t.Error("invalid response data", d, ti.data)
			}
		})
	}
}

func TestCacheSet(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init testInit
		testDataItem
		checks []testDataItem
	}{{
		"set in empty",
		nil,
		testDataItem{
			space: "s1",
			key:   "foo",
			data:  []byte{1, 2, 3},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
		}, {
			space: "s2",
			key:   "baz",
		}, {
			space: "s2",
			key:   "qux",
		}},
	}, {
		"set in addition, new space",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
		},
		testDataItem{
			space: "s2",
			key:   "baz",
			data:  []byte{7, 8, 9},
		},
		[]testDataItem{{
			"s1",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"baz",
			true,
			[]byte{7, 8, 9},
		}, {
			"s2",
			"qux",
			false,
			nil,
		}},
	}, {
		"set in addition, same space",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		testDataItem{
			space: "s1",
			key:   "bar",
			data:  []byte{4, 5, 6},
		},
		[]testDataItem{{
			"s1",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"baz",
			true,
			[]byte{7, 8, 9},
		}, {
			"s2",
			"qux",
			true,
			[]byte{0, 1, 2},
		}},
	}, {
		"overwrite",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		testDataItem{
			space: "s1",
			key:   "bar",
			data:  []byte{3, 4, 5},
		},
		[]testDataItem{{
			"s1",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"s1",
			"bar",
			true,
			[]byte{3, 4, 5},
		}, {
			"s2",
			"baz",
			true,
			[]byte{7, 8, 9},
		}, {
			"s2",
			"qux",
			true,
			[]byte{0, 1, 2},
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCache(ti.init)
			c.set(ti.space, ti.key, ti.data, time.Hour)
			checkData(t, ti.checks, c)
		})
	}
}

func TestCacheDelete(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init testInit
		testDataItem
		checks []testDataItem
	}{{
		"empty cache",
		nil,
		testDataItem{
			key: "foo",
		},
		nil,
	}, {
		"not found key",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		testDataItem{
			space: "s1",
			key:   "baz",
		},
		[]testDataItem{{
			"s1",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"baz",
			true,
			[]byte{7, 8, 9},
		}, {
			"s2",
			"qux",
			true,
			[]byte{0, 1, 2},
		}},
	}, {
		"delete",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		testDataItem{
			space: "s1",
			key:   "foo",
		},
		[]testDataItem{{
			"s1",
			"foo",
			false,
			nil,
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"baz",
			true,
			[]byte{7, 8, 9},
		}, {
			"s2",
			"qux",
			true,
			[]byte{0, 1, 2},
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCache(ti.init)
			c.del(ti.space, ti.key)
			checkData(t, ti.checks, c)
		})

	}
}

func TestCacheExpiration(t *testing.T) {
	c := newCache(1 << 9)
	c.set("s1", "foo", []byte{1, 2, 3}, 24*time.Millisecond)
	time.Sleep(12 * time.Millisecond)
	c.get("s1", "foo")
	time.Sleep(24 * time.Millisecond)
	if _, ok := c.get("s1", "foo"); ok {
		t.Error("failed to expire item")
	}
}

func TestCacheOverSize(t *testing.T) {
	c := newCache(4)
	c.set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	if _, ok := c.get("s1", "foo"); ok {
		t.Error("failed to reject oversized data")
	}
}

func TestCacheEvict(t *testing.T) {
	for _, ti := range []struct {
		msg     string
		init    testInit
		maxSize int
		warmup  map[string]string
		testDataItem
		checks []testDataItem
	}{{
		"no evict",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		33,
		nil,
		testDataItem{
			space: "s2",
			key:   "quux",
			data:  []byte{3, 4, 5},
		},
		[]testDataItem{{
			"s1",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"baz",
			true,
			[]byte{7, 8, 9},
		}, {
			"s2",
			"qux",
			true,
			[]byte{0, 1, 2},
		}, {
			"s2",
			"quux",
			true,
			[]byte{3, 4, 5},
		}},
	}, {
		"evict from own space",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
		},
		27,
		map[string]string{"s2": "qux"},
		testDataItem{
			space: "s2",
			key:   "quux",
			data:  []byte{3, 4, 5},
		},
		[]testDataItem{{
			"s1",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"qux",
			true,
			[]byte{0, 1, 2},
		}, {
			"s2",
			"quux",
			true,
			[]byte{3, 4, 5},
		}},
	}, {
		"evict from other spaces",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
				"bar": []byte{4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
			"s3": map[string][]byte{
				"foo": []byte{3, 4, 5},
				"qux": []byte{6, 7, 8},
			},
		},
		36,
		map[string]string{"s1": "bar", "s3": "qux"},
		testDataItem{
			space: "s2",
			key:   "quux",
			data:  []byte("12345789012345678"),
		},
		[]testDataItem{{
			"s1",
			"foo",
			false,
			nil,
		}, {
			"s1",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"s2",
			"baz",
			false,
			nil,
		}, {
			"s2",
			"qux",
			false,
			nil,
		}, {
			"s2",
			"quux",
			true,
			[]byte("12345789012345678"),
		}, {
			"s3",
			"foo",
			false,
			nil,
		}, {
			"s3",
			"qux",
			true,
			[]byte{6, 7, 8},
		}},
	}, {
		"zero another space",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
			"s3": map[string][]byte{
				"foo": []byte{3, 4, 5},
				"bar": []byte{6, 7, 8},
				"baz": []byte{9, 0, 1},
			},
		},
		36,
		map[string]string{"s3": "baz"},
		testDataItem{
			space: "s2",
			key:   "quux",
			data:  []byte("123456789012345678901"),
		},
		[]testDataItem{{
			"s1",
			"foo",
			false,
			nil,
		}, {
			"s2",
			"baz",
			false,
			nil,
		}, {
			"s2",
			"qux",
			false,
			nil,
		}, {
			"s2",
			"quux",
			true,
			[]byte("123456789012345678901"),
		}, {
			"s3",
			"foo",
			false,
			nil,
		}, {
			"s3",
			"bar",
			false,
			nil,
		}, {
			"s3",
			"baz",
			true,
			[]byte{9, 0, 1},
		}},
	}, {
		"zero all spaces",
		testInit{
			"s1": map[string][]byte{
				"foo": []byte{1, 2, 3},
			},
			"s2": map[string][]byte{
				"baz": []byte{7, 8, 9},
				"qux": []byte{0, 1, 2},
			},
			"s3": map[string][]byte{
				"foo": []byte{3, 4, 5},
				"bar": []byte{6, 7, 8},
				"baz": []byte{9, 0, 1},
			},
		},
		36,
		map[string]string{"s3": "baz"},
		testDataItem{
			space: "s2",
			key:   "quux",
			data:  []byte("12345678901234567890123456789"),
		},
		[]testDataItem{{
			"s1",
			"foo",
			false,
			nil,
		}, {
			"s2",
			"baz",
			false,
			nil,
		}, {
			"s2",
			"qux",
			false,
			nil,
		}, {
			"s2",
			"quux",
			true,
			[]byte("12345678901234567890123456789"),
		}, {
			"s3",
			"foo",
			false,
			nil,
		}, {
			"s3",
			"bar",
			false,
			nil,
		}, {
			"s3",
			"baz",
			false,
			nil,
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCacheWithSize(ti.init, ti.maxSize)
			for s, k := range ti.warmup {
				c.get(s, k)
			}

			c.set(ti.space, ti.key, ti.data, time.Hour)
			checkData(t, ti.checks, c)
		})

	}
}

func TestCacheKeyspaceStatus(t *testing.T) {
	c := newCache(1 << 9)

	s := c.getKeyspaceStatus("s1")
	if s == nil || s.Len != 0 || s.Size != 0 {
		t.Error("unexpected status")
		return
	}

	c.set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.set("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.set("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.set("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.set("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.getKeyspaceStatus("s1")
	if s == nil || s.Len != 2 || s.Size != 12 {
		t.Error("unexpected status")
		return
	}
	s = c.getKeyspaceStatus("s2")
	if s == nil || s.Len != 3 || s.Size != 19 {
		t.Error("unexpected status")
		return
	}
	s = c.getKeyspaceStatus("s3")
	if s == nil || s.Len != 0 || s.Size != 0 {
		t.Error("unexpected status")
		return
	}
}

func TestCacheStatus(t *testing.T) {
	c := newCache(1 << 9)

	s := c.getStatus()
	if s == nil || len(s.Keyspaces) != 0 || s.Len != 0 || s.Size != 0 {
		t.Error("unexpected status")
		return
	}

	c.set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.set("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.set("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.set("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.set("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.getStatus()
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

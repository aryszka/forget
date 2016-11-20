package forget

import (
	"bytes"
	"testing"
	"time"
)

type testInit map[string]map[string][]byte

type testDataItem struct {
	space      string
	key        string
	ok         bool
	data       []byte
	sizeChange Size
	evicted    map[string]int
}

func createTestCacheWithSize(d testInit, o Options) *cache {
	c := newCache(o)
	for ks, s := range d {
		for k, dk := range s {
			c.set(ks, k, dk, time.Hour)
		}
	}

	return c
}

func createTestCache(d testInit) *cache {
	return createTestCacheWithSize(d, Options{MaxSize: 1 << 9, SegmentSize: 1 << 3})
}

func compareEvicted(got, expect map[string]int) bool {
	if len(got) != len(expect) {
		return false
	}

	for k, e := range got {
		if e != expect[k] {
			return false
		}
	}

	return true
}

func checkData(t *testing.T, items []testDataItem, c *cache) {
	for _, i := range items {
		if d, ok, _ := c.get(i.space, i.key); ok != i.ok {
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
			d, ok, _ := c.get(ti.space, ti.key)

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
			space:      "s1",
			key:        "foo",
			data:       []byte{1, 2, 3},
			sizeChange: Size{},
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
			space:      "s2",
			key:        "baz",
			data:       []byte{7, 8, 9},
			sizeChange: Size{},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "baz",
			ok:    true,
			data:  []byte{7, 8, 9},
		}, {
			space: "s2",
			key:   "qux",
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
			space:      "s1",
			key:        "bar",
			data:       []byte{4, 5, 6},
			sizeChange: Size{},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "baz",
			ok:    true,
			data:  []byte{7, 8, 9},
		}, {
			space: "s2",
			key:   "qux",
			ok:    true,
			data:  []byte{0, 1, 2},
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
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{3, 4, 5},
		}, {
			space: "s2",
			key:   "baz",
			ok:    true,
			data:  []byte{7, 8, 9},
		}, {
			space: "s2",
			key:   "qux",
			ok:    true,
			data:  []byte{0, 1, 2},
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCache(ti.init)
			_, sizeChange := c.set(ti.space, ti.key, ti.data, time.Hour)
			if sizeChange != ti.sizeChange {
				t.Error("invalid size change", sizeChange, ti.sizeChange)
			}

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
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "baz",
			ok:    true,
			data:  []byte{7, 8, 9},
		}, {
			space: "s2",
			key:   "qux",
			ok:    true,
			data:  []byte{0, 1, 2},
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
			space:      "s1",
			key:        "foo",
			sizeChange: Size{},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "baz",
			ok:    true,
			data:  []byte{7, 8, 9},
		}, {
			space: "s2",
			key:   "qux",
			ok:    true,
			data:  []byte{0, 1, 2},
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCache(ti.init)

			if sizeChange := c.del(ti.space, ti.key); sizeChange != ti.sizeChange {
				t.Error("invalid size change", sizeChange, ti.sizeChange)
			}

			checkData(t, ti.checks, c)
		})

	}
}

func TestCacheExpiration(t *testing.T) {
	c := newCache(Options{MaxSize: 1 << 9, SegmentSize: 1 << 6})
	c.set("s1", "foo", []byte{1, 2, 3}, 24*time.Millisecond)
	time.Sleep(12 * time.Millisecond)
	c.get("s1", "foo")
	time.Sleep(24 * time.Millisecond)
	if _, ok, sizeChange := c.get("s1", "foo"); ok || sizeChange.zero() {
		t.Error("failed to expire item")
	}
}

func TestCacheOverSize(t *testing.T) {
	c := newCache(Options{MaxSize: 1 << 9, SegmentSize: 1 << 6})
	c.set("s1", "foo", []byte{1, 2, 3}, time.Hour)

	if evicted, sizeChange := c.set("s1", "foo", []byte{1, 2, 3, 4, 5, 6}, time.Hour); len(evicted) != 0 || sizeChange.zero() {
		t.Error("unexpected set success", len(evicted), sizeChange)
	}

	if _, ok, _ := c.get("s1", "foo"); ok {
		t.Error("failed to reject oversized data")
	}
}

func TestCacheEvict(t *testing.T) {
	for _, ti := range []struct {
		msg         string
		init        testInit
		maxSize     int
		segmentSize int
		warmup      map[string]string
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
		6,
		nil,
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte{3, 4, 5},
			sizeChange: Size{},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "baz",
			ok:    true,
			data:  []byte{7, 8, 9},
		}, {
			space: "s2",
			key:   "qux",
			ok:    true,
			data:  []byte{0, 1, 2},
		}, {
			space: "s2",
			key:   "quux",
			ok:    true,
			data:  []byte{3, 4, 5},
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
		6,
		map[string]string{"s2": "qux"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte{3, 4, 5},
			sizeChange: Size{},
			evicted:    map[string]int{"s2": 6},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
			ok:    true,
			data:  []byte{1, 2, 3},
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "qux",
			ok:    true,
			data:  []byte{0, 1, 2},
		}, {
			space: "s2",
			key:   "quux",
			ok:    true,
			data:  []byte{3, 4, 5},
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
		6,
		map[string]string{"s1": "bar", "s3": "qux"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte("12345789012345678"),
			sizeChange: Size{},
			evicted: map[string]int{
				"s1": 6,
				"s2": 12,
				"s3": 6,
			},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
		}, {
			space: "s1",
			key:   "bar",
			ok:    true,
			data:  []byte{4, 5, 6},
		}, {
			space: "s2",
			key:   "baz",
		}, {
			space: "s2",
			key:   "qux",
		}, {
			space: "s2",
			key:   "quux",
			ok:    true,
			data:  []byte("12345789012345678"),
		}, {
			space: "s3",
			key:   "foo",
		}, {
			space: "s3",
			key:   "qux",
			ok:    true,
			data:  []byte{6, 7, 8},
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
		6,
		map[string]string{"s3": "baz"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte("123456789012345678901"),
			sizeChange: Size{},
			evicted: map[string]int{
				"s1": 6,
				"s2": 12,
				"s3": 12,
			},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
		}, {
			space: "s2",
			key:   "baz",
		}, {
			space: "s2",
			key:   "qux",
		}, {
			space: "s2",
			key:   "quux",
			ok:    true,
			data:  []byte("123456789012345678901"),
		}, {
			space: "s3",
			key:   "foo",
		}, {
			space: "s3",
			key:   "bar",
		}, {
			space: "s3",
			key:   "baz",
			ok:    true,
			data:  []byte{9, 0, 1},
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
		6,
		map[string]string{"s3": "baz"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte("12345678901234567890123456789"),
			sizeChange: Size{},
			evicted: map[string]int{
				"s1": 12,
				"s2": 12,
				"s3": 18,
			},
		},
		[]testDataItem{{
			space: "s1",
			key:   "foo",
		}, {
			space: "s2",
			key:   "baz",
		}, {
			space: "s2",
			key:   "qux",
		}, {
			space: "s2",
			key:   "quux",
			ok:    true,
			data:  []byte("12345678901234567890123456789"),
		}, {
			space: "s3",
			key:   "foo",
		}, {
			space: "s3",
			key:   "bar",
		}, {
			space: "s3",
			key:   "baz",
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := createTestCacheWithSize(ti.init, Options{MaxSize: ti.maxSize, SegmentSize: ti.segmentSize})
			for s, k := range ti.warmup {
				c.get(s, k)
			}

			if evicted, sizeChange := c.set(ti.space, ti.key, ti.data, time.Hour); sizeChange != ti.sizeChange {
				t.Error("invalid size change", sizeChange, ti.sizeChange)
			} else if !compareEvicted(evicted, ti.evicted) {
				t.Error("invalid evicted", evicted, ti.evicted)
			}

			checkData(t, ti.checks, c)
		})

	}
}

func TestCacheKeyspaceStatus(t *testing.T) {
	c := newCache(Options{MaxSize: 1 << 9, SegmentSize: 6})

	s := c.getKeyspaceStatus("s1")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	c.set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.set("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.set("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.set("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.set("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.getKeyspaceStatus("s1")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}
	s = c.getKeyspaceStatus("s2")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}
	s = c.getKeyspaceStatus("s3")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}
}

func TestCacheStatus(t *testing.T) {
	c := newCache(Options{MaxSize: 1 << 9, SegmentSize: 6})

	s := c.getStatus()
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	c.set("s1", "foo", []byte{1, 2, 3}, time.Hour)
	c.set("s1", "bar", []byte{3, 4, 5}, time.Hour)
	c.set("s2", "baz", []byte{7, 8, 9}, time.Hour)
	c.set("s2", "qux", []byte{0, 1, 2}, time.Hour)
	c.set("s2", "quux", []byte{0, 1, 2}, time.Hour)

	s = c.getStatus()
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s1"].Len != 2 || s.Keyspaces["s1"].Segments != 2 || s.Keyspaces["s1"].Effective != 12 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s2"].Len != 2 || s.Keyspaces["s2"].Segments != 2 || s.Keyspaces["s2"].Effective != 12 {
		t.Error("unexpected status")
		return
	}
}

func TestCopy(t *testing.T) {
	c := newCache(Options{MaxSize: 1 << 9, SegmentSize: 6})
	b := []byte{1, 2, 3}
	c.set("s1", "foo", b, time.Hour)

	b[0] = 4
	b, _, _ = c.get("s1", "foo")
	if b[0] != 1 {
		t.Error("failed to copy on set")
	}

	b[2] = 1
	b, _, _ = c.get("s1", "foo")
	if b[0] != 1 {
		t.Error("failed to copy on get")
	}
}

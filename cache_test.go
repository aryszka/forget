package forget

import (
	"bytes"
	"io"
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

type weakHash struct{}

func (wh weakHash) Write(b []byte) (int, error) { return len(b), nil }
func (wh weakHash) Sum(b []byte) []byte         { return b }
func (wh weakHash) Reset()                      {}
func (wh weakHash) Size() int                   { return 0 }
func (wh weakHash) BlockSize() int              { return 1 }
func (wh weakHash) Sum64() uint64               { return 42 }

var testOptions = Options{MaxSize: 1 << 9, SegmentSize: 1 << 3}

func createTestCacheWithSize(d testInit, o Options) *cache {
	c := newCache(o)
	for ks, s := range d {
		for k, dk := range s {
			e, _, _, _ := c.set(ks, k, len(dk), time.Hour)
			c.write(e, 3, dk)
		}
	}

	return c
}

func createTestCache(d testInit) *cache {
	return createTestCacheWithSize(d, testOptions)
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

func readNToEnd(c *cache, e *entry, offset, count int) ([]byte, bool) {
	p := make([]byte, count)
	if count > 0 {
		if n, err := c.read(e, offset, p); n != count || err != nil {
			return p, false
		}
	}

	if _, err := c.read(e, offset+count, make([]byte, count)); err != io.EOF {
		return p, false
	}

	return p, true
}

func writeNFull(c *cache, e *entry, offset int, data []byte) bool {
	if n, err := c.write(e, offset, data); n != len(data) || err != nil {
		return false
	}

	if _, err := c.write(e, offset+len(data), []byte{1, 2, 3}); err != ErrWriteLimit {
		return false
	}

	return true
}

func checkData(t *testing.T, items []testDataItem, c *cache) {
	for _, i := range items {
		e, ok, _ := c.get(i.space, i.key)
		if ok != i.ok {
			t.Error("unexpected response status", i.space, i.key, ok, i.ok)
			return
		}

		if !ok {
			return
		}

		if b, ok := readNToEnd(c, e, len(i.key), len(i.data)); !ok || !bytes.Equal(b, i.data) {
			t.Error("failed to read from entry")
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
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
			e, ok, _ := c.get(ti.space, ti.key)

			if ok != ti.ok {
				t.Error("unexpected response status", ok, ti.ok)
				return
			}

			if !ok {
				return
			}

			if d, ok := readNToEnd(c, e, len(ti.key), len(ti.data)); !ok || !bytes.Equal(d, ti.data) {
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
			sizeChange: Size{Len: 1, Segments: 1, Effective: 6},
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
		},
		testDataItem{
			space:      "s2",
			key:        "baz",
			data:       []byte{7, 8, 9},
			sizeChange: Size{Len: 1, Segments: 1, Effective: 6},
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
				"foo": {1, 2, 3},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
		},
		testDataItem{
			space:      "s1",
			key:        "bar",
			data:       []byte{4, 5, 6},
			sizeChange: Size{Len: 1, Segments: 1, Effective: 6},
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
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
			e, ok, _, sizeChange := c.set(ti.space, ti.key, len(ti.data), time.Hour)
			if !ok {
				t.Error("failed to set entry")
			}

			if sizeChange != ti.sizeChange {
				t.Error("invalid size change", sizeChange, ti.sizeChange)
			}

			if !writeNFull(c, e, len(ti.key), ti.data) {
				t.Error("failed to write data")
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
		},
		testDataItem{
			space:      "s1",
			key:        "foo",
			sizeChange: Size{Len: -1, Segments: -1, Effective: -6},
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
	c := newCache(testOptions)

	if e, ok, _, _ := c.set("s1", "foo", 3, 24*time.Millisecond); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to write entry data")
	}

	time.Sleep(12 * time.Millisecond)
	c.get("s1", "foo")
	time.Sleep(24 * time.Millisecond)
	if _, ok, sizeChange := c.get("s1", "foo"); ok || sizeChange.zero() {
		t.Error("failed to expire item")
	}
}

func TestCacheOversize(t *testing.T) {
	c := newCache(Options{MaxSize: 8, SegmentSize: 2})

	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to write entry data")
	}

	if _, ok, evicted, sizeChange := c.set("s1", "foo", 6, time.Hour); ok {
		t.Error("unxpected successful set of oversized entry")
	} else if len(evicted) != 0 || sizeChange.zero() {
		t.Error("unexpected set success", len(evicted), sizeChange)
	}

	s := c.getStatus()
	if len(s.Keyspaces) != 0 || s.Len != 0 {
		t.Error("old item was not deleted")
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
		},
		40,
		8,
		nil,
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte{3, 4, 5},
			sizeChange: Size{Len: 1, Segments: 1, Effective: 7},
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
		},
		32,
		8,
		map[string]string{"s2": "qux"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte{3, 4, 5},
			sizeChange: Size{Effective: 1},
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
				"foo": {1, 2, 3},
				"bar": {4, 5, 6},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
			"s3": map[string][]byte{
				"foo": {3, 4, 5},
				"qux": {6, 7, 8},
			},
		},
		48,
		8,
		map[string]string{"s1": "bar", "s3": "qux"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte("123456789012345678901234567"),
			sizeChange: Size{Len: -3, Segments: 0, Effective: 7},
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
			data:  []byte("123456789012345678901234567"),
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
				"foo": {1, 2, 3},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
			"s3": map[string][]byte{
				"foo": {3, 4, 5},
				"bar": {6, 7, 8},
				"baz": {9, 0, 1},
			},
		},
		48,
		8,
		map[string]string{"s3": "baz"},
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte("12345678901234567890123456789012"),
			sizeChange: Size{Len: -4, Segments: 0, Effective: 6},
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
			data:  []byte("12345678901234567890123456789012"),
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
				"foo": {1, 2, 3},
			},
			"s2": map[string][]byte{
				"baz": {7, 8, 9},
				"qux": {0, 1, 2},
			},
			"s3": map[string][]byte{
				"foo": {3, 4, 5},
				"bar": {6, 7, 8},
				"baz": {9, 0, 1},
			},
		},
		48,
		8,
		nil,
		testDataItem{
			space:      "s2",
			key:        "quux",
			data:       []byte("123456789012345678901234567890123456789012"),
			sizeChange: Size{Len: -5, Segments: 0, Effective: 10},
			evicted: map[string]int{
				"s1": 6,
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
			data:  []byte("123456789012345678901234567890123456789012"),
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

			if e, ok, evicted, sizeChange := c.set(ti.space, ti.key, len(ti.data), time.Hour); !ok {
				t.Error("failed to set entry")
			} else if !writeNFull(c, e, len(ti.key), ti.data) {
				t.Error("failed to write entry data")
			} else if !compareEvicted(evicted, ti.evicted) {
				t.Error("unexpected set success", len(evicted), sizeChange)
			}

			checkData(t, ti.checks, c)
		})

	}
}

func TestCacheKeyspaceStatus(t *testing.T) {
	o := testOptions
	o.SegmentSize = 6
	c := newCache(o)

	s := c.getKeyspaceStatus("s1")
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s1", "bar", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{3, 4, 5}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s2", "baz", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{7, 8, 9}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s2", "qux", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{0, 1, 2}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s2", "quux", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 4, []byte{0, 1, 2}) {
		t.Error("failed to write entry data")
	}

	s = c.getKeyspaceStatus("s1")
	if s.Len != 2 || s.Segments != 2 || s.Effective != 12 {
		t.Error("unexpected status")
		return
	}
	s = c.getKeyspaceStatus("s2")
	if s.Len != 3 || s.Segments != 4 || s.Effective != 19 {
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
	o := testOptions
	o.SegmentSize = 6
	c := newCache(o)

	s := c.getStatus()
	if s.Len != 0 || s.Segments != 0 || s.Effective != 0 {
		t.Error("unexpected status")
		return
	}

	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s1", "bar", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{3, 4, 5}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s2", "baz", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{7, 8, 9}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s2", "qux", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{0, 1, 2}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s2", "quux", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 4, []byte{0, 1, 2}) {
		t.Error("failed to write entry data")
	}

	s = c.getStatus()
	if s.Len != 5 || s.Segments != 6 || s.Effective != 31 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s1"].Len != 2 || s.Keyspaces["s1"].Segments != 2 || s.Keyspaces["s1"].Effective != 12 {
		t.Error("unexpected status")
		return
	}

	if s.Keyspaces["s2"].Len != 3 || s.Keyspaces["s2"].Segments != 4 || s.Keyspaces["s2"].Effective != 19 {
		t.Error("unexpected status")
		return
	}
}

func TestCopy(t *testing.T) {
	c := newCache(testOptions)
	b := []byte{1, 2, 3}
	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, b) {
		t.Error("failed to write entry data")
	}

	b[0] = 4
	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get entry")
	} else if b, ok = readNToEnd(c, e, 3, 3); !ok {
		t.Error("failed to read data")
	}

	if b[0] != 1 {
		t.Error("failed to copy on set")
	}

	b[2] = 1
	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get entry")
	} else if b, ok = readNToEnd(c, e, 3, 3); !ok {
		t.Error("failed to read data")
	}
	if b[0] != 1 {
		t.Error("failed to copy on get")
	}
}

func TestHashCollision(t *testing.T) {
	o := testOptions
	o.Hash = weakHash{}
	c := newCache(o)

	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to set colliding entry")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to write colliding entry data")
	}

	if e, ok, _, _ := c.set("s1", "bar", 3, time.Hour); !ok {
		t.Error("failed to set colliding entry")
	} else if !writeNFull(c, e, 3, []byte{2, 3, 1}) {
		t.Error("failed to write colliding entry data")
	}

	if e, ok, _, _ := c.set("s1", "baz", 3, time.Hour); !ok {
		t.Error("failed to set colliding entry")
	} else if !writeNFull(c, e, 3, []byte{3, 1, 2}) {
		t.Error("failed to write colliding entry data")
	}

	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get colliding entry")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to find colliding key")
	}

	if e, ok, _ := c.get("s1", "bar"); !ok {
		t.Error("failed to get colliding entry")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{2, 3, 1}) {
		t.Error("failed to find colliding key")
	}

	if e, ok, _ := c.get("s1", "baz"); !ok {
		t.Error("failed to get colliding entry")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{3, 1, 2}) {
		t.Error("failed to find colliding key")
	}

	c.del("s1", "bar")

	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get colliding entry")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to find colliding key")
	}

	if _, ok, _ := c.get("s1", "bar"); ok {
		t.Error("failed to delete colliding entry")
	}

	if e, ok, _ := c.get("s1", "baz"); !ok {
		t.Error("failed to get colliding entry")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{3, 1, 2}) {
		t.Error("failed to find colliding key")
	}
}

func TestEmptyItem(t *testing.T) {
	c := newCache(testOptions)

	if _, ok, _, _ := c.set("", "", 0, time.Hour); !ok {
		t.Error("failed to create empty item")
	}

	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to create empty item")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to create non-empty item after empty item")
	}

	if e, ok, _ := c.get("", ""); !ok {
		t.Error("failed to get empty item")
	} else if _, ok := readNToEnd(c, e, 0, 0); !ok {
		t.Error("failed to read empty item")
	}

	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get non-empty item")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to read non-empty item")
	}

	c.del("", "")

	if _, ok, _ := c.get("", ""); ok {
		t.Error("failed to delete empty item")
	}

	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get non-empty item")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to read non-empty item after deleting empty")
	}
}

func TestOverwritingLastItem(t *testing.T) {
	c := newCache(testOptions)

	if e, ok, _, _ := c.set("s1", "foo", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{1, 2, 3}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s1", "bar", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{2, 3, 1}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _, _ := c.set("s1", "bar", 3, time.Hour); !ok {
		t.Error("failed to set entry")
	} else if !writeNFull(c, e, 3, []byte{3, 1, 2}) {
		t.Error("failed to write entry data")
	}

	if e, ok, _ := c.get("s1", "foo"); !ok {
		t.Error("failed to get item")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{1, 2, 3}) {
		t.Error("failed to read non-empty item after deleting empty")
	}

	if e, ok, _ := c.get("s1", "bar"); !ok {
		t.Error("failed to get item")
	} else if b, ok := readNToEnd(c, e, 3, 3); !ok || !bytes.Equal(b, []byte{3, 1, 2}) {
		t.Error("failed to read item after deleting empty")
	}
}

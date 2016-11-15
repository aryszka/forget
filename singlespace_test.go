package forget

import (
	"bytes"
	"testing"
	"time"
)

type testDataItem struct {
	space string
	key   string
	ok    bool
	data  []byte
}

func testCache(d map[string][]byte) *SingleSpace {
	c := NewSingleSpace(1 << 9)
	for k, dk := range d {
		c.Set(k, dk, time.Hour)
	}

	return c
}

func TestGet(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init map[string][]byte
		testDataItem
	}{{
		"empty cache",
		nil,
		testDataItem{
			key: "foo",
		},
	}, {
		"not found key",
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		testDataItem{
			key: "baz",
		},
	}, {
		"key found",
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		testDataItem{
			key:  "bar",
			ok:   true,
			data: []byte{4, 5, 6},
		},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := testCache(ti.init)
			defer c.Close()

			d, ok := c.Get(ti.key)

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

func TestSet(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init map[string][]byte
		testDataItem
		checks []testDataItem
	}{{
		"set in empty",
		nil,
		testDataItem{
			key:  "foo",
			data: []byte{1, 2, 3},
		},
		[]testDataItem{{
			key:  "foo",
			ok:   true,
			data: []byte{1, 2, 3},
		}, {
			key: "bar",
		}},
	}, {
		"set in addition",
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		testDataItem{
			key:  "baz",
			data: []byte{7, 8, 9},
		},
		[]testDataItem{{
			"",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"",
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
			"",
			"baz",
			true,
			[]byte{7, 8, 9},
		}},
	}, {
		"overwrite",
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		testDataItem{
			key:  "bar",
			data: []byte{7, 8, 9},
		},
		[]testDataItem{{
			"",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"",
			"bar",
			true,
			[]byte{7, 8, 9},
		}, {
			"",
			"baz",
			false,
			nil,
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := testCache(ti.init)
			defer c.Close()

			c.Set(ti.key, ti.data, time.Hour)
			for _, chk := range ti.checks {
				if d, ok := c.Get(chk.key); ok != chk.ok {
					t.Error("unexpected response status", ok, chk.ok)
					return
				} else if !bytes.Equal(d, chk.data) {
					t.Error("invalid response data", d, chk.data)
				}
			}
		})
	}
}

func TestOverSize(t *testing.T) {
	c := NewSingleSpace(4)
	defer c.Close()
	c.Set("foo", []byte{1, 2, 3}, time.Hour)
	if _, ok := c.Get("foo"); ok {
		t.Error("failed to reject oversized data")
	}
}

func TestDelete(t *testing.T) {
	for _, ti := range []struct {
		msg  string
		init map[string][]byte
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
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		testDataItem{
			key: "baz",
		},
		[]testDataItem{{
			"",
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"",
			"bar",
			true,
			[]byte{4, 5, 6},
		}},
	}, {
		"delete",
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		testDataItem{
			key: "foo",
		},
		[]testDataItem{{
			"",
			"bar",
			true,
			[]byte{4, 5, 6},
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := testCache(ti.init)
			defer c.Close()

			c.Del(ti.key)
			for _, chk := range ti.checks {
				if d, ok := c.Get(chk.key); ok != chk.ok {
					t.Error("unexpected response status", ok, chk.ok)
					return
				} else if !bytes.Equal(d, chk.data) {
					t.Error("invalid response data", d, chk.data)
				}
			}
		})

	}
}

func TestSizeLen(t *testing.T) {
	for _, ti := range []struct {
		msg                   string
		init                  map[string][]byte
		expectSize, expectLen int
	}{{
		"empty",
		nil,
		0,
		0,
	}, {
		"not empty",
		map[string][]byte{
			"foo": []byte{1, 2, 3},
			"bar": []byte{4, 5, 6},
		},
		12,
		2,
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := testCache(ti.init)
			defer c.Close()

			if s := c.Size(); s != ti.expectSize {
				t.Error("invalid size", s, ti.expectSize)
			}

			if s := c.Len(); s != ti.expectLen {
				t.Error("invalid len", s, ti.expectLen)
			}
		})
	}
}

func TestClose(t *testing.T) {
	c := testCache(map[string][]byte{
		"foo": []byte{1, 2, 3},
		"bar": []byte{4, 5, 6},
	})
	c.Close()

	if _, ok := c.Get("foo"); ok {
		t.Error("failed to close cache")
	}

	c.Set("baz", []byte{7, 8, 9}, time.Hour)
	c.Del("foo")
	c.Size()
}

func TestExpiration(t *testing.T) {
	c := NewSingleSpace(1 << 9)
	defer c.Close()

	c.Set("foo", []byte{1, 2, 3}, 24*time.Millisecond)

	time.Sleep(12 * time.Millisecond)
	c.Get("foo")
	time.Sleep(24 * time.Millisecond)
	if _, ok := c.Get("foo"); ok {
		t.Error("failed to expire item")
	}
}

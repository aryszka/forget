package forget

import (
	"bytes"
	"testing"
	"time"
)

type testDataItem struct {
	key  string
	ok   bool
	data []byte
}

func initForget(d map[string][]byte) *Forget {
	f := New(1 << 9)
	for k, dk := range d {
		f.Set(k, dk, time.Hour)
	}

	return f
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
			f := initForget(ti.init)
			defer f.Close()

			d, ok := f.Get(ti.key)

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
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"bar",
			true,
			[]byte{4, 5, 6},
		}, {
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
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
			"bar",
			true,
			[]byte{7, 8, 9},
		}, {
			"baz",
			false,
			nil,
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			f := initForget(ti.init)
			defer f.Close()

			f.Set(ti.key, ti.data, time.Hour)
			for _, c := range ti.checks {
				if d, ok := f.Get(c.key); ok != c.ok {
					t.Error("unexpected response status", ok, c.ok)
					return
				} else if !bytes.Equal(d, c.data) {
					t.Error("invalid response data", d, c.data)
				}
			}
		})
	}
}

func TestOverSize(t *testing.T) {
	f := New(4)
	defer f.Close()
	f.Set("foo", []byte{1, 2, 3}, time.Hour)
	if _, ok := f.Get("foo"); ok {
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
			"foo",
			true,
			[]byte{1, 2, 3},
		}, {
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
			"bar",
			true,
			[]byte{4, 5, 6},
		}},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			f := initForget(ti.init)
			defer f.Close()

			f.Del(ti.key)
			for _, c := range ti.checks {
				if d, ok := f.Get(c.key); ok != c.ok {
					t.Error("unexpected response status", ok, c.ok)
					return
				} else if !bytes.Equal(d, c.data) {
					t.Error("invalid response data", d, c.data)
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
			f := initForget(ti.init)
			defer f.Close()

			if s := f.Size(); s != ti.expectSize {
				t.Error("invalid size", s, ti.expectSize)
			}

			if s := f.Len(); s != ti.expectLen {
				t.Error("invalid len", s, ti.expectLen)
			}
		})
	}
}

func TestClose(t *testing.T) {
	f := initForget(map[string][]byte{
		"foo": []byte{1, 2, 3},
		"bar": []byte{4, 5, 6},
	})
	f.Close()

	if _, ok := f.Get("foo"); ok {
		t.Error("failed to close cache")
	}

	f.Set("baz", []byte{7, 8, 9}, time.Hour)
	f.Del("foo")
	f.Size()
	f.Close()
}

func TestExpiration(t *testing.T) {
	f := New(1 << 9)
	defer f.Close()

	f.Set("foo", []byte{1, 2, 3}, 24*time.Millisecond)

	time.Sleep(12 * time.Millisecond)
	f.Get("foo")
	time.Sleep(24 * time.Millisecond)
	if _, ok := f.Get("foo"); ok {
		t.Error("failed to expire item")
	}
}

package forget

import "testing"

func newTestCache(init map[string]interface{}) *Cache {
	c := New()
	for k, v := range init {
		c.Set(k, v)
	}

	return c
}

func checkCache(c *Cache, check map[string]interface{}) bool {
	for k, vk := range check {
		if v, ok := c.Get(k); !ok || v != vk {
			return false
		}
	}

	return true
}

func TestGet(t *testing.T) {
	for _, ti := range []struct {
		msg   string
		init  map[string]interface{}
		key   string
		value interface{}
		ok    bool
	}{{
		"empty",
		nil,
		"foo",
		nil,
		false,
	}, {
		"not found",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
		"qux",
		nil,
		false,
	}, {
		"found",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
		"bar",
		2,
		true,
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := newTestCache(ti.init)
			defer c.Close()

			v, ok := c.Get(ti.key)
			if ok != ti.ok {
				t.Error("invalid result")
				return
			}

			if ok && v != ti.value {
				t.Error("invalid result value")
				return
			}
		})
	}
}

func TestSet(t *testing.T) {
	for _, ti := range []struct {
		msg   string
		init  map[string]interface{}
		key   string
		value interface{}
		check map[string]interface{}
	}{{
		"empty",
		nil,
		"foo",
		1,
		map[string]interface{}{
			"foo": 1,
		},
	}, {
		"new",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
		"qux",
		4,
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
			"qux": 4,
		},
	}, {
		"overwrite",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
		"bar",
		4,
		map[string]interface{}{
			"foo": 1,
			"bar": 4,
			"baz": 3,
		},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := newTestCache(ti.init)
			defer c.Close()

			c.Set(ti.key, ti.value)
			if !checkCache(c, ti.check) {
				t.Error("failed to set the key")
			}
		})
	}
}

func TestDel(t *testing.T) {
	for _, ti := range []struct {
		msg   string
		init  map[string]interface{}
		key   string
		check map[string]interface{}
	}{{
		"empty",
		nil,
		"foo",
		nil,
	}, {
		"not found",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
		"qux",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
	}, {
		"found",
		map[string]interface{}{
			"foo": 1,
			"bar": 2,
			"baz": 3,
		},
		"bar",
		map[string]interface{}{
			"foo": 1,
			"baz": 3,
		},
	}} {
		t.Run(ti.msg, func(t *testing.T) {
			c := newTestCache(ti.init)
			defer c.Close()

			c.Del(ti.key)
			if !checkCache(c, ti.check) {
				t.Error("failed to delete the key")
			}
		})
	}
}

func TestClose(t *testing.T) {
	c := New()
	c.Set("foo", 42)

	c.Close()

	if _, ok := c.Get("foo"); ok {
		t.Error("failed to close cache")
		return
	}

	// crash test:
	func() {
		defer func() {
			if err := recover(); err != nil {
				t.Error(err)
			}
		}()

		c.Set("bar", 3.14)
		if _, ok := c.Get("bar"); ok {
			t.Error("failed to close cache")
			return
		}

		c.Del("foo")
		c.Close()
	}()
}

package forget

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestReadFromDiscardedItem(t *testing.T) {
	c := New(testOptions)
	defer c.Close()

	w := c.Set("s1", "foo", 3, time.Hour)
	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write", n, err)
	}

	r, ok := c.Get("s1", "foo")
	if !ok {
		t.Error("failed to get item")
	}

	c.Del("s1", "foo")

	if _, err := r.Read(make([]byte, 3)); err != ErrItemDiscarded {
		t.Error("failed report discarded item")
	}
}

func TestWriteToDiscardedItem(t *testing.T) {
	c := New(testOptions)
	defer c.Close()

	w := c.Set("s1", "foo", 3, time.Hour)

	c.Del("s1", "foo")

	if _, err := w.Write([]byte{1, 2, 3}); err != ErrItemDiscarded {
		t.Error("failed report discarded item")
	}
}

func TestWriteBeyondLimit(t *testing.T) {
	c := New(testOptions)
	defer c.Close()

	w := c.Set("s1", "foo", 3, time.Hour)
	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("failed to write", n, err)
	}

	if _, err := w.Write([]byte{1, 2, 3}); err != ErrWriteLimit {
		t.Error("failed to report write limit")
	}
}

func TestWriteOverSize(t *testing.T) {
	c := New(testOptions)
	defer c.Close()

	w := c.Set("s1", "foo", 3, time.Hour)
	if _, err := w.Write([]byte{1, 2, 3, 4}); err != ErrWriteLimit {
		t.Error("failed to report write limit")
	}
}

func TestWaitForWrite(t *testing.T) {
	c := New(testOptions)
	defer c.Close()

	w := c.Set("s1", "foo", 3, time.Hour)

	var wg sync.WaitGroup
	read := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()

			r, ok := c.Get("s1", "foo")
			if !ok {
				t.Error("get failed")
				return
			}

			b := make([]byte, 3)
			if n, err := r.Read(b); n != 3 || err != nil || !bytes.Equal(b, []byte{1, 2, 3}) {
				t.Error("read failed", n, err, b)
			}
		}()
	}

	read()
	read()

	time.Sleep(12 * time.Millisecond)
	if n, err := w.Write([]byte{1, 2, 3}); n != 3 || err != nil {
		t.Error("write failed")
	}

	wg.Wait()
}

func TestDiscarded(t *testing.T) {
	p := make([]byte, 3)
	if _, err := discardedIO.Read(p); err != ErrItemDiscarded {
		t.Error("failed to read from discarded")
	} else if _, err = discardedIO.Write(p); err != ErrItemDiscarded {
		t.Error("failed to write to discarded")
	}
}

func TestIOAfterCacheClosed(t *testing.T) {
	t.Run("read after cache closed", func(t *testing.T) {
		c := New(Options{})
		c.SetBytes("s1", "foo", []byte{1, 2, 3}, time.Hour)

		r, ok := c.Get("s1", "foo")
		if !ok {
			t.Error("failed to retrieve item")
			return
		}

		c.Close()

		p := make([]byte, 3)
		if _, err := r.Read(p); err != ErrCacheClosed {
			t.Error("failed to report cache closed")
		}
	})

	t.Run("close while waiting for data", func(t *testing.T) {
		c := New(Options{})
		c.Set("s1", "foo", 3, time.Hour)

		r, ok := c.Get("s1", "foo")
		if !ok {
			t.Error("failed to retrieve item")
			return
		}

		done := make(chan struct{})
		go func() {
			p := make([]byte, 3)
			if _, err := r.Read(p); err != ErrCacheClosed {
				t.Error("failed to report cache closed")
			}

			close(done)
		}()

		time.Sleep(12 * time.Millisecond)
		c.Close()
		<-done
	})
}

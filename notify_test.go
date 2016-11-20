package forget

import (
	"testing"
	"time"
)

func TestNotifyHitMiss(t *testing.T) {
	nc := make(chan *Notification, 1)
	c := New(Options{Notify: nc, NotificationLevel: Moderate})
	defer c.Close()

	t.Run("no hit at moderate", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)

		c.Get("s1", "foo")
		select {
		case <-nc:
			t.Error("unexpected notification")
		default:
		}
	})

	t.Run("notify miss at moderate", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, 0)

		time.Sleep(12 * time.Millisecond)
		c.Get("s1", "foo")
		select {
		case n := <-nc:
			if n.Type != Miss || n.Keyspace != "s1" || n.Key != "foo" || n.SizeChange.Len != -1 || n.Status == nil {
				t.Error("invalid notification")
			}
		default:
			t.Error("missing notification")
		}
	})

	c = New(Options{Notify: nc, NotificationLevel: Verbose})
	defer c.Close()

	t.Run("notify hit at verbose", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		<-nc

		c.Get("s1", "foo")
		select {
		case n := <-nc:
			if n.Type != Hit || n.Keyspace != "s1" || n.Key != "foo" || n.SizeChange.Len != 0 || n.Status == nil {
				t.Error("invalid notification")
			}
		default:
			t.Error("missing notification")
		}
	})
}

func TestNotifySet(t *testing.T) {
	nc := make(chan *Notification, 1)
	c := New(Options{Notify: nc, NotificationLevel: Moderate})
	defer c.Close()

	t.Run("no set under verbose", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		select {
		case <-nc:
			t.Error("unexpected notification")
		default:
		}
	})

	c = New(Options{MaxSize: 9, Notify: nc, NotificationLevel: Moderate})
	defer c.Close()

	t.Run("notifiy if evicted", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.Set("s1", "bar", []byte{1, 2, 3, 4, 5, 6}, time.Hour)
		select {
		case n := <-nc:
			if n.Type != Eviction || n.Keyspace != "s1" || n.Key != "bar" || n.SizeChange.Len != 0 || n.Status == nil {
				t.Error("invalid notification")
			}
		default:
			t.Error("missing notification")
		}
	})

	c = New(Options{Notify: nc, NotificationLevel: Verbose})
	defer c.Close()

	t.Run("do not notify when no size change and no eviction", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		<-nc
		c.Set("s1", "foo", []byte{4, 5, 6}, time.Hour)

		select {
		case <-nc:
			t.Error("unexpected notification")
		default:
		}
	})

	t.Run("notify at verbose", func(t *testing.T) {
		c.Set("s1", "bar", []byte{1, 2, 3}, time.Hour)
		select {
		case n := <-nc:
			if n.Type != SizeChange || n.Keyspace != "s1" || n.Key != "bar" || n.SizeChange.Len != 1 || n.Status == nil {
				t.Error("invalid notification")
			}
		default:
			t.Error("missing notification")
		}
	})
}

func TestNotifyDelete(t *testing.T) {
	nc := make(chan *Notification, 1)
	c := New(Options{Notify: nc, NotificationLevel: Moderate})
	defer c.Close()

	t.Run("do not notify under verbose", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		c.Del("s1", "foo")
		select {
		case <-nc:
			t.Error("unexpected notification")
		default:
		}
	})

	c = New(Options{Notify: nc, NotificationLevel: Verbose})
	defer c.Close()

	t.Run("do not notify when no size change", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		<-nc

		c.Del("s1", "bar")
		select {
		case <-nc:
			t.Error("unexpected notification")
		default:
		}
	})

	t.Run("notify on size change", func(t *testing.T) {
		c.Set("s1", "foo", []byte{1, 2, 3, 4, 5, 6}, time.Hour)
		<-nc

		c.Del("s1", "foo")
		select {
		case n := <-nc:
			if n.Type != SizeChange || n.Keyspace != "s1" || n.Key != "foo" || n.SizeChange.Len != -1 || n.Status == nil {
				t.Error("invalid notification")
			}
		default:
			t.Error("missing notification")
		}
	})
}

func TestNoNotificationAfterClose(t *testing.T) {
	nc := make(chan *Notification)
	c := New(Options{Notify: nc, NotificationLevel: Verbose})

	done := make(chan struct{})
	go func() {
		c.Set("s1", "foo", []byte{1, 2, 3}, time.Hour)
		close(done)
	}()

	time.Sleep(12 * time.Millisecond)

	c.Close()
	select {
	case <-done:
	case <-time.After(120 * time.Millisecond):
		t.Error("failed to exit notification call")
	}
}

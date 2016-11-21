package forget_test

import (
	"fmt"
	"time"

	"github.com/aryszka/forget"
)

func Example() {
	c := forget.New(forget.Options{MaxSize: 1 << 9, SegmentSize: 1 << 6})
	defer c.Close()

	c.Set("pages", "/home", []byte("Hello, world!"), time.Minute)
	c.Set("pages", "/article-one", []byte("This is cached."), time.Minute)
	c.Set("ajax-data", "/api/site-index", []byte(`{"data": 42}`), 12*time.Minute)

	if d, ok := c.Get("pages", "/article-one"); ok {
		fmt.Println(string(d))
	} else {
		fmt.Println("article not found in cache")
	}

	// Output:
	// This is cached.
}

func ExampleNotification() {
	quit := make(chan struct{})
	nc := make(chan *forget.Notification, 8)
	go func() {
		for {
			select {
			case n := <-nc:
				switch n.Type {
				case forget.Hit:
					fmt.Printf("cache hit: %s:%s\n", n.Keyspace, n.Key)
				case forget.Miss:
					fmt.Printf("cache miss: %s:%s\n", n.Keyspace, n.Key)
				}
			case <-quit:
				return
			}
		}
	}()

	c := forget.New(forget.Options{
		MaxSize:           1 << 9,
		SegmentSize:       1 << 6,
		Notify:            nc,
		NotificationLevel: forget.Verbose,
	})
	defer c.Close()

	c.Set("pages", "/home", []byte{1, 2, 3}, time.Minute)
	c.Set("pages", "/article-one", []byte{2, 3, 1}, time.Minute)
	c.Set("ajax-data", "/api/site-index", []byte{3, 4, 5, 6, 1, 2}, 12*time.Minute)

	if _, ok := c.Get("pages", "/article-one"); ok {
		fmt.Println("article found in cache")
	} else {
		fmt.Println("article not found in cache")
	}

	close(quit)
}

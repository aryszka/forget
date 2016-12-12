package forget_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/aryszka/forget"
)

func Example() {
	// intialize a cache with 1MB cache size and 1KB chunk size
	c := forget.New(forget.Options{CacheSize: 1 << 20, ChunkSize: 1 << 10})
	defer c.Close()

	// store a cache item
	if !c.SetBytes("foo", []byte("bar"), time.Minute) {
		log.Println("failed to set cache item")
		return
	}

	// retrieve a cache item
	b, ok := c.GetBytes("foo")
	if !ok {
		log.Println("failed to get cache item")
		return
	}

	fmt.Printf("Item from the cache: %s.\n", string(b))

	// Output:
	// Item from the cache: bar.
}

func Example_keyspaces() {
	c := forget.NewCacheSpaces(forget.Options{CacheSize: 1 << 9, ChunkSize: 1 << 6})
	defer c.Close()

	// store items with different keyspaces
	c.SetBytes("pages", "/home", []byte("Hello, world!"), time.Minute)
	c.SetBytes("pages", "/article-one", []byte("This is cached."), time.Minute)
	c.SetBytes("ajax-data", "/article-one", []byte(`{"data": 42}`), 12*time.Minute)

	// retrieve an item
	if d, ok := c.GetBytes("pages", "/article-one"); ok {
		fmt.Println(string(d))
	} else {
		fmt.Println("article not found in cache")
	}

	// Output:
	// This is cached.
}

func Example_io() {
	c := forget.New(forget.Options{CacheSize: 1 << 20, ChunkSize: 1 << 10})
	defer c.Close()

	// set an item and receive a writer
	w, ok := c.Set("foo", time.Minute)
	if !ok {
		log.Println("failed to set cache item")
		return
	}

	// get three readers before the item data has been written
	var r []io.Reader
	for i := 0; i < 3; i++ {
		ri, ok := c.Get("foo")
		if !ok {
			log.Println("failed to get cache item")
			return
		}

		defer ri.Close()
		r = append(r, ri)
	}

	// start filling the cache item in the background
	go func() {
		if _, err := w.Write([]byte("foobarbaz")); err != nil {
			log.Println("failed to write item")
		}

		// closing the writer indicates that the item is filled
		w.Close()
	}()

	// read from the readers, not necessarily after the cache item was filled:
	for _, ri := range r {
		p, err := ioutil.ReadAll(ri)
		if err != nil {
			log.Println("failed to read item")
			return
		}

		fmt.Println(string(p))
	}

	// Output:
	// foobarbaz
	// foobarbaz
	// foobarbaz
}

func Example_proxyfill() {
	// The following example shows a backend server and a caching proxy in front of it. The backend produces
	// an expensive resource. The proxy caches it, it prevents multiple requests reaching the backend in
	// case of a cache miss, and serves any data to multiple clients in parallel as soon as it is available.
	// (See the order of the output.)

	// create a test backend server
	testContent := []byte{1, 2, 3}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// send slow content
		for _, b := range testContent {
			time.Sleep(12 * time.Millisecond)
			w.Write([]byte{b})
		}

		fmt.Println("backend done")
	}))
	defer backend.Close()

	// create a caching proxy
	c := forget.New(forget.Options{CacheSize: 1 << 20, ChunkSize: 1 << 10})
	cacheServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// check if it is a hit
		if r, ok := c.Get(r.URL.Path); ok {
			fmt.Println("hit")
			defer r.Close()

			// make a preread to know that the backend responded with success
			// during cache filling
			b := make([]byte, 1<<10)
			n, err := r.Read(b)
			if err != nil && err != io.EOF {
				fmt.Println("cache fill failed", err)
				w.WriteHeader(http.StatusNotFound)
				return
			}

			w.Write(b[:n])

			// copy the rest of the cached content to the response
			io.Copy(w, r)
			return
		}

		// if it is a miss, optimistically create a cache item
		fmt.Println("miss")
		cacheItem, itemCreated := c.Set(r.URL.Path, time.Minute)
		if itemCreated {
			defer cacheItem.Close()
		}

		// initiate the streaming of the actual content
		rsp, err := http.Get(backend.URL + r.URL.Path)
		if err != nil {
			// if the request fails, we can discard the invalid cache item
			c.Delete(r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer rsp.Body.Close()

		// initiate the outgoing response
		w.WriteHeader(rsp.StatusCode)
		if !itemCreated {
			io.Copy(w, rsp.Body)
			return
		}

		// for this example, cache only the responses with status 200
		var body io.Reader = rsp.Body
		if rsp.StatusCode == http.StatusOK {
			body = io.TeeReader(body, cacheItem)
		} else {
			c.Delete(r.URL.Path)
		}

		// send the response to the client and, on success, to the cache through the tee reader.
		// if it fails, delete the invalid cache item
		if _, err := io.Copy(w, body); err != nil {
			c.Delete(r.URL.Path)
		}
	}))
	defer c.Close()
	defer cacheServer.Close()

	// make multiple requests faster than how the backend can respond
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(delay int) {
			time.Sleep(time.Duration(3*delay) * time.Millisecond)
			rsp, err := http.Get(cacheServer.URL + "/test-item")
			if err != nil {
				fmt.Println("request error", err)
				wg.Done()
				return
			}
			defer rsp.Body.Close()

			if content, err := ioutil.ReadAll(rsp.Body); err != nil || !bytes.Equal(content, testContent) {
				fmt.Println("error reading response content", err)
			}

			fmt.Println("request done")
			wg.Done()
		}(i)
	}

	wg.Wait()

	// Output:
	// miss
	// hit
	// hit
	// backend done
	// request done
	// request done
	// request done
}

package forget

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

func Example_fill() {
	// create a test backend server
	testContent := []byte{1, 2, 3}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// send slow content
		for _, b := range testContent {
			time.Sleep(9 * time.Millisecond)
			w.Write([]byte{b})
		}

		fmt.Println("backend done")
	}))
	defer backend.Close()

	// create a caching proxy
	c := New(Options{CacheSize: 1 << 20, SegmentSize: 10})
	cacheServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// check if it is a hit
		if r, ok := c.Get("default", r.URL.Path); ok {
			fmt.Println("hit")
			defer r.Close()
			io.Copy(w, r)
			return
		}

		// if it is a miss, optimistically set the cache item
		fmt.Println("miss")
		cacheItem, itemCreated := c.Set("default", r.URL.Path, time.Minute)
		if itemCreated {
			defer cacheItem.Close()
		}

		// request the actual content
		rsp, err := http.Get(backend.URL + r.URL.Path)
		if err != nil {
			// if the request fails, we can discard the invalid cache item
			c.Del("default", r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer rsp.Body.Close()

		w.WriteHeader(rsp.StatusCode)
		if !itemCreated {
			io.Copy(w, rsp.Body)
			return
		}

		// caching only responses with status 200
		var body io.Reader = rsp.Body
		if rsp.StatusCode == http.StatusOK {
			body = io.TeeReader(body, cacheItem)
		} else {
			c.Del("default", r.URL.Path)
		}

		// send the response to the client and into the cache.
		// if it fails, delete the invalid cache item
		if _, err := io.Copy(w, body); err != nil {
			c.Del("default", r.URL.Path)
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

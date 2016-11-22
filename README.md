[![GoDoc](https://godoc.org/github.com/aryszka/forget?status.svg)](https://godoc.org/github.com/aryszka/forget)
[![Go Report Card](https://goreportcard.com/badge/github.com/aryszka/forget)](https://goreportcard.com/report/github.com/aryszka/forget)
[![Coverage](http://gocover.io/_badge/github.com/aryszka/forget)](http://gocover.io/github.com/aryszka/forget)

# Forget

Forget is a Go library providing in-memory caching. It can be used as a safe, in-process cache for storing binary
data with keys and keyspaces.

It:

- uses a hard memory limit for the combined byte size of the cached items;
- preallocates the maximum required memory in advance, no further, large allocations required;
- uses keys and keyspaces to identify cached items, so that a key can appear in multiple keyspaces with
  different cached data; 
- supports TTL based expiration, where every item can have a different TTL;
- evicts the least recently used item from the cache when there is no more space for new items (LRU);
- evicts first the items in the keyspace of the new item, fitting this way less frequently accessed but more
  expensive recources next to frequently accessed but cheaper ones, staying within a shared memory limit;
- provides continuous usage statistics for monitoring health and performance;
- supports to run any number of instances in a process with different configuration, if needed;

### Documentation:

More details about the usage and the package description can be found here:

[https://godoc.org/github.com/aryszka/forget](https://godoc.org/github.com/aryszka/forget)

### Example:

```
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
```

### Installation:

```
go get github.com/aryszka/forget
```

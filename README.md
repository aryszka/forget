[![GoDoc](https://godoc.org/github.com/aryszka/forget?status.svg)](https://godoc.org/github.com/aryszka/forget)
[![Go Report Card](https://goreportcard.com/badge/github.com/aryszka/forget)](https://goreportcard.com/report/github.com/aryszka/forget)
[![Go Cover](https://gocover.io/_badge/github.com/aryszka/forget)](https://gocover.io/github.com/aryszka/forget)

# Forget

Forget is a Go library for in-memory caching. It can be used as a safe, in-process cache for storing binary
data:

- supports use cases with different characteristics: key store, cache for small and large binary data;
- has a simple get/set/delete interface;
- supports streaming style read/write IO, with seeking and immediate read access to items being filled;
- implements TTL based expiration;
- implements LRU style eviction optimized with keyspaces;
- allocates the predefined maximum used memory in advance, on startup;
- protects busy items from delete, reset and eviction;
- all its operations are ready for concurrent access, and, as much as possible, accessible in parallel;
- supports monitoring with detailed statistics and continuous notifications;

### Documentation:

The detailed description of the library and its usage can be found in Godoc:

[https://godoc.org/github.com/aryszka/forget](https://godoc.org/github.com/aryszka/forget)

### Example:

```
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
```

### Installation:

```
go get github.com/aryszka/forget
```

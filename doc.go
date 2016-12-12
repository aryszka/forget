/*
Package forget provides an in-memory cache for arbitrary, binary data.

Caching

The cache identifies cached items by their key. Items to be stored are provided with their key, individually
selected expiration time (TTL), and binary data. Storing an item with the same key, overrides the TTL and the
previous one. A cached item can be retrieved or deleted with its key. As a special use case, it is possible to
store only keys, where the useful information is whether a key exists, and whether it has expired. If a new item
doesn't fit in the free space, the least recently used item is evicted (LRU).

Keyspaces

Keyspaces, when used (see NewCacheSpaces()), allow the optimization for the LRU eviction mechanism. Items that
are more expensive to produce but less frequently accessed than others, can be stored in different keyspaces.
When eviction is required, the cache tries to evict enough items from the same keyspace as the one currently
being filled. When using keyspaces, the same key can appear in different keyspaces pointing to different items.

Memory

The cache allocates all the used memory on start. To support parallel access, it splits the allocated memory
into segments. There are typically as many segments as the maximum of NumCPU() and GOMAXPROCS(). The maximum
size of a stored item is the total cache size divided by the number of segments.

The segments are split into chunks. One cached item can span over multiple chunks, but the chunks cannot be
shared between the items. This means that the cache is almost never fully utilized. The chunk size is an
initialization option, it typically should be a 'couple of times' smaller than the expected size of the 'bulk'
of the cached items.

The cache counts the size of the item keys in the used space, but there is some lookup metadata that is not
counted: ~24 bytes for each chunk and ~120 bytes for each item.

IO

The associated data of the keys can be accessed for read, seek and write through the standard Go interfaces. As
a shortcut, the data can be retrieved or set as a single byte slice, too. When using the IO interfaces, the item
data can be accessed concurrently, and reading from an item can be started before the write has finished. When
the reader reaches a point that was not yet filled by the writer, it blocks, and continues only when more data
was written, or returns with EOF once the write was finished.

While writing an item, chunks are continuously assigned to the item from the free range of allocated memory. If
there are no free chunks, the cache evicts enough of the least recently used items. The cache doesn't evict
those items that are currently being read by an unclosed reader. Similarly, when deleting an item or overwriting
one, while it has active readers associated with it, the item is only marked for delete, but the active readers
can finish reading from it.

Monitoring

The cache provides statistics about its internal state, including metrics like item count, effective and used
size, active readers and writers. When configured, it also provides change notifications. Depending on the
configured notification mask, it can send events about: cache hit/miss, evictions, allocation failures, etc.
*/
package forget

/*
Package forget provides an in-memory cache for arbitrary, binary data.

Caching

Forget identifies items by a keyspace and key (or just a key, in case of the single space mode). Items to be
stored in the cache are provided with a keyspace, key, the cached data in byte slice format, and a TTL duration
that is added to the current time to define the items expiration. Storing an item with the same keyspace and key
overrides the previous one. A cached item can be retrieved by its keyspace and key. If needed, it can be
deleted. Each cache segment has a maximum size, defined in the number of used bytes. If a new item doesn't fit
in the available space, the required number of the least recently used items are removed from the cache
(eviction).

Keyspaces

Keyspaces allow multiple items to be stored with the same key, but with different data and TTL. Besides this
rather frivolous sounding comfort, keyspaces have a more useful advantage: when allocating space for new items
by evicting enough of the least recently used ones, Forget evicts first those in the same keyspace as of the
newly cached one. This way, it is possible to prevent, to some extent, that cheaper but more frequently used
items purge out those that are less frequently used, but are more expensive to produce. Instead of using the
item size or the expiration for prioritization, Forget lets the calling code to categorize the cached items
based on the expected behavior with keyspaces. If this feature is not required, it is possible to initialize
Forget in single space mode (NewSingleSpace()).

Memory

Forget allocates all the memory on start (when calling New()) defined in the MaxSize option. This is to make the
most possibly sure that the required memory is available. It can store items of arbitrary size, but only that
are smaller than the total allocated memory. To avoid the need for layouting the active items, Forget splits the
total allocated memory into chunks. One chunk can contain data only from one item. One item can span over 0
or any number of chunks (0 when both the key and the content have 0 length). The chunk size is defined in
the accordingly named option.

The chunkation means that Forget can only store maximum MaxSize / ChunkSize number of items. It also means
that, since the items of arbitrary size, the total allocated memory will probably be never fully utilized. The
utilization can be designed for the expected usage pattern and controlled by selecting the best fitting chunk
size. Typically, it is better to choose a chunk size a 'couple of times' smaller than the expected size of the
'bulk' of the items, but choosing 'too small' chunk size can negatively impact the retrieve time and the
required metadata can take too much space besides what's defined in MaxSize. The default settings chunk size
is selected so that it anticipates the caching of 'modern' web pages and aims for a ~90% utilization. (To be
tested more extensively yet.)

In the maximum used size, also the size of the item keys count. (It is possible to use Forget for scenarios
where only the keys are stored without payload, where the practical stored information is whether a key exists
or not.) On the other hand, Forget cheats to some extent with the maximum size, because the required metadata is
not counted. This includes: the size of the keyspace names, additional ~24 bytes for each chunk, additional
~120 bytes of lookup metadata for each stored item.

IO

Forget allows concurrent read and write of a cached item (in fact, only a single write). If a new item is set,
reading from it can start immediately, even if the data to be cached was not yet fully written. When the read
reaches a point that the write hasn't reached yet, the read blocks and only continues once there is more data
available.

Statistics and Notifications

Forget provides notifications about its internal behavior for logging and monitoring purposes. The notifications
can be received through a channel provided in the options. It is recommended to use a buffered channel to avoid
unnecessary blocking, with a small buffer size (e.g. 2). The values in the notifications are snapshots, which
means that they were true in combination in a recent point in time. Besides the notifications, there are also
direct function calls to get information about the total current size, or individually about a keyspace.
*/
package forget

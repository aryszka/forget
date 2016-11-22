package forget

// Cache provides an in-memory cache for arbitrary binary data
// identified by keyspace and key. All methods of Cache are thread safe.
type Cache map[string]interface{}

// concurrency

// once possible, make an http comparison

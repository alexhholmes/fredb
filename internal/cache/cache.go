package cache

import (
	"encoding/binary"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/elastic/go-freelru"

	"github.com/alexhholmes/fredb/internal/base"
)

// Cache implements a simple LRU cache without awareness of version tracking or
// disk I/O.
type Cache struct {
	lru freelru.ShardedLRU[base.PageID, base.PageData] // LRU list of pages
}

const (
	MinCacheSize = 16 // Minimum: hold tree path + concurrent ops
)

// NewCache creates a new Page cache with the specified maximum size
func NewCache(size int, _ func(base.PageID, base.PageData)) *Cache {
	hash := func(s base.PageID) uint32 {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(s))
		return uint32(xxhash.Sum64(b[:]))
	}

	size = max(size, MinCacheSize)
	lru, err := freelru.NewSharded[base.PageID, base.PageData](uint32(size), hash)
	if err != nil {
		panic(err)
	}

	c := &Cache{
		lru: *lru,
	}

	return c
}

// Put adds a page to the cache, replacing any existing entry for the id.
func (c *Cache) Put(pageID base.PageID, page base.PageData) {
	c.lru.AddWithLifetime(pageID, page, 10000*time.Millisecond)
}

// Get retrieves a page from the cache.
// Returns (PageData, true) on cache hit, (nil, false) on miss.
func (c *Cache) Get(pageID base.PageID) (base.PageData, bool) {
	if val, ok := c.lru.GetAndRefresh(pageID, 20000*time.Millisecond); ok {
		return val, true
	}
	return nil, false
}

// Remove invalidates a cache entry for the given page ID.
// Called when a page is freed and may be reused with different data.
func (c *Cache) Remove(pageID base.PageID) {
	c.lru.Remove(pageID)
}

// Size returns current number of cached entries
func (c *Cache) Size() int {
	return c.lru.Len()
}

type Stats freelru.Metrics

// Stats returns cache statistics
func (c *Cache) Stats() Stats {
	return Stats(c.lru.Metrics())
}

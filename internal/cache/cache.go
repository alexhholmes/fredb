package cache

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/elastic/go-freelru"

	"fredb/internal/base"
)

// Cache implements a simple LRU cache without awareness of version tracking or
// disk I/O.
type Cache struct {
	lru freelru.ShardedLRU[base.PageID, *base.Node] // LRU list of *entry

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

const (
	MinCacheSize = 16 // Minimum: hold tree path + concurrent ops
)

// NewCache creates a new Page cache with the specified maximum size
func NewCache(size int, onEvict func(base.PageID, *base.Node)) *Cache {
	hash := func(s base.PageID) uint32 {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(s))
		return uint32(xxhash.Sum64(b[:]))
	}

	size = max(size, MinCacheSize)
	lru, err := freelru.NewSharded[base.PageID, *base.Node](uint32(size), hash)
	if err != nil {
		panic(err)
	}

	if onEvict != nil {
		lru.SetOnEvict(onEvict)
	}

	return &Cache{
		lru: *lru,
	}
}

// Put adds a node to the cache, replacing any existing entry for the id.
func (c *Cache) Put(pageID base.PageID, node *base.Node) {
	evicted := c.lru.Add(pageID, node)
	if evicted {
		c.evictions.Add(1)
	}
}

// Get retrieves a node from the cache.
// Returns (Node, true) on cache hit, (nil, false) on miss.
func (c *Cache) Get(pageID base.PageID) (*base.Node, bool) {
	if val, ok := c.lru.Get(pageID); ok {
		c.hits.Add(1)
		return val, true
	}
	c.misses.Add(1)
	return nil, false
}

// Size returns current number of cached entries
func (c *Cache) Size() int {
	return c.lru.Len()
}

type Stats struct {
	Hits      uint64
	Misses    uint64
	Evictions uint64
}

// Stats returns cache statistics
func (c *Cache) Stats() Stats {
	return Stats{
		Hits:      c.hits.Load(),
		Misses:    c.misses.Load(),
		Evictions: c.evictions.Load(),
	}
}

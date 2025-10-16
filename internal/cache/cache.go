package cache

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/elastic/go-freelru"

	"fredb/internal/base"
)

type reclamation struct {
	mu    sync.Mutex
	nodes []*base.Node
}

// Cache implements a simple LRU cache without awareness of version tracking or
// disk I/O.
type Cache struct {
	lru     freelru.ShardedLRU[base.PageID, *base.Node] // LRU list of *entry
	reclaim sync.Map                                    // txID -> *reclamation

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

const (
	MinCacheSize = 16 // Minimum: hold tree path + concurrent ops
)

// NewCache creates a new Page cache with the specified maximum size
func NewCache(size int) *Cache {
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

	c := &Cache{
		lru: *lru,
	}

	// On eviction, add node to reclaim map keyed by TxID
	// Nodes will be returned to pool when all transactions < TxID have completed
	lru.SetOnEvict(func(key base.PageID, node *base.Node) {
		// Get or create reclamation list for this TxID
		val, _ := c.reclaim.LoadOrStore(node.TxID, &reclamation{
			nodes: make([]*base.Node, 0, 1),
		})
		r := val.(*reclamation)

		r.mu.Lock()
		r.nodes = append(r.nodes, node)
		r.mu.Unlock()

		c.evictions.Add(1)
	})

	return c
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

// Put adds a node to the cache, replacing any existing entry for the id.
func (c *Cache) Put(pageID base.PageID, node *base.Node) {
	c.lru.Add(pageID, node)
}

// Remove removes a node from the cache, triggering the eviction callback
// which returns the node to the pool
func (c *Cache) Remove(pageID base.PageID) {
	c.lru.Remove(pageID)
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

// ReclaimNodes returns nodes to pool for all transactions < minTxID
// Safe to call concurrently. Returns count of nodes reclaimed.
func (c *Cache) ReclaimNodes(minTxID uint64) int {
	reclaimed := 0

	// Scan reclaim map for TxIDs < minTxID
	c.reclaim.Range(func(key, value interface{}) bool {
		txID := key.(uint64)
		if txID < minTxID {
			r := value.(*reclamation)

			r.mu.Lock()
			for _, node := range r.nodes {
				base.Pool.Put(node)
				reclaimed++
			}
			r.mu.Unlock()

			// Remove this TxID from reclaim map
			c.reclaim.Delete(txID)
		}
		return true // Continue iteration
	})

	return reclaimed
}

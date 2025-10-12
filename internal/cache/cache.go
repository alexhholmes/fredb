package cache

import (
	"container/list"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
)

// Cache implements a simple LRU cache without awareness of version tracking or
// disk I/O.
type Cache struct {
	mu       sync.RWMutex
	lru      *list.List             // Doubly-linked list (front=MRU, back=LRU)
	entries  map[base.PageID]*entry // Single entry per page
	maxSize  int                    // Max total entries (e.g., 1024)
	lowWater int                    // Evict to this (80% of max)

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// entry represents a cached Node in the LRU cache
type entry struct {
	id         base.PageID
	node       *base.Node    // Parsed BTree node
	pinCount   int           // For preventing eviction of in-use entries
	lruElement *list.Element // Position in LRU list
}

const (
	MinCacheSize = 16 // Minimum: hold tree path + concurrent ops
)

// NewCache creates a new Page cache with the specified maximum size
func NewCache(maxSize int) *Cache {
	maxSize = max(maxSize, MinCacheSize)

	return &Cache{
		maxSize:  maxSize,
		lowWater: (maxSize * 4) / 5, // 80%
		entries:  make(map[base.PageID]*entry),
		lru:      list.New(),
	}
}

// Put adds a node to the cache, replacing any existing entry for the id.
func (c *Cache) Put(pageID base.PageID, node *base.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if ent already exists
	if existing, exists := c.entries[pageID]; exists {
		// Update existing ent
		existing.node = node
		c.lru.MoveToFront(existing.lruElement)
		return
	}

	// Create new ent
	ent := &entry{
		id:       pageID,
		node:     node,
		pinCount: 0,
	}
	ent.lruElement = c.lru.PushFront(ent)

	// Add to cache
	c.entries[pageID] = ent

	// Trigger eviction if over limit
	if len(c.entries) >= c.maxSize {
		// Evict unpinned entries from LRU end until at lowWater
		target := c.lowWater

		for len(c.entries) > target {
			elem := c.lru.Back()
			if elem == nil {
				break
			}

			e := elem.Value.(*entry)

			// Skip pinned entries
			if e.pinCount > 0 {
				c.lru.MoveToFront(elem)
				// If all entries are pinned, we can't evict anything
				if elem == c.lru.Front() {
					break
				}
				continue
			}

			// Remove from LRU list
			c.lru.Remove(elem)

			// Remove from cache
			delete(c.entries, e.id)
			c.evictions.Add(1)
		}
	}
}

// Get retrieves a node from the cache.
// Returns (Node, true) on cache hit, (nil, false) on miss.
func (c *Cache) Get(pageID base.PageID) (*base.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[pageID]
	if !exists {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.lru.MoveToFront(entry.lruElement)
	return entry.node, true
}

// Delete removes a page from the cache.
func (c *Cache) Delete(pageID base.PageID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[pageID]
	if !exists {
		return
	}

	// Remove from LRU list
	if entry.lruElement != nil {
		c.lru.Remove(entry.lruElement)
		c.evictions.Add(1)
	}

	// Remove from cache
	delete(c.entries, pageID)
}

// Size returns current number of cached entries
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// Stats returns cache statistics
func (c *Cache) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

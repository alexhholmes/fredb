package pkg

import (
	"container/list"
	"fmt"
	"sync"
	"sync/atomic"
)

// PageCache implements an LRU cache for deserialized BTree nodes
// with pin/unpin semantics to prevent eviction of in-use pages.
type PageCache struct {
	maxSize  int                    // Max pages (e.g., 1024 = ~5MB)
	lowWater int                    // Evict to this (80% of max)
	entries  map[PageID]*cacheEntry // O(1) lookup
	lruList  *list.List             // Doubly-linked list (front=MRU, back=LRU)
	mu       sync.RWMutex

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// cacheEntry represents a cached node with LRU tracking and pin count
type cacheEntry struct {
	pageID     PageID
	node       *Node         // Parsed BTree node
	pinCount   int           // Ref count (0 = evictable)
	lruElement *list.Element // Position in LRU list
}

const (
	DefaultCacheSize = 1024  // 5MB (1024 pages × 4KB)
	MinCacheSize     = 16    // Minimum: hold tree path + concurrent ops
	MaxCacheSize     = 65536 // 320MB TODO increase if needed
)

// NewPageCache creates a new page cache with the specified maximum size
func NewPageCache(maxSize int) *PageCache {
	if maxSize < MinCacheSize {
		maxSize = MinCacheSize
	}
	if maxSize > MaxCacheSize {
		maxSize = MaxCacheSize
	}

	return &PageCache{
		maxSize:  maxSize,
		lowWater: (maxSize * 4) / 5, // 80%
		entries:  make(map[PageID]*cacheEntry),
		lruList:  list.New(),
	}
}

// Get retrieves a node from cache and pins it for use.
// Returns (node, true) on cache hit, (nil, false) on miss.
// Caller must call Unpin() when done with the node.
func (c *PageCache) Get(pageID PageID) (*Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[pageID]
	if !exists {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	entry.pinCount++                        // Pin for use
	c.lruList.MoveToFront(entry.lruElement) // Mark as MRU
	return entry.node, true
}

// Put adds a node to the cache (initially pinned).
// If the node is already cached, increments its pin count.
// Caller must call Unpin() when done with the node.
func (c *PageCache) Put(pageID PageID, node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, exists := c.entries[pageID]; exists {
		// Node already cached - increment pin for new reference and update node
		entry.node = node
		entry.pinCount++
		c.lruList.MoveToFront(entry.lruElement)
		return
	}

	// New entry
	entry := &cacheEntry{
		pageID:   pageID,
		node:     node,
		pinCount: 1, // Initially pinned
	}
	entry.lruElement = c.lruList.PushFront(entry)
	c.entries[pageID] = entry

	// Trigger batch eviction if at or over limit
	if len(c.entries) >= c.maxSize {
		c.evictToWaterMark()
	}
}

// Unpin releases a reference to a cached node.
// When pinCount reaches 0, the node becomes eligible for eviction.
func (c *PageCache) Unpin(pageID PageID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.entries[pageID]
	if !exists {
		// Already evicted or never cached
		return
	}

	entry.pinCount--
	if entry.pinCount < 0 {
		panic(fmt.Sprintf("pagecache: double unpin detected for page %d", pageID))
	}
}

// FlushDirty writes all dirty cached nodes to disk using the provided pager.
// Returns the first error encountered, but attempts to flush all dirty nodes.
func (c *PageCache) FlushDirty(pager *PageManager) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	// Iterate through all entries and flush dirty ones
	for _, entry := range c.entries {
		if !entry.node.dirty {
			continue
		}

		// Serialize node
		if serErr := entry.node.serialize(); serErr != nil {
			if err == nil {
				err = serErr
			}
			continue
		}

		// Write to disk
		if writeErr := (*pager).WritePage(entry.pageID, entry.node.page); writeErr != nil {
			if err == nil {
				err = writeErr
			}
			continue
		}

		// Clear dirty flag after successful write
		entry.node.dirty = false
	}

	return err
}

// WriteDirtyPages writes specific dirty pages to disk without affecting their pin count
func (c *PageCache) WriteDirtyPages(pageIDs []PageID, pager PageManager) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pageID := range pageIDs {
		if entry, exists := c.entries[pageID]; exists && entry.node.dirty {
			if err := entry.node.serialize(); err != nil {
				return err
			}
			if err := pager.WritePage(entry.node.pageID, entry.node.page); err != nil {
				return err
			}
			entry.node.dirty = false
		}
	}

	return nil
}

// Stats returns cache statistics
func (c *PageCache) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// Size returns current number of cached pages
func (c *PageCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}

// evictToWaterMark evicts unpinned pages from LRU end until at lowWater.
// Must be called with write lock held.
func (c *PageCache) evictToWaterMark() {
	target := c.lowWater
	attempts := 0
	maxAttempts := c.maxSize * 2 // Avoid infinite loop if all pages pinned

	for len(c.entries) > target && attempts < maxAttempts {
		elem := c.lruList.Back() // Oldest (LRU)
		if elem == nil {
			break // Empty list
		}

		entry := elem.Value.(*cacheEntry)
		attempts++

		// Can't evict pinned pages
		if entry.pinCount > 0 {
			// Move to front to avoid checking again immediately
			c.lruList.MoveToFront(elem)
			continue
		}

		// Skip dirty pages for now (will be flushed during eviction in future)
		// For now we just keep them in cache
		if entry.node.dirty {
			c.lruList.MoveToFront(elem)
			continue
		}

		// Evict clean, unpinned page
		c.lruList.Remove(elem)
		delete(c.entries, entry.pageID)
		c.evictions.Add(1)
	}
}

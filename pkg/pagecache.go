package pkg

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// PageCache implements a versioned LRU cache for MVCC isolation.
// Each page can have multiple versions (one per committing transaction).
// Readers see the latest version where version.txnID <= reader.txnID.
type PageCache struct {
	maxSize  int                               // Max total versions (e.g., 1024)
	lowWater int                               // Evict to this (80% of max)
	entries  map[PageID][]*VersionedEntry     // Multiple versions per page
	lruList  *list.List                        // Doubly-linked list (front=MRU, back=LRU)
	mu       sync.RWMutex

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// VersionedEntry represents a cached node version with MVCC tracking
type VersionedEntry struct {
	pageID     PageID
	txnID      uint64        // Transaction that committed this version
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
		entries:  make(map[PageID][]*VersionedEntry),
		lruList:  list.New(),
	}
}

// Get retrieves a node version visible to the transaction (MVCC snapshot isolation).
// Returns the latest version where version.txnID <= txnID.
// Returns (node, true) on cache hit, (nil, false) on miss.
func (c *PageCache) Get(pageID PageID, txnID uint64) (*Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	versions, exists := c.entries[pageID]
	if !exists || len(versions) == 0 {
		c.misses.Add(1)
		return nil, false
	}

	// Find latest version visible to this transaction
	// Versions are sorted by txnID (ascending), so search backwards
	var found *VersionedEntry
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i].txnID <= txnID {
			found = versions[i]
			break
		}
	}

	if found == nil {
		c.misses.Add(1)
		return nil, false
	}

	c.hits.Add(1)
	c.lruList.MoveToFront(found.lruElement) // Mark as MRU
	return found.node, true
}

// Put adds a new version of a page to the cache.
// The version is tagged with the committing transaction's ID for MVCC.
func (c *PageCache) Put(pageID PageID, txnID uint64, node *Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if this exact version already exists (race condition protection)
	if versions, exists := c.entries[pageID]; exists {
		for _, existing := range versions {
			if existing.txnID == txnID {
				// Version already cached, update LRU and return
				c.lruList.MoveToFront(existing.lruElement)
				return
			}
		}
	}

	// Create new versioned entry
	entry := &VersionedEntry{
		pageID:   pageID,
		txnID:    txnID,
		node:     node,
		pinCount: 0, // Not pinned initially (caller doesn't hold reference)
	}
	entry.lruElement = c.lruList.PushFront(entry)

	// Append to versions list (maintains sorted order if txnID is monotonic)
	c.entries[pageID] = append(c.entries[pageID], entry)

	// Count total entries across all versions
	totalEntries := 0
	for _, versions := range c.entries {
		totalEntries += len(versions)
	}

	// Trigger batch eviction if at or over limit
	if totalEntries >= c.maxSize {
		c.evictToWaterMark()
	}
}

// Unpin is a no-op in the versioned cache (kept for API compatibility).
// With the hybrid model, pages in use are in tx.pages, not the global cache.
func (c *PageCache) Unpin(pageID PageID) {
	// No-op: versioned cache doesn't use pinning
}

// FlushDirty writes all dirty cached node versions to disk.
// Returns the first error encountered, but attempts to flush all dirty nodes.
func (c *PageCache) FlushDirty(pager *PageManager) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	// Iterate through all page versions
	for _, versions := range c.entries {
		for _, entry := range versions {
			if !entry.node.dirty {
				continue
			}

			// Serialize node with the transaction ID that committed this version
			if serErr := entry.node.serialize(entry.txnID); serErr != nil {
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
	}

	return err
}

// WriteDirtyPages is deprecated in versioned cache (no-op for compatibility).
// With the hybrid model, tx.Commit() handles writes directly.
func (c *PageCache) WriteDirtyPages(pageIDs []PageID, pager PageManager) error {
	// No-op: hybrid model writes pages directly in tx.Commit()
	return nil
}

// Stats returns cache statistics
func (c *PageCache) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// Size returns current number of cached page versions
func (c *PageCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := 0
	for _, versions := range c.entries {
		total += len(versions)
	}
	return total
}

// Invalidate removes all cached versions of a page.
// Used when a PageID is reallocated to prevent stale cache hits.
func (c *PageCache) Invalidate(pageID PageID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	versions, exists := c.entries[pageID]
	if !exists {
		return
	}

	// Remove all versions from LRU list
	for _, entry := range versions {
		if entry.lruElement != nil {
			c.lruList.Remove(entry.lruElement)
			c.evictions.Add(1)
		}
	}

	// Remove pageID entirely from cache
	delete(c.entries, pageID)
}

// evictToWaterMark evicts old versions from LRU end until at lowWater.
// Must be called with write lock held.
func (c *PageCache) evictToWaterMark() {
	target := c.lowWater
	attempts := 0
	maxAttempts := c.maxSize * 2 // Avoid infinite loop

	totalEntries := 0
	for _, versions := range c.entries {
		totalEntries += len(versions)
	}

	for totalEntries > target && attempts < maxAttempts {
		elem := c.lruList.Back() // Oldest (LRU)
		if elem == nil {
			break // Empty list
		}

		entry := elem.Value.(*VersionedEntry)
		attempts++

		// Skip dirty pages for now (will be flushed during eviction in future)
		if entry.node.dirty {
			c.lruList.MoveToFront(elem)
			continue
		}

		// Evict clean version
		c.lruList.Remove(elem)

		// Remove from versions array
		versions := c.entries[entry.pageID]
		for i, v := range versions {
			if v == entry {
				// Remove this version
				c.entries[entry.pageID] = append(versions[:i], versions[i+1:]...)
				break
			}
		}

		// If no versions left, remove pageID entirely
		if len(c.entries[entry.pageID]) == 0 {
			delete(c.entries, entry.pageID)
		}

		c.evictions.Add(1)
		totalEntries--
	}
}

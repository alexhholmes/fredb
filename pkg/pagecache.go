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
	maxSize  int                          // Max total versions (e.g., 1024)
	lowWater int                          // Evict to this (80% of max)
	entries  map[PageID][]*VersionedEntry // Multiple versions per page
	lruList  *list.List                   // Doubly-linked list (front=MRU, back=LRU)
	mu       sync.RWMutex
	pager    PageManager // For flushing dirty pages during eviction

	// Atomic GetOrLoad coordination
	loadStates  sync.Map // map[PageID]*loadState - coordinate concurrent disk loads
	generations sync.Map // map[PageID]uint64 - detect invalidation during load

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// loadState coordinates concurrent disk loads for the same PageID
type loadState struct {
	mu        sync.Mutex
	loading   bool          // Is someone currently loading?
	done      chan struct{} // Closed when load completes
	node      *Node         // Loaded node
	diskTxnID uint64        // TxnID read from disk
	err       error         // Load error if any
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
func NewPageCache(maxSize int, pager PageManager) *PageCache {
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
		pager:    pager,
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

	// Increment generation to invalidate in-flight loads
	gen := c.getGeneration(pageID)
	c.generations.Store(pageID, gen+1)

	// Clear any coordination state for in-flight loads
	c.loadStates.Delete(pageID)

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

// GetOrLoad retrieves a node from cache or loads it from disk atomically.
// Coordinates concurrent loads to prevent multiple threads loading the same page.
// Returns (node, true) if version is visible, (nil, false) otherwise.
func (c *PageCache) GetOrLoad(pageID PageID, txnID uint64,
	loadFunc func() (*Node, uint64, error)) (*Node, bool) {

	// Fast path: active cache first
	if node, hit := c.Get(pageID, txnID); hit {
		return node, true
	}

	// Cache miss - coordinate disk load
	// Capture generation before load to detect invalidation
	startGen := c.getGeneration(pageID)

	// LoadOrStore ensures only one loadState exists per PageID
	stateVal, _ := c.loadStates.LoadOrStore(pageID, &loadState{
		done: make(chan struct{}),
	})
	state := stateVal.(*loadState)

	// Try to become the loader
	state.mu.Lock()
	if state.loading {
		// Someone else is loading, wait for them
		state.mu.Unlock()
		<-state.done

		// Check result from loader
		if state.err != nil {
			return nil, false
		}

		// Check if loaded version is visible to this transaction
		if state.diskTxnID <= txnID {
			// Loader already cached it, return from cache
			if node, hit := c.Get(pageID, txnID); hit {
				return node, true
			}
		}

		return nil, false
	}

	// We're the loader
	state.loading = true
	state.mu.Unlock()

	// Load from disk WITHOUT holding any locks (I/O)
	node, diskTxnID, err := loadFunc()

	// Store result for waiting threads
	state.mu.Lock()
	state.node = node
	state.diskTxnID = diskTxnID
	state.err = err
	close(state.done) // Wake up waiters
	state.mu.Unlock()

	if err != nil {
		c.loadStates.Delete(pageID)
		return nil, false
	}

	// Check if page was invalidated during load
	endGen := c.getGeneration(pageID)
	if endGen != startGen {
		// Page was freed/reallocated during load, discard
		c.loadStates.Delete(pageID)
		return nil, false
	}

	// Cache the result (makes it visible to future threads)
	c.Put(pageID, diskTxnID, node)

	// Cleanup coordination state
	c.loadStates.Delete(pageID)

	// Check visibility for THIS transaction
	if diskTxnID <= txnID {
		return node, true
	}

	return nil, false
}

// getGeneration returns the current generation counter for a PageID.
// Used to detect invalidation during disk loads.
func (c *PageCache) getGeneration(pageID PageID) uint64 {
	if val, ok := c.generations.Load(pageID); ok {
		return val.(uint64)
	}
	return 0
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

		// Flush dirty pages before evicting
		if entry.node.dirty {
			// Serialize and write to disk
			if err := entry.node.serialize(entry.txnID); err == nil {
				if err := c.pager.WritePage(entry.pageID, entry.node.page); err == nil {
					entry.node.dirty = false
				}
			}

			// If flush failed, skip this page and try next
			if entry.node.dirty {
				c.lruList.MoveToFront(elem)
				continue
			}
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

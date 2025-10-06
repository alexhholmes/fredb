package fredb

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

	// Version relocation tracking for old versions needed by long-running readers
	versionMap *VersionMap

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
	node      *node         // Loaded node
	diskTxnID uint64        // TxnID read from disk
	err       error         // Load error if any
}

// VersionedEntry represents a cached node version with MVCC tracking
type VersionedEntry struct {
	pageID     PageID
	txnID      uint64        // Transaction that committed this version
	node       *node         // Parsed BTree node
	pinCount   int           // Ref count (0 = evictable)
	lruElement *list.Element // Position in LRU list
}

const (
	DefaultCacheSize = 1024      // 5MB (1024 pages × 4KB)
	MinCacheSize     = 16        // Minimum: hold tree path + concurrent ops
	MaxCacheSize     = 335544320 // 320MB TODO increase if needed
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
		maxSize:    maxSize,
		lowWater:   (maxSize * 4) / 5, // 80%
		entries:    make(map[PageID][]*VersionedEntry),
		lruList:    list.New(),
		pager:      pager,
		versionMap: NewVersionMap(),
	}
}

// Get retrieves a node version visible to the transaction (MVCC snapshot isolation).
// Returns the latest version where version.txnID <= txnID.
// Returns (node, true) on cache hit, (nil, false) on miss.
func (c *PageCache) Get(pageID PageID, txnID uint64) (*node, bool) {
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
func (c *PageCache) Put(pageID PageID, txnID uint64, node *node) {
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

			// serialize node with the transaction ID that committed this version
			page, serErr := entry.node.serialize(entry.txnID)
			if serErr != nil {
				if err == nil {
					err = serErr
				}
				continue
			}

			// Write to disk
			if writeErr := (*pager).WritePage(entry.pageID, page); writeErr != nil {
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
	loadFunc func() (*node, uint64, error)) (*node, bool) {

	// Fast path: active cache first
	if node, hit := c.Get(pageID, txnID); hit {
		return node, true
	}

	// Cache miss - check if ANY visible relocated version exists
	// Do this BEFORE loading from disk, because disk might have a newer version
	if node, hit := c.loadRelocatedVersion(pageID, txnID); hit {
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

		// Disk version is too new - check relocated versions
		return c.loadRelocatedVersion(pageID, txnID)
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

	// Disk version is too new - check relocated versions
	return c.loadRelocatedVersion(pageID, txnID)
}

// getGeneration returns the current generation counter for a PageID.
// Used to detect invalidation during disk loads.
func (c *PageCache) getGeneration(pageID PageID) uint64 {
	if val, ok := c.generations.Load(pageID); ok {
		return val.(uint64)
	}
	return 0
}

// loadRelocatedVersion attempts to load an older relocated version visible to txnID.
// Returns (node, true) if found, (nil, false) otherwise.
func (c *PageCache) loadRelocatedVersion(pageID PageID, txnID uint64) (*node, bool) {
	relocatedPageID, relocatedTxnID := c.versionMap.GetLatestVisible(pageID, txnID)
	if relocatedPageID == 0 {
		return nil, false
	}

	// Found an older relocated version
	page, err := c.pager.ReadPage(relocatedPageID)
	if err != nil {
		return nil, false
	}

	relocatedNode := &node{
		pageID: pageID, // Use original pageID, not relocated
		dirty:  false,
	}
	header := page.Header()
	if header.NumKeys == 0 {
		return nil, false
	}

	if err := relocatedNode.deserialize(page); err != nil {
		return nil, false
	}

	// Cache the relocated version
	c.Put(pageID, relocatedTxnID, relocatedNode)
	return relocatedNode, true
}

// EvictCheckpointed removes page versions that have been checkpointed to disk
// AND are older than all active readers.
// Only versions where (txnID <= CheckpointTxnID AND txnID < minReaderTxn) are safe to evict.
// Called by background checkpointer after checkpoint completes.
func (c *PageCache) EvictCheckpointed(minReaderTxn uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta := c.pager.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	evicted := 0

	for pageID, versions := range c.entries {
		// Track which versions to keep
		keep := make([]*VersionedEntry, 0, len(versions))

		for _, entry := range versions {
			// Only evict if BOTH conditions are true:
			// 1. Version is checkpointed (on disk)
			// 2. Version is older than all active readers (no reader needs it)
			if entry.txnID <= checkpointTxn && entry.txnID < minReaderTxn {
				// Safe to evict
				if entry.lruElement != nil {
					c.lruList.Remove(entry.lruElement)
				}
				evicted++
				c.evictions.Add(1)
			} else {
				// Keep version (either in WAL only or needed by reader)
				keep = append(keep, entry)
			}
		}

		if len(keep) == 0 {
			// All versions evicted, remove pageID
			delete(c.entries, pageID)
		} else {
			// Update versions list
			c.entries[pageID] = keep
		}
	}

	return evicted
}

// canEvict returns true if a page version is safe to evict.
// A version is safe to evict if it has been checkpointed to disk.
func (c *PageCache) canEvict(txnID uint64) bool {
	if c.pager == nil {
		return true // No pager (test mode), allow eviction
	}
	meta := c.pager.GetMeta()
	return txnID <= meta.CheckpointTxnID
}

// CleanupRelocatedVersions removes VersionMap entries for versions older than minReaderTxn
// and adds relocated pages to freelist.pending.
// Called by background releaser when readers finish.
// Returns the number of relocated versions cleaned up.
func (c *PageCache) CleanupRelocatedVersions(minReaderTxn uint64) int {
	// Cleanup VersionMap entries
	freedPages := c.versionMap.Cleanup(minReaderTxn)

	// Add relocated pages to freelist.pending with CURRENT minReaderTxn
	// This prevents them from being allocated by transactions currently running
	// They'll be freed when Release() is called with a higher minReaderTxn
	if len(freedPages) > 0 && c.pager != nil {
		_ = c.pager.FreePending(minReaderTxn, freedPages)
	}

	return len(freedPages)
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

		// Only evict checkpointed versions (on disk)
		if !c.canEvict(entry.txnID) {
			// Version only in WAL, skip
			c.lruList.MoveToFront(elem)
			continue
		}

		// Flush dirty pages before evicting
		if entry.node.dirty {
			// serialize and write to disk
			page, err := entry.node.serialize(entry.txnID)
			if err == nil {
				if err := c.pager.WritePage(entry.pageID, page); err == nil {
					entry.node.dirty = false
				}
			}

			// If flush failed, skip this page and try next
			if entry.node.dirty {
				c.lruList.MoveToFront(elem)
				continue
			}
		}

		// Before evicting checkpointed version, check if we need to relocate it
		// Relocate if there are multiple versions of this page (readers might need old version)
		if len(c.entries[entry.pageID]) > 1 {
			// serialize the version
			page, err := entry.node.serialize(entry.txnID)
			if err != nil {
				// Can't serialize, skip
				c.lruList.MoveToFront(elem)
				continue
			}

			// Allocate a free page for relocation
			relocatedPageID, err := c.pager.AllocatePage()
			if err != nil {
				// Can't allocate, skip eviction
				c.lruList.MoveToFront(elem)
				continue
			}

			// Write version to relocated page
			if err := c.pager.WritePage(relocatedPageID, page); err != nil {
				// Write failed, free the allocated page and skip
				_ = c.pager.FreePage(relocatedPageID)
				c.lruList.MoveToFront(elem)
				continue
			}

			// Track relocation
			// Note: relocated page will be freed by CleanupRelocatedVersions()
			c.versionMap.Track(entry.pageID, entry.txnID, relocatedPageID)
		}

		// Evict version from cache
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

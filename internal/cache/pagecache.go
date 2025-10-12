package cache

import (
	"container/list"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
	"fredb/internal/storage"
)

// PageCache implements a versioned LRU cache for MVCC isolation.
// Each Page can have multiple versions (one per committing transaction).
// Readers see the latest version where version.txnID <= reader.txnID.
type PageCache struct {
	mu       sync.RWMutex
	maxSize  int                             // Max total versions (e.g., 1024)
	lowWater int                             // Evict to this (80% of max)
	entries  map[base.PageID][]*versionEntry // Multiple versions per Page
	lruList  *list.List                      // Doubly-linked list (front=MRU, back=LRU)
	pager    *storage.PageManager            // Sibling-reference not ownership

	// Atomic GetOrLoad coordination
	loadStates  sync.Map // map[PageID]*loadState - coordinate concurrent disk loads
	generations sync.Map // map[PageID]uint64 - detect invalidation during load

	// Stats
	hits         atomic.Uint64
	misses       atomic.Uint64
	evictions    atomic.Uint64
	totalEntries atomic.Int64 // Total number of cached versions
}

// loadState coordinates concurrent disk loads for the same PageID
type loadState struct {
	mu        sync.Mutex
	loading   bool          // Is someone currently loading?
	done      chan struct{} // Closed when load completes
	node      *base.Node    // Loaded node
	diskTxnID uint64        // TxID read from disk
	err       error         // Load error if any
}

// versionEntry represents a cached Node version with MVCC tracking
type versionEntry struct {
	pageID     base.PageID
	txnID      uint64        // Transaction that committed this version
	node       *base.Node    // Parsed BTree node
	pinCount   int           // Ref count (0 = evictable)
	lruElement *list.Element // Position in LRU list
}

const (
	MinCacheSize = 16 // Minimum: hold tree path + concurrent ops
)

// NewPageCache creates a new Page cache with the specified maximum size
func NewPageCache(maxSize int, pagemanager *storage.PageManager) *PageCache {
	maxSize = max(maxSize, MinCacheSize)

	return &PageCache{
		maxSize:  maxSize,
		lowWater: (maxSize * 4) / 5, // 80%
		entries:  make(map[base.PageID][]*versionEntry),
		lruList:  list.New(),
		pager:    pagemanager,
	}
}

// Put adds a new version of a Page to the cache.
// The version is tagged with the committing transaction's ID for MVCC.
func (c *PageCache) Put(pageID base.PageID, txnID uint64, node *base.Node) {
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
	entry := &versionEntry{
		pageID:   pageID,
		txnID:    txnID,
		node:     node,
		pinCount: 0, // Not pinned initially (caller doesn't hold reference)
	}
	entry.lruElement = c.lruList.PushFront(entry)

	// Append to versions list (maintains sorted order if txnID is monotonic)
	c.entries[pageID] = append(c.entries[pageID], entry)
	c.totalEntries.Add(1)

	// Trigger batch eviction if at or over limit
	if int(c.totalEntries.Load()) >= c.maxSize {
		c.evictToWaterMark()
	}
}

// FlushDirty writes all Dirty cached Node versions to disk.
// Returns the first error encountered, but attempts to flush all Dirty nodes.
func (c *PageCache) FlushDirty(pager *storage.PageManager) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	// Iterate through all Page versions
	for _, versions := range c.entries {
		for _, entry := range versions {
			if !entry.node.Dirty {
				continue
			}

			// Serialize Node with the transaction ID that committed this version
			page, serErr := entry.node.Serialize(entry.txnID)
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

			// Clear Dirty flag after successful write
			entry.node.Dirty = false
		}
	}

	return err
}

// Stats returns cache statistics
func (c *PageCache) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// Size returns current number of cached Page versions
func (c *PageCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := 0
	for _, versions := range c.entries {
		total += len(versions)
	}
	return total
}

// Invalidate removes all cached versions of a Page.
// Used when a PageID is reallocated to prevent stale cache hits.
func (c *PageCache) Invalidate(pageID base.PageID) {
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
	numVersions := len(versions)
	for _, entry := range versions {
		if entry.lruElement != nil {
			c.lruList.Remove(entry.lruElement)
			c.evictions.Add(1)
		}
	}

	// Remove PageID entirely from cache
	delete(c.entries, pageID)
	c.totalEntries.Add(-int64(numVersions))
}

// EvictCheckpointed removes Page versions that have been checkpointed to disk
// AND are superseded by newer versions visible to all active readers.
// For each page, keeps the latest version where txnID <= minReaderTxn (what oldest reader sees)
// plus any newer versions. Evicts older superseded versions that are checkpointed.
// Called by background checkpointer after checkpoint completes.
func (c *PageCache) EvictCheckpointed(minReaderTxn uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta := c.pager.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	evicted := 0

	for pageID, versions := range c.entries {
		// Find the latest version visible to oldest reader (txnID <= minReaderTxn)
		// This is the version readers actually see
		latestVisibleIdx := -1
		for i := len(versions) - 1; i >= 0; i-- {
			if versions[i].txnID <= minReaderTxn {
				latestVisibleIdx = i
				break
			}
		}

		// Keep versions: latest visible to readers + all newer versions
		keep := make([]*versionEntry, 0, len(versions))

		for i, entry := range versions {
			// Keep if:
			// 1. It's the latest version visible to oldest reader, OR
			// 2. It's newer than oldest reader (future transactions might need it)
			// Evict if: checkpointed AND superseded by latestVisible version
			shouldEvict := entry.txnID <= checkpointTxn &&
				latestVisibleIdx != -1 &&
				i < latestVisibleIdx

			if shouldEvict {
				// Evict superseded version
				if entry.lruElement != nil {
					c.lruList.Remove(entry.lruElement)
				}
				evicted++
				c.evictions.Add(1)
				c.totalEntries.Add(-1)
			} else {
				// Keep version
				keep = append(keep, entry)
			}
		}

		if len(keep) == 0 {
			// All versions evicted, remove PageID
			delete(c.entries, pageID)
		} else {
			// Update versions list
			c.entries[pageID] = keep
		}
	}

	return evicted
}

// GetOrLoad retrieves a Node from cache or loads it from disk atomically.
// Coordinates concurrent loads to prevent multiple threads loading the same Page.
// Returns (Node, true) if version is visible, (nil, false) otherwise.
func (c *PageCache) GetOrLoad(pageID base.PageID, txnID uint64,
	loadFunc func() (*base.Node, uint64, error)) (*base.Node, bool) {

	// Fast path: active cache first
	if node, hit := c.get(pageID, txnID); hit {
		return node, true
	}

	// Check for relocated versions (MVCC), may hit disk
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
			if node, hit := c.get(pageID, txnID); hit {
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

	// store result for waiting threads
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

	// Check if Page was invalidated during load
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

// get retrieves a Node version visible to the transaction (MVCC snapshot isolation).
// Returns the latest version where version.txnID <= txnID.
// Returns (Node, true) on cache hit, (nil, false) on miss.
func (c *PageCache) get(pageID base.PageID, txnID uint64) (*base.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	versions, exists := c.entries[pageID]
	if !exists || len(versions) == 0 {
		c.misses.Add(1)
		return nil, false
	}

	// Find latest version visible to this transaction
	// Versions are sorted by txnID (ascending), so search backwards
	var found *versionEntry
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
	c.lruList.MoveToFront(found.lruElement)
	return found.node, true
}

// getGeneration returns the current generation counter for a PageID.
// Used to detect invalidation during disk loads.
func (c *PageCache) getGeneration(pageID base.PageID) uint64 {
	if val, ok := c.generations.Load(pageID); ok {
		return val.(uint64)
	}
	return 0
}

// loadRelocatedVersion attempts to load an older relocated version visible to txnID.
// Returns (Node, true) if found, (nil, false) otherwise.
func (c *PageCache) loadRelocatedVersion(pageID base.PageID, txnID uint64) (*base.Node, bool) {
	relocatedPageID, relocatedTxnID := c.pager.GetLatestVisible(pageID, txnID)
	if relocatedPageID == 0 {
		return nil, false
	}

	// Found an older relocated version
	page, err := c.pager.ReadPage(relocatedPageID)
	if err != nil {
		return nil, false
	}

	relocatedNode := &base.Node{
		PageID: pageID, // Use original PageID, not relocated
		Dirty:  false,
	}
	header := page.Header()
	if header.NumKeys == 0 {
		return nil, false
	}

	if err := relocatedNode.Deserialize(page); err != nil {
		return nil, false
	}

	// Cache the relocated version
	c.Put(pageID, relocatedTxnID, relocatedNode)
	return relocatedNode, true
}

// canEvict returns true if a Page version is safe to evict.
// A version is safe to evict if it has been checkpointed to disk.
func (c *PageCache) canEvict(txnID uint64) bool {
	if c.pager == nil {
		return true // No storage (test mode), allow eviction
	}
	meta := c.pager.GetMeta()
	return txnID <= meta.CheckpointTxnID
}

// CleanupRelocatedVersions removes VersionMap entries for versions older than minReaderTxn
// and adds relocated pages to freelist.pending.
// Called by background releaser when readers finish.
// Returns the number of relocated versions cleaned up.
func (c *PageCache) CleanupRelocatedVersions(minReaderTxn uint64) int {
	// Cleanup is now handled by ReleasePages since pendingVersions
	// contains both in-place freed pages and relocated versions
	return 0
}

// countEntries returns total number of cached entries across all pages
func (c *PageCache) countEntries() int {
	total := 0
	for _, versions := range c.entries {
		total += len(versions)
	}
	return total
}

// flushDirtyPage writes a Dirty node to disk and marks it clean.
// Returns true if successfully flushed, false if should skip eviction.
func (c *PageCache) flushDirtyPage(entry *versionEntry) bool {
	if !entry.node.Dirty {
		return true // Already clean
	}

	// Serialize and write to disk
	page, err := entry.node.Serialize(entry.txnID)
	if err != nil {
		return false
	}

	if err := c.pager.WritePage(entry.pageID, page); err != nil {
		return false
	}

	entry.node.Dirty = false
	return true
}

// relocateVersion writes an old version to a new location for MVCC.
// Returns true if successfully relocated, false if should skip eviction.
func (c *PageCache) relocateVersion(entry *versionEntry) bool {
	// Only relocate if there are multiple versions (readers might need old version)
	if len(c.entries[entry.pageID]) <= 1 {
		return true // No need to relocate
	}

	// Serialize the version
	page, err := entry.node.Serialize(entry.txnID)
	if err != nil {
		return false
	}

	// Allocate a free pages for relocation
	relocatedPageID, err := c.pager.AllocatePage()
	if err != nil {
		return false
	}

	// Write version to relocated pages
	if err := c.pager.WritePage(relocatedPageID, page); err != nil {
		_ = c.pager.FreePage(relocatedPageID)
		return false
	}

	// Track relocation
	c.pager.FreePendingRelocated(entry.txnID, entry.pageID, relocatedPageID)
	return true
}

// removeEntry removes a version entry from cache structures
func (c *PageCache) removeEntry(entry *versionEntry) {
	// Remove from LRU list
	c.lruList.Remove(entry.lruElement)

	// Remove from versions array
	versions := c.entries[entry.pageID]
	for i, v := range versions {
		if v == entry {
			c.entries[entry.pageID] = append(versions[:i], versions[i+1:]...)
			break
		}
	}

	// If no versions left, remove PageID entirely
	if len(c.entries[entry.pageID]) == 0 {
		delete(c.entries, entry.pageID)
	}

	c.totalEntries.Add(-1)
	c.evictions.Add(1)
}

// evictToWaterMark evicts old versions from LRU end until at lowWater.
// Must be called with write lock held.
func (c *PageCache) evictToWaterMark() {
	target := c.lowWater
	attempts := 0
	maxAttempts := c.maxSize * 2

	for int(c.totalEntries.Load()) > target && attempts < maxAttempts {
		elem := c.lruList.Back()
		if elem == nil {
			break
		}

		entry := elem.Value.(*versionEntry)
		attempts++

		// Flush if Dirty - write to disk before evicting from memory
		if !c.flushDirtyPage(entry) {
			c.lruList.MoveToFront(elem)
			continue
		}

		// Check if page has been checkpointed to disk
		// Pages only in WAL (not checkpointed) MUST be relocated so they're readable from disk
		isCheckpointed := c.canEvict(entry.txnID)

		if !isCheckpointed {
			// Page only exists in WAL, not at final disk location
			// Force relocation: write to new location so it's readable even after eviction
			page, err := entry.node.Serialize(entry.txnID)
			if err != nil {
				c.lruList.MoveToFront(elem)
				continue
			}

			relocatedPageID, err := c.pager.AllocatePage()
			if err != nil {
				c.lruList.MoveToFront(elem)
				continue
			}

			if err := c.pager.WritePage(relocatedPageID, page); err != nil {
				_ = c.pager.FreePage(relocatedPageID)
				c.lruList.MoveToFront(elem)
				continue
			}

			// Track relocation mapping (originalPageID -> relocatedPageID at txnID)
			c.pager.FreePendingRelocated(entry.txnID, entry.pageID, relocatedPageID)
		} else {
			// Already checkpointed - relocate only if needed for MVCC
			if !c.relocateVersion(entry) {
				c.lruList.MoveToFront(elem)
				continue
			}
		}

		// Evict from memory cache (removeEntry decrements totalEntries atomically)
		c.removeEntry(entry)
	}
}

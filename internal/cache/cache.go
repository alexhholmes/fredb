package cache

import (
	"container/list"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
	"fredb/internal/coordinator"
)

// Cache implements a versioned LRU cache for MVCC isolation.
// Each Page can have multiple versions (one per committing transaction).
// Readers see the latest version where version.txnID <= reader.txnID.
type Cache struct {
	mu       sync.RWMutex
	maxSize  int                             // Max total versions (e.g., 1024)
	lowWater int                             // Evict to this (80% of max)
	versions map[base.PageID][]*versionEntry // Multiple versions per Page
	lru      *list.List                      // Doubly-linked list (front=MRU, back=LRU)
	pager    *coordinator.Coordinator        // Sibling-reference not ownership

	// Atomic GetOrLoad coordination
	loadStates  sync.Map // map[PageID]*loadState - coordinate concurrent disk loads
	generations sync.Map // map[PageID]uint64 - detect invalidation during load

	// Stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
	entries   atomic.Int64 // Total number of cached versions
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

// NewCache creates a new Page cache with the specified maximum size
func NewCache(maxSize int, pagemanager *coordinator.Coordinator) *Cache {
	maxSize = max(maxSize, MinCacheSize)

	return &Cache{
		maxSize:  maxSize,
		lowWater: (maxSize * 4) / 5, // 80%
		versions: make(map[base.PageID][]*versionEntry),
		lru:      list.New(),
		pager:    pagemanager,
	}
}

// Put adds a new version of a Page to the cache.
// The version is tagged with the committing transaction's ID for MVCC.
func (c *Cache) Put(pageID base.PageID, txnID uint64, node *base.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if this exact version already exists (race condition protection)
	if versions, exists := c.versions[pageID]; exists {
		for _, existing := range versions {
			if existing.txnID == txnID {
				// Version already cached, update LRU and return
				c.lru.MoveToFront(existing.lruElement)
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
	entry.lruElement = c.lru.PushFront(entry)

	// Append to versions list (maintains sorted order if txnID is monotonic)
	c.versions[pageID] = append(c.versions[pageID], entry)
	c.entries.Add(1)

	// Trigger batch eviction if at or over limit
	if int(c.entries.Load()) >= c.maxSize {
		c.evictToWaterMark()
	}
}

// FlushDirty writes all Dirty cached Node versions to disk.
// Returns the first error encountered, but attempts to flush all Dirty nodes.
func (c *Cache) FlushDirty(pager *coordinator.Coordinator) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	// Iterate through all Page versions
	for _, versions := range c.versions {
		for _, entry := range versions {
			if !entry.node.Dirty {
				continue
			}

			// Delegate to coordinator for serialize + write
			if flushErr := (*pager).FlushNode(entry.node, entry.txnID, entry.pageID); flushErr != nil {
				if err == nil {
					err = flushErr
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
func (c *Cache) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// Size returns current number of cached Page versions
func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := 0
	for _, versions := range c.versions {
		total += len(versions)
	}
	return total
}

// Invalidate removes all cached versions of a Page.
// Used when a PageID is reallocated to prevent stale cache hits.
func (c *Cache) Invalidate(pageID base.PageID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Increment generation to invalidate in-flight loads
	gen := c.getGeneration(pageID)
	c.generations.Store(pageID, gen+1)

	// Clear any coordination state for in-flight loads
	c.loadStates.Delete(pageID)

	versions, exists := c.versions[pageID]
	if !exists {
		return
	}

	// Remove all versions from LRU list
	numVersions := len(versions)
	for _, entry := range versions {
		if entry.lruElement != nil {
			c.lru.Remove(entry.lruElement)
			c.evictions.Add(1)
		}
	}

	// Remove PageID entirely from cache
	delete(c.versions, pageID)
	c.entries.Add(-int64(numVersions))
}

// EvictCheckpointed removes Page versions that have been checkpointed to disk
// AND are superseded by newer versions visible to all active readers.
// For each page, keeps the latest version where txnID <= minReaderTxn (what oldest reader sees)
// plus any newer versions. Evicts older superseded versions that are checkpointed.
// Called by background checkpointer after checkpoint completes.
func (c *Cache) EvictCheckpointed(minReaderTxn uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta := c.pager.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	evicted := 0

	for pageID, versions := range c.versions {
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
					c.lru.Remove(entry.lruElement)
				}
				evicted++
				c.evictions.Add(1)
				c.entries.Add(-1)
			} else {
				// Keep version
				keep = append(keep, entry)
			}
		}

		if len(keep) == 0 {
			// All versions evicted, remove PageID
			delete(c.versions, pageID)
		} else {
			// Update versions list
			c.versions[pageID] = keep
		}
	}

	return evicted
}

// GetOrLoad retrieves a Node from cache or loads it from disk atomically.
// Coordinates concurrent loads to prevent multiple threads loading the same Page.
// Returns (Node, true) if version is visible, (nil, false) otherwise.
func (c *Cache) GetOrLoad(pageID base.PageID, txnID uint64) (*base.Node, bool) {
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
	node, diskTxnID, err := c.loadFromDisk(pageID)

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

// loadFromDisk delegates to coordinator for disk I/O and deserialization.
// Called by GetOrLoad when page is not in cache.
func (c *Cache) loadFromDisk(pageID base.PageID) (*base.Node, uint64, error) {
	return c.pager.LoadNodeFromDisk(pageID)
}

// Get retrieves a Node version visible to the transaction (MVCC snapshot isolation).
// Returns the latest version where version.txnID <= txnID.
// Returns (Node, true) on cache hit, (nil, false) on miss.
// Pure cache lookup - for future use when coordinator owns disk I/O.
func (c *Cache) Get(pageID base.PageID, txnID uint64) (*base.Node, bool) {
	return c.get(pageID, txnID)
}

// get retrieves a Node version visible to the transaction (MVCC snapshot isolation).
// Returns the latest version where version.txnID <= txnID.
// Returns (Node, true) on cache hit, (nil, false) on miss.
func (c *Cache) get(pageID base.PageID, txnID uint64) (*base.Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	versions, exists := c.versions[pageID]
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
	c.lru.MoveToFront(found.lruElement)
	return found.node, true
}

// getGeneration returns the current generation counter for a PageID.
// Used to detect invalidation during disk loads.
func (c *Cache) getGeneration(pageID base.PageID) uint64 {
	if val, ok := c.generations.Load(pageID); ok {
		return val.(uint64)
	}
	return 0
}

// loadRelocatedVersion attempts to load an older relocated version visible to txnID.
// Returns (Node, true) if found, (nil, false) otherwise.
func (c *Cache) loadRelocatedVersion(pageID base.PageID, txnID uint64) (*base.Node, bool) {
	relocatedPageID, relocatedTxnID := c.pager.GetLatestVisible(pageID, txnID)
	if relocatedPageID == 0 {
		return nil, false
	}

	// Delegate disk I/O and deserialization to coordinator
	node, err := c.pager.LoadRelocatedNode(pageID, relocatedPageID, relocatedTxnID)
	if err != nil {
		return nil, false
	}

	// Cache the relocated version
	c.Put(pageID, relocatedTxnID, node)
	return node, true
}

// canEvict returns true if a Page version is safe to evict.
// A version is safe to evict if it has been checkpointed to disk.
func (c *Cache) canEvict(txnID uint64) bool {
	if c.pager == nil {
		return true // No storage (test mode), allow eviction
	}
	meta := c.pager.GetMeta()
	return txnID <= meta.CheckpointTxnID
}

// CleanupRelocatedVersions removes VersionMap versions for versions older than minReaderTxn
// and adds relocated pages to freelist.pending.
// Called by background releaser when readers finish.
// Returns the number of relocated versions cleaned up.
func (c *Cache) CleanupRelocatedVersions(minReaderTxn uint64) int {
	// Cleanup is now handled by ReleasePages since pendingVersions
	// contains both in-place freed pages and relocated versions
	return 0
}

// countEntries returns total number of cached versions across all pages
func (c *Cache) countEntries() int {
	total := 0
	for _, versions := range c.versions {
		total += len(versions)
	}
	return total
}

// flushDirtyPage writes a Dirty node to disk and marks it clean.
// Returns true if successfully flushed, false if should skip eviction.
func (c *Cache) flushDirtyPage(entry *versionEntry) bool {
	if !entry.node.Dirty {
		return true // Already clean
	}

	// Delegate to coordinator for serialize + write
	if err := c.pager.FlushNode(entry.node, entry.txnID, entry.pageID); err != nil {
		return false
	}

	entry.node.Dirty = false
	return true
}

// relocateVersion writes an old version to a new location for MVCC.
// Returns true if successfully relocated, false if should skip eviction.
func (c *Cache) relocateVersion(entry *versionEntry) bool {
	// Only relocate if there are multiple versions (readers might need old version)
	if len(c.versions[entry.pageID]) <= 1 {
		return true // No need to relocate
	}

	// Delegate to coordinator for serialization, allocation, write, and tracking
	_, err := c.pager.RelocateVersion(entry.node, entry.txnID, entry.pageID)
	return err == nil
}

// removeEntry removes a version entry from cache structures
func (c *Cache) removeEntry(entry *versionEntry) {
	// Remove from LRU list
	c.lru.Remove(entry.lruElement)

	// Remove from versions array
	versions := c.versions[entry.pageID]
	for i, v := range versions {
		if v == entry {
			c.versions[entry.pageID] = append(versions[:i], versions[i+1:]...)
			break
		}
	}

	// If no versions left, remove PageID entirely
	if len(c.versions[entry.pageID]) == 0 {
		delete(c.versions, entry.pageID)
	}

	c.entries.Add(-1)
	c.evictions.Add(1)
}

// evictToWaterMark evicts old versions from LRU end until at lowWater.
// Must be called with write lock held.
func (c *Cache) evictToWaterMark() {
	target := c.lowWater
	attempts := 0
	maxAttempts := c.maxSize * 2

	for int(c.entries.Load()) > target && attempts < maxAttempts {
		elem := c.lru.Back()
		if elem == nil {
			break
		}

		entry := elem.Value.(*versionEntry)
		attempts++

		// Flush if Dirty - write to disk before evicting from memory
		if !c.flushDirtyPage(entry) {
			c.lru.MoveToFront(elem)
			continue
		}

		// Check if page has been checkpointed to disk
		// Pages only in WAL (not checkpointed) MUST be relocated so they're readable from disk
		isCheckpointed := c.canEvict(entry.txnID)

		if !isCheckpointed {
			// Page only exists in WAL, not at final disk location
			// Force relocation: delegate to coordinator
			if _, err := c.pager.RelocateVersion(entry.node, entry.txnID, entry.pageID); err != nil {
				c.lru.MoveToFront(elem)
				continue
			}
		} else {
			// Already checkpointed - relocate only if needed for MVCC
			if !c.relocateVersion(entry) {
				c.lru.MoveToFront(elem)
				continue
			}
		}

		// Evict from memory cache (removeEntry decrements entries atomically)
		c.removeEntry(entry)
	}
}

package fredb

import (
	"container/list"
	"sync"
	"sync/atomic"

	"fredb/internal"
)

// PageCache implements a versioned LRU cache for MVCC isolation.
// Each Page can have multiple versions (one per committing transaction).
// Readers see the latest version where version.txnID <= reader.txnID.
type PageCache struct {
	maxSize  int                                 // Max total versions (e.g., 1024)
	lowWater int                                 // Evict to this (80% of max)
	entries  map[internal.PageID][]*versionEntry // Multiple versions per Page
	lruList  *list.List                          // Doubly-linked list (front=MRU, back=LRU)
	mu       sync.RWMutex
	pager    *PageManager // For flushing Dirty pages during eviction

	// Version relocation tracking for old versions needed by long-running readers
	versionMap *VersionMap

	// Atomic getOrLoad coordination
	loadStates  sync.Map // map[PageID]*loadState - coordinate concurrent disk loads
	generations sync.Map // map[PageID]uint64 - detect invalidation during load

	// stats
	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

// loadState coordinates concurrent disk loads for the same PageID
type loadState struct {
	mu        sync.Mutex
	loading   bool           // Is someone currently loading?
	done      chan struct{}  // Closed when load completes
	node      *internal.Node // Loaded node
	diskTxnID uint64         // TxnID read from disk
	err       error          // Load error if any
}

// versionEntry represents a cached Node version with MVCC tracking
type versionEntry struct {
	pageID     internal.PageID
	txnID      uint64         // Transaction that committed this version
	node       *internal.Node // Parsed BTree node
	pinCount   int            // Ref count (0 = evictable)
	lruElement *list.Element  // Position in LRU list
}

const (
	DefaultCacheSize = 1024      // 5MB (1024 pages × 4KB)
	MinCacheSize     = 16        // Minimum: hold tree path + concurrent ops
	MaxCacheSize     = 335544320 // 320MB TODO increase if needed
)

// NewPageCache creates a new Page cache with the specified maximum size
func NewPageCache(maxSize int, pager *PageManager) *PageCache {
	if maxSize < MinCacheSize {
		maxSize = MinCacheSize
	}
	if maxSize > MaxCacheSize {
		maxSize = MaxCacheSize
	}

	return &PageCache{
		maxSize:    maxSize,
		lowWater:   (maxSize * 4) / 5, // 80%
		entries:    make(map[internal.PageID][]*versionEntry),
		lruList:    list.New(),
		pager:      pager,
		versionMap: NewVersionMap(),
	}
}

// get retrieves a Node version visible to the transaction (MVCC snapshot isolation).
// Returns the latest version where version.txnID <= txnID.
// Returns (Node, true) on cache hit, (nil, false) on miss.
func (c *PageCache) get(pageID internal.PageID, txnID uint64) (*internal.Node, bool) {
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

// Put adds a new version of a Page to the cache.
// The version is tagged with the committing transaction's ID for MVCC.
func (c *PageCache) Put(pageID internal.PageID, txnID uint64, node *internal.Node) {
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

// flushDirty writes all Dirty cached Node versions to disk.
// Returns the first error encountered, but attempts to flush all Dirty nodes.
func (c *PageCache) flushDirty(pager *PageManager) error {
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

// stats returns cache statistics
func (c *PageCache) stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// size returns current number of cached Page versions
func (c *PageCache) size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	total := 0
	for _, versions := range c.entries {
		total += len(versions)
	}
	return total
}

// invalidate removes all cached versions of a Page.
// Used when a PageID is reallocated to prevent stale cache hits.
func (c *PageCache) invalidate(pageID internal.PageID) {
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

	// Remove PageID entirely from cache
	delete(c.entries, pageID)
}

// getOrLoad retrieves a Node from cache or loads it from disk atomically.
// Coordinates concurrent loads to prevent multiple threads loading the same Page.
// Returns (Node, true) if version is visible, (nil, false) otherwise.
func (c *PageCache) getOrLoad(pageID internal.PageID, txnID uint64,
	loadFunc func() (*internal.Node, uint64, error)) (*internal.Node, bool) {

	// Fast path: active cache first
	if node, hit := c.get(pageID, txnID); hit {
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

// getGeneration returns the current generation counter for a PageID.
// Used to detect invalidation during disk loads.
func (c *PageCache) getGeneration(pageID internal.PageID) uint64 {
	if val, ok := c.generations.Load(pageID); ok {
		return val.(uint64)
	}
	return 0
}

// loadRelocatedVersion attempts to load an older relocated version visible to txnID.
// Returns (Node, true) if found, (nil, false) otherwise.
func (c *PageCache) loadRelocatedVersion(pageID internal.PageID, txnID uint64) (*internal.Node, bool) {
	relocatedPageID, relocatedTxnID := c.versionMap.GetLatestVisible(pageID, txnID)
	if relocatedPageID == 0 {
		return nil, false
	}

	// Found an older relocated version
	page, err := c.pager.ReadPage(relocatedPageID)
	if err != nil {
		return nil, false
	}

	relocatedNode := &internal.Node{
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

// evictCheckpointed removes Page versions that have been checkpointed to disk
// AND are older than all active readers.
// Only versions where (txnID <= CheckpointTxnID AND txnID < minReaderTxn) are safe to evict.
// Called by background checkpointer after checkpoint completes.
func (c *PageCache) evictCheckpointed(minReaderTxn uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta := c.pager.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	evicted := 0

	for pageID, versions := range c.entries {
		// Track which versions to keep
		keep := make([]*versionEntry, 0, len(versions))

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
			// All versions evicted, remove PageID
			delete(c.entries, pageID)
		} else {
			// Update versions list
			c.entries[pageID] = keep
		}
	}

	return evicted
}

// canEvict returns true if a Page version is safe to evict.
// A version is safe to evict if it has been checkpointed to disk.
func (c *PageCache) canEvict(txnID uint64) bool {
	if c.pager == nil {
		return true // No pager (test mode), allow eviction
	}
	meta := c.pager.GetMeta()
	return txnID <= meta.CheckpointTxnID
}

// cleanupRelocatedVersions removes VersionMap entries for versions older than minReaderTxn
// and adds relocated pages to freelist.pending.
// Called by background releaser when readers finish.
// Returns the number of relocated versions cleaned up.
func (c *PageCache) cleanupRelocatedVersions(minReaderTxn uint64) int {
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

	// Allocate a free page for relocation
	relocatedPageID, err := c.pager.AllocatePage()
	if err != nil {
		return false
	}

	// Write version to relocated page
	if err := c.pager.WritePage(relocatedPageID, page); err != nil {
		_ = c.pager.FreePage(relocatedPageID)
		return false
	}

	// Track relocation
	c.versionMap.Track(entry.pageID, entry.txnID, relocatedPageID)
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

	c.evictions.Add(1)
}

// evictToWaterMark evicts old versions from LRU end until at lowWater.
// Must be called with write lock held.
func (c *PageCache) evictToWaterMark() {
	target := c.lowWater
	attempts := 0
	maxAttempts := c.maxSize * 2
	totalEntries := c.countEntries()

	for totalEntries > target && attempts < maxAttempts {
		elem := c.lruList.Back()
		if elem == nil {
			break
		}

		entry := elem.Value.(*versionEntry)
		attempts++

		// Only evict checkpointed versions (on disk)
		if !c.canEvict(entry.txnID) {
			c.lruList.MoveToFront(elem)
			continue
		}

		// Flush if Dirty
		if !c.flushDirtyPage(entry) {
			c.lruList.MoveToFront(elem)
			continue
		}

		// Relocate if needed for MVCC
		if !c.relocateVersion(entry) {
			c.lruList.MoveToFront(elem)
			continue
		}

		// Evict
		c.removeEntry(entry)
		totalEntries--
	}
}

// VersionMap tracks where old Page versions have been relocated
// when evicted from cache. This allows long-running readers to access
// old versions that have been overwritten on disk by checkpoints.
//
// This is a purely in-memory structure - it doesn't need persistence because:
// - Readers don't survive crashes
// - Relocated pages are immediately added to freelist.pending
// - On restart, pending pages are freed (no readers exist)
//
// Example:
//   - Page 100 has version@3 (cached)
//   - Page 100 updated to version@8 (checkpointed to disk at PageID=100)
//   - Cache needs space, evicts version@3
//   - Relocate version@3 to free Page 500
//   - Track: versionMap[100][3] = 500
//   - Add Page 500 to freelist.pending[3]
//   - Reader@5 can still load version@3 from Page 500
type VersionMap struct {
	// Map: originalPageID -> (txnID -> relocatedPageID)
	relocations map[internal.PageID]map[uint64]internal.PageID
	mu          sync.RWMutex
}

// NewVersionMap creates a new version relocation tracker
func NewVersionMap() *VersionMap {
	return &VersionMap{
		relocations: make(map[internal.PageID]map[uint64]internal.PageID),
	}
}

// Track records that a Page version has been relocated to a new location
func (vm *VersionMap) Track(originalPageID internal.PageID, txnID uint64, relocatedPageID internal.PageID) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.relocations[originalPageID] == nil {
		vm.relocations[originalPageID] = make(map[uint64]internal.PageID)
	}
	vm.relocations[originalPageID][txnID] = relocatedPageID
}

// Get returns the relocated Page ID for a specific version, or 0 if not relocated
func (vm *VersionMap) Get(originalPageID internal.PageID, txnID uint64) internal.PageID {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if versions := vm.relocations[originalPageID]; versions != nil {
		return versions[txnID]
	}
	return 0
}

// GetLatestVisible returns the relocated Page ID for the latest version visible to txnID,
// or 0 if no visible version is relocated. Used for MVCC snapshot isolation.
func (vm *VersionMap) GetLatestVisible(originalPageID internal.PageID, maxTxnID uint64) (internal.PageID, uint64) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	versions := vm.relocations[originalPageID]
	if versions == nil {
		return 0, 0
	}

	// Find latest version where versionTxnID <= maxTxnID
	var latestTxnID uint64
	var latestPageID internal.PageID

	for versionTxnID, relocatedPageID := range versions {
		if versionTxnID <= maxTxnID && versionTxnID > latestTxnID {
			latestTxnID = versionTxnID
			latestPageID = relocatedPageID
		}
	}

	return latestPageID, latestTxnID
}

// Remove removes a relocation entry (called when version is no longer needed)
func (vm *VersionMap) Remove(originalPageID internal.PageID, txnID uint64) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if versions := vm.relocations[originalPageID]; versions != nil {
		delete(versions, txnID)
		if len(versions) == 0 {
			delete(vm.relocations, originalPageID)
		}
	}
}

// Cleanup removes all relocations for versions older than minReaderTxn
// Returns the relocated Page IDs that can now be freed
func (vm *VersionMap) Cleanup(minReaderTxn uint64) []internal.PageID {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	var toFree []internal.PageID

	for originalPageID, versions := range vm.relocations {
		for txnID, relocatedPageID := range versions {
			if txnID < minReaderTxn {
				// No readers need this version anymore
				toFree = append(toFree, relocatedPageID)
				delete(versions, txnID)
			}
		}

		// Remove empty version maps
		if len(versions) == 0 {
			delete(vm.relocations, originalPageID)
		}
	}

	return toFree
}

// Size returns the total number of tracked relocations
func (vm *VersionMap) Size() int {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	total := 0
	for _, versions := range vm.relocations {
		total += len(versions)
	}
	return total
}

Looking at the coupling between Cache, Coordinator, and Tx, here's the refactor plan to make Cache a
pure LRU cache:

Refactor Requirements

1. Cache Simplification (internal/cache/cache.go)

Time box: 2 hours

Current state: MVCC-aware, version tracking, disk I/O coordination
Target state: Simple LRU cache mapping PageID -> Node

Remove from Cache:
- Version tracking (versionEntry, versions map)
- Transaction ID awareness (txnID parameters)
- Disk loading logic (GetOrLoad, loadFromDisk, loadRelocatedVersion)
- Coordinator reference (pager field)
- Load coordination (loadStates, generations)
- Checkpoint/relocation logic (canEvict, EvictCheckpointed, CleanupRelocatedVersions)
- Dirty page flushing (FlushDirty, flushDirtyPage)

New Cache interface:
type Cache struct {
mu       sync.RWMutex
maxSize  int
entries  map[base.PageID]*base.Node
lru      *list.List
// Stats only
hits     atomic.Uint64
misses   atomic.Uint64
evictions atomic.Uint64
}

// Simple operations
func (c *Cache) Get(pageID base.PageID) (*base.Node, bool)
func (c *Cache) Put(pageID base.PageID, node *base.Node)
func (c *Cache) Delete(pageID base.PageID)
func (c *Cache) Evict() // LRU eviction when over capacity

2. Coordinator Version Management (internal/coordinator/coordinator.go)

Time box: 3 hours

Move version management from Cache to Coordinator:

Add to Coordinator:
type NodeVersion struct {
Node  *base.Node
TxnID uint64
}

type Coordinator struct {
// ... existing fields ...

      // Version tracking (moved from cache)
      versionsMu sync.RWMutex
      versions   map[base.PageID][]NodeVersion  // Multiple versions per page

      // Cache reference
      cache *cache.Cache  // Simple LRU cache
}

// New version-aware methods
func (c *Coordinator) GetNode(pageID base.PageID, txnID uint64) (*base.Node, error)
func (c *Coordinator) PutNodeVersion(pageID base.PageID, txnID uint64, node *base.Node)
func (c *Coordinator) LoadNodeWithCache(pageID base.PageID, txnID uint64) (*base.Node, error)

The Coordinator now:
- Maintains version history per page
- Determines which version is visible to a transaction
- Coordinates cache hits/misses
- Handles disk I/O when cache misses
- Manages relocated versions

3. Transaction Layer Updates (tx.go)

Time box: 1 hour

Update Tx to use new Coordinator interface:

Changes to loadNode:
func (tx *Tx) loadNode(pageID base.PageID) (*base.Node, error) {
// Check TX-local pages first
if tx.writable && tx.pages != nil {
if node, exists := tx.pages[pageID]; exists {
return node, nil
}
}

      // Use coordinator's version-aware loading
      return tx.db.coord.GetNode(pageID, tx.txID)
}

4. Commit Flow Refactor

Time box: 2 hours

Update CommitTransaction in Coordinator:
- Keep disk write logic
- Update cache with Put() after writes (no version tracking)
- Maintain version map in Coordinator
- Handle eviction pressure through simple LRU

5. Migration Path

Sequence of changes:

1. Extract version tracking from Cache to VersionMap in Coordinator
2. Simplify Cache.Get/Put to remove txnID parameters
3. Move GetOrLoad logic to Coordinator.LoadNodeWithCache
4. Remove Cache->Coordinator dependency (reverse it)
5. Simplify eviction to pure LRU without checkpoint awareness
6. Update all callers (mainly Tx and DB)

Key Invariants to Maintain

1. MVCC still works - Coordinator handles version visibility
2. Cache coherency - Coordinator ensures cache has current data
3. COW semantics - Tx.pages still provides transaction isolation
4. Concurrent reads - Multiple readers can hit cache simultaneously
5. Single writer - Write coordination unchanged

Testing Strategy

1. Unit test new simple Cache independently
2. Unit test Coordinator's version management
3. Integration test MVCC visibility across transactions
4. Benchmark before/after to ensure no performance regression
5. Concurrent stress test with -race detector

This ships when: All existing tests pass with the refactored cache acting as a pure LRU without
version awareness.

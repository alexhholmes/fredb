# WAL (Write-Ahead Log) Implementation Plan

## Overview

Add Write-Ahead Logging to enable:
- Batched fsyncs (multiple commits per fsync)
- Async checkpointing (decouple commit from disk writes)
- Crash recovery (replay WAL from last checkpoint)

## Current State Analysis

**Key files:**
- `tx.go`: Commit() writes pages directly to disk, calls PutMeta (with fsync)
- `pagemanager.go`: DiskPageManager handles all disk I/O
- `pagecache.go`: Holds dirty pages in memory
- `db.go`: Has background releaser for freelist

**Current commit flow (tx.go:220-300):**
```
1. Lock db.mu
2. Update db.store.root
3. For each page in tx.pages:
   - Serialize(tx.txnID) → Page
   - pager.WritePage(pageID, page) ← DIRECT DISK WRITE
   - cache.Put(pageID, tx.txnID, node)
4. PutMeta(meta) ← includes fsync
```

---

## Architecture with WAL

**Two write paths:**

1. **WAL (durability)**: Append-only log, fsync on every commit (but batched)
2. **Main B-tree file (storage)**: Random writes, async checkpoint

**New commit flow:**
```
┌─────────────────────────────────────────┐
│ Commit (synchronous)                    │
├─────────────────────────────────────────┤
│ 1. Serialize dirty pages                │
│ 2. Append to WAL: [TxnID, PageID, Page]│
│ 3. WAL commit record: [TxnID, COMMIT]  │
│ 4. Fsync WAL ← COMMIT POINT            │
│ 5. Update in-memory cache               │
│ 6. Return success                       │
└─────────────────────────────────────────┘

┌─────────────────────────────────────────┐
│ Checkpoint (async background)           │
├─────────────────────────────────────────┤
│ 1. Pick pages to checkpoint             │
│ 2. Serialize pages (again)              │
│ 3. Write pages to main B-tree file     │
│ 4. Fsync main file                      │
│ 5. Update meta (checkpoint = TxnID X)   │
│ 6. Fsync meta                           │
│ 7. Truncate WAL up to X                 │
└─────────────────────────────────────────┘
```

---

## Phase 1: WAL File Structure (New file: `wal.go`)

### Step 1.1: Define WAL format
```go
// wal.go
package fredb

type WAL struct {
    file   *os.File
    mu     sync.Mutex
    offset int64  // Current write position
}

// Record types
const (
    WALRecordPage   uint8 = 1  // Page write
    WALRecordCommit uint8 = 2  // Commit marker
)

// Record format: [Type:1][TxnID:8][PageID:8][DataLen:4][Data:PageSize]
const WALRecordHeaderSize = 1 + 8 + 8 + 4
```

### Step 1.2: Implement WAL operations
```go
func NewWAL(path string) (*WAL, error) {
    // Open/create .wal file
    // Seek to end to get current offset
}

func (w *WAL) AppendPage(txnID uint64, pageID PageID, page *Page) error {
    // Write: [WALRecordPage][txnID][pageID][PageSize][page.data]
    // Update w.offset
}

func (w *WAL) AppendCommit(txnID uint64) error {
    // Write: [WALRecordCommit][txnID][0][0][]
}

func (w *WAL) Sync() error {
    return w.file.Sync()
}
```

**Files affected:** NEW `wal.go`

---

## Phase 2: Integrate WAL into PageManager

### Step 2.1: Add WAL to DiskPageManager
**File:** `pagemanager.go`

**Line 23-28** - Add WAL field:
```go
type DiskPageManager struct {
    mu       sync.Mutex
    file     *os.File
    meta     MetaPage
    freelist *FreeList
    wal      *WAL  // ← ADD THIS
}
```

### Step 2.2: Open WAL on startup
**File:** `pagemanager.go`
**Line 31-65** - Modify `NewDiskPageManager`:

```go
func NewDiskPageManager(path string) (*DiskPageManager, error) {
    // ... existing file open code ...

    dm := &DiskPageManager{
        file:     file,
        freelist: NewFreeList(),
    }

    // Open WAL
    walPath := path + ".wal"
    wal, err := NewWAL(walPath)
    if err != nil {
        file.Close()
        return nil, err
    }
    dm.wal = wal

    // ... rest of initialization ...

    if info.Size() == 0 {
        // New database
        if err := dm.initializeNewDB(); err != nil {
            wal.Close()
            file.Close()
            return nil, err
        }
    } else {
        // Load existing + replay WAL
        if err := dm.loadExistingDB(); err != nil {
            wal.Close()
            file.Close()
            return nil, err
        }
        // ← ADD WAL REPLAY HERE (Phase 5)
    }

    return dm, nil
}
```

### Step 2.3: Add WAL methods to PageManager
**File:** `pagemanager.go`
**After line 108** - Add new methods:

```go
// WALAppendPage writes a page to the WAL
func (dm *DiskPageManager) WALAppendPage(txnID uint64, pageID PageID, page *Page) error {
    return dm.wal.AppendPage(txnID, pageID, page)
}

// WALCommit writes a commit marker to the WAL
func (dm *DiskPageManager) WALCommit(txnID uint64) error {
    return dm.wal.AppendCommit(txnID)
}

// WALSync fsyncs the WAL
func (dm *DiskPageManager) WALSync() error {
    return dm.wal.Sync()
}
```

**Files affected:** `pagemanager.go`

---

## Phase 3: Modify Transaction Commit to Use WAL

### Step 3.1: Change tx.Commit() to write to WAL
**File:** `tx.go`
**Lines 242-264** - Replace direct disk writes:

**BEFORE:**
```go
// Write all TX-local pages to disk and flush to global cache
for pageID, node := range tx.pages {
    // Serialize node to a page with this transaction's ID
    page, err := node.Serialize(tx.txnID)
    if err != nil {
        tx.db.store.root = oldRoot
        return err
    }

    // Write to disk
    if err := tx.db.store.pager.WritePage(pageID, page); err != nil {
        tx.db.store.root = oldRoot
        return err
    }

    node.dirty = false
    tx.db.store.cache.Put(pageID, tx.txnID, node)
}
```

**AFTER:**
```go
// Write all TX-local pages to WAL (not disk!)
for pageID, node := range tx.pages {
    page, err := node.Serialize(tx.txnID)
    if err != nil {
        tx.db.store.root = oldRoot
        return err
    }

    // Write to WAL instead of disk
    if dm, ok := tx.db.store.pager.(*DiskPageManager); ok {
        if err := dm.WALAppendPage(tx.txnID, pageID, page); err != nil {
            tx.db.store.root = oldRoot
            return err
        }
    }

    node.dirty = false  // Still mark clean (in cache)
    tx.db.store.cache.Put(pageID, tx.txnID, node)
}

// Append commit marker to WAL
if dm, ok := tx.db.store.pager.(*DiskPageManager); ok {
    if err := dm.WALCommit(tx.txnID); err != nil {
        tx.db.store.root = oldRoot
        return err
    }

    // Fsync WAL (this is the commit point!)
    if err := dm.WALSync(); err != nil {
        tx.db.store.root = oldRoot
        return err
    }
}
```

### Step 3.2: Remove fsync from PutMeta
**File:** `pagemanager.go`
**Lines 140-144** - Remove fsync:

```go
// TODO: Meta page will be fsynced during checkpoint, not on every commit
// if err := dm.file.Sync(); err != nil {
//     return err
// }
```

**Files affected:** `tx.go`, `pagemanager.go`

---

## Phase 4: Add MetaPage Checkpoint Field

### Step 4.1: Add checkpoint marker to MetaPage
**File:** `meta.go` (or wherever MetaPage is defined)

```go
type MetaPage struct {
    Magic              uint32
    Version            uint32
    PageSize           uint32
    RootPageID         PageID
    FreelistID         PageID
    FreelistPages      uint64
    TxnID              uint64
    CheckpointTxnID    uint64  // ← ADD THIS: Last TxnID fully checkpointed
    NumPages           uint64
    Checksum           uint32
}
```

Update `CalculateChecksum()` to include new field.

**Files affected:** `meta.go`

---

## Phase 5: Background Checkpointer

### Step 5.1: Add checkpointer to db.go
**File:** `db.go`
**After line 75** - Add checkpointer goroutine:

```go
func (d *db) startCheckpointer() {
    go d.backgroundCheckpointer()
}

func (d *db) backgroundCheckpointer() {
    ticker := time.NewTicker(5 * time.Second)  // Checkpoint every 5s
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            d.checkpoint()
        case <-d.closeC:  // Need to add closeC channel
            return
        }
    }
}
```

### Step 5.2: Implement checkpoint logic
**File:** `db.go`
**Add new method:**

```go
func (d *db) checkpoint() error {
    d.mu.Lock()
    defer d.mu.Unlock()

    dm, ok := d.store.pager.(*DiskPageManager)
    if !ok {
        return nil  // In-memory, skip
    }

    // Get current TxnID (everything up to this is committed in WAL)
    currentTxn := dm.GetMeta().TxnID

    // Flush all dirty cached pages to main file
    if err := d.store.cache.FlushDirty(&d.store.pager); err != nil {
        return err
    }

    // Fsync main B-tree file
    dm.mu.Lock()
    if err := dm.file.Sync(); err != nil {
        dm.mu.Unlock()
        return err
    }
    dm.mu.Unlock()

    // Update meta with checkpoint marker
    meta := dm.GetMeta()
    meta.CheckpointTxnID = currentTxn
    if err := dm.PutMeta(meta); err != nil {
        return err
    }

    // Fsync meta page
    dm.mu.Lock()
    err := dm.file.Sync()
    dm.mu.Unlock()
    if err != nil {
        return err
    }

    // Truncate WAL up to checkpoint
    if err := dm.WALTruncate(currentTxn); err != nil {
        return err
    }

    return nil
}
```

### Step 5.3: Call checkpointer on startup
**File:** `db.go`
**In `Open()` function, after opening DB:**

```go
func Open(path string) (*DB, error) {
    // ... existing code ...

    d.startCheckpointer()  // ← ADD THIS

    return db, nil
}
```

### Step 5.4: Add WAL truncate
**File:** `wal.go`

```go
func (w *WAL) Truncate(upToTxnID uint64) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    // Find offset in WAL where txnID > upToTxnID
    // Truncate file at that offset
    // (Implementation details: scan WAL, find commit markers)
}
```

**Files affected:** `db.go`, `wal.go`

---

## Phase 5.5: Version-Aware Cache Eviction

### Problem Statement

**After Phase 3, tx.Commit() marks pages as `dirty = false` after WAL write:**
```go
dm.AppendPageWAL(txnID, pageID, page)  // Write to WAL
node.dirty = false                      // ← Mark clean BUT NOT ON DISK YET
cache.Put(pageID, tx.txnID, node)      // Add to cache
```

**If cache evicts this node before checkpoint:**
1. Cache miss triggers `ReadPage(pageID)` from disk
2. Disk has **old version** (pre-checkpoint)
3. Serve stale data ❌ **CORRECTNESS BUG**

### Solution: Checkpoint-Aware Eviction

**Core principle:** Only evict page versions that have been checkpointed to disk.

```
Checkpoint boundary divides versions:
────────────────────────────────────────────────
TxnID:  1    2    3    4    5    6    7    8
        ├────┼────┼────┼────┼────┼────┼────┤
        │    │    │    │    │    │    │    │
             CheckpointTxnID = 4
             │              │
        ─────┴──────────────┴─────────────────
        Can evict           Must keep in cache
        (on disk)           (only in WAL)
```

**Eviction policy:**
- Page versions with `txnID ≤ CheckpointTxnID` → **safe to evict** (on disk)
- Page versions with `txnID > CheckpointTxnID` → **must keep** (only in WAL)

### Step 5.5.1: Add PageManager reference to cache

**File:** `pagecache.go`

**Current structure:**
```go
type PageCache struct {
    mu    sync.RWMutex
    cache map[PageID]map[uint64]*Node  // pageID -> (txnID -> Node)
}
```

**Updated structure:**
```go
type PageCache struct {
    mu    sync.RWMutex
    cache map[PageID]map[uint64]*Node  // pageID -> (txnID -> Node)
    pager PageManager                   // ← ADD: Need to query CheckpointTxnID
}
```

**Constructor update:**
```go
func NewPageCache(pager PageManager) *PageCache {
    return &PageCache{
        cache: make(map[PageID]map[uint64]*Node),
        pager: pager,  // ← Pass pager reference
    }
}
```

### Step 5.5.2: Implement checkpoint-aware eviction

**File:** `pagecache.go`

**Add helper method:**
```go
// canEvict returns true if a page version is safe to evict
// A version is safe to evict if it has been checkpointed to disk
func (c *PageCache) canEvict(txnID uint64) bool {
    meta := c.pager.GetMeta()
    return txnID <= meta.CheckpointTxnID
}
```

**Evict old versions after checkpoint:**
```go
// EvictCheckpointed removes page versions that have been checkpointed
// Called by background checkpointer after checkpoint completes
func (c *PageCache) EvictCheckpointed() int {
    c.mu.Lock()
    defer c.mu.Unlock()

    meta := c.pager.GetMeta()
    checkpointTxn := meta.CheckpointTxnID
    evicted := 0

    for pageID, versions := range c.cache {
        for txnID, _ := range versions {
            if txnID <= checkpointTxn {
                delete(versions, txnID)
                evicted++
            }
        }

        // Remove empty version maps
        if len(versions) == 0 {
            delete(c.cache, pageID)
        }
    }

    return evicted
}
```

**Optional: LRU eviction when cache is full:**
```go
// evictLRU evicts the oldest checkpointed version when cache is full
// Only evicts versions with txnID <= CheckpointTxnID
func (c *PageCache) evictLRU() bool {
    meta := c.pager.GetMeta()
    checkpointTxn := meta.CheckpointTxnID

    var oldestTxn uint64 = math.MaxUint64
    var evictPageID PageID
    var evictTxnID uint64
    found := false

    // Find oldest checkpointed version
    for pageID, versions := range c.cache {
        for txnID := range versions {
            if txnID <= checkpointTxn && txnID < oldestTxn {
                oldestTxn = txnID
                evictPageID = pageID
                evictTxnID = txnID
                found = true
            }
        }
    }

    if found {
        delete(c.cache[evictPageID], evictTxnID)
        if len(c.cache[evictPageID]) == 0 {
            delete(c.cache, evictPageID)
        }
        return true
    }

    return false  // No evictable versions (all in WAL only)
}
```

### Step 5.5.3: Call EvictCheckpointed after checkpoint

**File:** `db.go`
**In `checkpoint()` method, after truncating WAL:**

```go
func (d *db) checkpoint() error {
    // ... existing checkpoint logic ...

    // Truncate WAL up to checkpoint
    if err := dm.TruncateWAL(currentTxn); err != nil {
        return err
    }

    // Evict checkpointed versions from cache
    // These versions are now on disk and safe to remove from memory
    evicted := d.store.cache.EvictCheckpointed()
    // Optional: log eviction count for monitoring

    return nil
}
```

### Step 5.5.4: Update BTree store initialization

**File:** `btree.go` (or wherever BTreeStore is created)

**Update constructor to pass pager to cache:**
```go
func newBTreeStore(pager PageManager) *BTreeStore {
    return &BTreeStore{
        pager: pager,
        cache: NewPageCache(pager),  // ← Pass pager reference
        root:  nil,
    }
}
```

### Edge Cases

**1. Cache miss on non-checkpointed version:**
- Version only in WAL, not on disk
- Cache miss → disk read gives old version
- **Prevention:** Never evict `txnID > CheckpointTxnID`

**2. Checkpoint in progress during read:**
- Reader starts with `txnID = 5`, checkpoint advances to 6
- Reader's version (5) might get evicted
- **Solution:** MVCC isolation - reader holds reference until done

**3. Unbounded cache growth:**
- Slow checkpoint → many un-evictable versions accumulate
- **Mitigation:** Checkpoint interval (5s) limits growth
- **Future:** Back-pressure on writes if cache too large

### Testing Strategy

**Test 1: Eviction respects checkpoint boundary**
```go
// Commit txn 1, 2, 3
// Checkpoint (CheckpointTxnID = 3)
// Evict
// Verify: txns 1-3 evicted, later txns retained
```

**Test 2: Cache miss after eviction returns correct data**
```go
// Commit txn with key="foo" val="bar"
// Checkpoint
// Evict from cache
// Read key="foo" → should return "bar" from disk
```

**Test 3: Un-checkpointed versions not evictable**
```go
// Commit txn 1, 2, 3 (CheckpointTxnID = 0)
// Attempt evict
// Verify: Nothing evicted (all in WAL only)
```

**Files affected:** `pagecache.go`, `db.go`, `btree.go`

---

## Phase 6: WAL Recovery

### Step 6.1: Implement WAL replay
**File:** `wal.go`

```go
type WALRecord struct {
    Type   uint8
    TxnID  uint64
    PageID PageID
    Page   *Page
}

func (w *WAL) Replay(fromTxnID uint64, applyFn func(PageID, *Page) error) error {
    // Scan WAL from beginning
    // For each committed transaction with TxnID > fromTxnID:
    //   - Read all page records
    //   - Call applyFn(pageID, page) for each page
    // Skip uncommitted transactions (no commit marker found)
}
```

### Step 6.2: Call replay on startup
**File:** `pagemanager.go`
**In `loadExistingDB()`, after loading freelist:**

```go
func (dm *DiskPageManager) loadExistingDB() error {
    // ... existing meta/freelist load code ...

    // Replay WAL from last checkpoint
    checkpointTxn := dm.meta.CheckpointTxnID

    err = dm.wal.Replay(checkpointTxn, func(pageID PageID, page *Page) error {
        return dm.writePageAtUnsafe(pageID, page)
    })
    if err != nil {
        return fmt.Errorf("WAL replay failed: %w", err)
    }

    // Update in-memory TxnID to max seen in WAL
    // (WAL might have commits beyond what's in meta)

    return nil
}
```

**Files affected:** `wal.go`, `pagemanager.go`

---

## Phase 7: Testing & Edge Cases

### Step 7.1: Add WAL close to db.Close()
**File:** `db.go`

```go
func (d *db) Close() error {
    close(d.closeC)  // Stop checkpointer

    // Final checkpoint
    d.checkpoint()

    return d.store.Close()
}
```

**File:** `pagemanager.go` - Add to Close():
```go
func (dm *DiskPageManager) Close() error {
    // ... existing code ...

    if dm.wal != nil {
        dm.wal.Close()
    }

    return dm.file.Close()
}
```

### Step 7.2: Handle crash scenarios
**Test cases to add:**
1. Crash during commit (before WAL fsync) → should lose transaction
2. Crash after WAL fsync, before checkpoint → should replay from WAL
3. Crash during checkpoint → should replay partial checkpoint from WAL

**Files affected:** `db.go`, `pagemanager.go`, test files

---

## Summary of File Changes

| File | Lines Changed | Description |
|------|---------------|-------------|
| **wal.go** | NEW FILE | WAL implementation |
| **pagemanager.go** | 23-28, 31-65, +108 | Add WAL field, open WAL, add WAL methods |
| **tx.go** | 242-264 | Replace WritePage with WALAppend |
| **meta.go** | MetaPage struct | Add CheckpointTxnID field |
| **db.go** | +75, +new methods | Add checkpointer goroutine |
| **pagecache.go** | Unchanged | FlushDirty already exists |

## Implementation Order

1. Create `wal.go` with basic structure
2. Add CheckpointTxnID to MetaPage
3. Integrate WAL into DiskPageManager
4. Modify tx.Commit() to use WAL
5. Implement checkpoint() in db.go
6. Implement WAL.Replay() for recovery
7. Add tests for crash scenarios

## Key Design Decisions

**Why serialize twice (WAL + checkpoint)?**
- WAL needs immediate durability (can't wait for cache)
- Checkpoint uses cached pages (might be modified by later txns)
- Trade-off: CPU (serialize twice) vs simplicity

**Why not remove dirty pages from cache after checkpoint?**
- MVCC readers might need old versions
- Cache eviction handles this via LRU + version tracking

**Why fsync WAL on every commit?**
- Append-only = sequential writes (fast even with fsync)
- Can batch: multiple commits → single fsync
- Future: group commit (batch WAL appends)

**Recovery guarantees:**
- Committed transaction (WAL fsynced) → always recovered
- Uncommitted transaction (no commit marker) → always discarded
- Checkpointed transaction → in main file, truncated from WAL
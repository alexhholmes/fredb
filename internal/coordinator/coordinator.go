package coordinator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/freelist"
	"fredb/internal/storage"
)

// SyncMode controls when to fsync (copied from main package to avoid import cycle)
type SyncMode int

const (
	SyncEveryCommit SyncMode = iota
	SyncOff
)

// Coordinator coordinates store, cache, meta, and freelist
type Coordinator struct {
	cache *cache.Cache     // Simple LRU cache
	store *storage.Storage // File I/O backend

	// Dual meta pages for atomic writes visible to readers stored at page IDs 0 and 1
	active atomic.Pointer[base.Snapshot]
	meta0  base.Snapshot
	meta1  base.Snapshot

	// Page allocation tracking (separate from meta to avoid data races)
	pages atomic.Uint64 // Total pages allocated (includes uncommitted allocations)

	// Freelist management (owns its own mutex)
	freelist *freelist.Freelist

	// Bucket reference count tracking
	// This tracks number of references to root pages of buckets by transactions.
	// The deleted map tracks which buckets have been deleted and can be cleaned
	// up since the last transaction that referenced them has completed.
	buckets   sync.Map                 // Bucket root PageID -> atomic.Int32 ref count
	DeletedMu sync.RWMutex             // Protects Deleted map
	Deleted   map[base.PageID]struct{} // Buckets pending deletion, 0 ref count
}

// NewCoordinator creates a coordinator with injected dependencies
func NewCoordinator(storage *storage.Storage, cache *cache.Cache) (*Coordinator, error) {
	pm := &Coordinator{
		store:    storage,
		cache:    cache,
		freelist: freelist.New(),
		Deleted:  make(map[base.PageID]struct{}),
	}

	// Check if new file (empty)
	empty, err := storage.Empty()
	if err != nil {
		return nil, err
	} else if empty {
		// New database - initialize
		if err := pm.initializeNewDB(); err != nil {
			return nil, err
		}
	} else {
		// Existing database - load meta and freelist
		if err := pm.loadExistingDB(); err != nil {
			return nil, err
		}
	}

	return pm, nil
}

// AssignPageID allocates a new Page (from freelist or grows file)
func (c *Coordinator) AssignPageID() (base.PageID, error) {
	// Try freelist first
	id := c.freelist.Allocate()
	if id != 0 {
		return id, nil
	}

	// Grow file - use atomic pages counter (includes uncommitted allocations)
	// Atomically increment and get the new page ID
	id = base.PageID(c.pages.Add(1) - 1)

	return id, nil
}

// FreePage adds a Page to the freelist
func (c *Coordinator) FreePage(id base.PageID) error {
	c.freelist.Free(id)
	return nil
}

// FreePending adds pages to the pending freelist at the given transaction ID
func (c *Coordinator) FreePending(txnID uint64, pageIDs []base.PageID) error {
	c.freelist.Pending(txnID, pageIDs)
	return nil
}

// ReleasePages moves pages from pending to free for all transactions < minTxnID
func (c *Coordinator) ReleasePages(minTxnID uint64) int {
	return c.freelist.Release(minTxnID)
}

// GetMeta returns the current metadata
func (c *Coordinator) GetMeta() base.MetaPage {
	return c.active.Load().Meta
}

// GetSnapshot returns a COPY of the bundled metadata and root pointer atomically
// Returns by value to prevent data races with concurrent PutSnapshot updates
func (c *Coordinator) GetSnapshot() base.Snapshot {
	return *c.active.Load()
}

// PutSnapshot updates the metadata and root pointer, persists metadata to disk
// Does NOT make it visible to readers - call CommitSnapshot after fsync
func (c *Coordinator) PutSnapshot(meta base.MetaPage, root *base.Node) error {
	// Sync NumPages from atomic counter (may have uncommitted allocations)
	meta.NumPages = c.pages.Load()

	// Update checksum (after NumPages sync)
	meta.Checksum = meta.CalculateChecksum()

	// Determine which page to write to based on TxID
	metaPageID := base.PageID(meta.TxID % 2)

	// Write to disk
	buf := c.store.GetBuffer()
	defer c.store.PutBuffer(buf)
	metaPage := (*base.Page)(unsafe.Pointer(&buf[0]))
	metaPage.WriteMeta(&meta)
	if err := c.store.WritePage(metaPageID, metaPage); err != nil {
		return err
	}

	// Update inactive in-memory copy with both meta and root (don't swap pointer yet)
	if metaPageID == 0 {
		c.meta0.Meta = meta
		c.meta0.Root = root
	} else {
		c.meta1.Meta = meta
		c.meta1.Root = root
	}

	return nil
}

// CommitSnapshot atomically makes the last PutSnapshot visible to readers
// Call AFTER fsync to ensure durability before visibility
// Lock-free: single writer guarantee + atomic swap
func (c *Coordinator) CommitSnapshot() {
	// Swap to the meta with higher or equal TxID
	// Use >= to handle initial case where both metas have TxID=0
	if c.meta0.Meta.TxID >= c.meta1.Meta.TxID {
		c.active.Swap(&c.meta0)
	} else {
		c.active.Swap(&c.meta1)
	}
}

// Close serializes freelist to disk and closes the file
func (c *Coordinator) Close() error {
	// Get active meta for reading
	meta := c.active.Load().Meta

	// Serialize freelist to disk
	pagesNeeded := c.freelist.PagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxID ensures they're only released after this close() completes
		oldPages := make([]base.PageID, meta.FreelistPages)
		for i := uint64(0); i < meta.FreelistPages; i++ {
			oldPages[i] = meta.FreelistID + base.PageID(i)
		}
		c.freelist.Pending(meta.TxID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = c.freelist.PagesNeeded()

		// Move freelist to new pages at end of file
		meta.FreelistID = base.PageID(meta.NumPages)
		meta.FreelistPages = uint64(pagesNeeded)
		meta.NumPages += uint64(pagesNeeded)
	}

	// Write freelist
	freelistPages := make([]*base.Page, pagesNeeded)
	for i := 0; i < pagesNeeded; i++ {
		freelistPages[i] = &base.Page{}
	}
	c.freelist.Serialize(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := c.store.WritePage(meta.FreelistID+base.PageID(i),
			freelistPages[i]); err != nil {
			return err
		}
	}

	// Update meta (increment TxID, recalculate checksum)
	meta.TxID++
	meta.Checksum = meta.CalculateChecksum()

	// Determine which metapage to write to
	metaPageID := base.PageID(meta.TxID % 2)

	// Write meta to disk
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)
	if err := c.store.WritePage(metaPageID, metaPage); err != nil {
		return err
	}

	// Update in-memory and swap pointer
	if metaPageID == 0 {
		c.meta0.Meta = meta
		c.active.Swap(&c.meta0)
	} else {
		c.meta1.Meta = meta
		c.active.Swap(&c.meta1)
	}

	return c.store.Close()
}

// initializeNewDB creates a new database with dual meta pages and empty freelist
func (c *Coordinator) initializeNewDB() error {
	// Initialize meta
	meta := base.MetaPage{
		Magic:           base.MagicNumber,
		Version:         base.FormatVersion,
		PageSize:        base.PageSize,
		RootPageID:      0, // Will be set by BTree
		FreelistID:      2, // Page 2
		FreelistPages:   1, // One freelist Page
		TxID:            0, // First transaction
		CheckpointTxnID: 0, // No checkpoint yet
		NumPages:        3, // Pages 0-1 (meta), 2 (freelist) reserved
	}
	meta.Checksum = meta.CalculateChecksum()

	// Set both in-memory copies with Meta and Root (Root is nil initially)
	c.meta0.Meta = meta
	c.meta0.Root = nil
	c.meta1.Meta = meta
	c.meta1.Root = nil
	c.active.Store(&c.meta0)

	// Initialize atomic pages counter
	c.pages.Store(meta.NumPages)

	// Write meta to both disk pages 0 and 1
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)

	if err := c.store.WritePage(0, metaPage); err != nil {
		return err
	}
	if err := c.store.WritePage(1, metaPage); err != nil {
		return err
	}

	// Write empty freelist to Page 2
	freelistPages := []*base.Page{&base.Page{}}
	c.freelist.Serialize(freelistPages)
	if err := c.store.WritePage(2, freelistPages[0]); err != nil {
		return err
	}

	// Fsync to ensure durability
	return c.store.Sync()
}

// loadExistingDB loads meta and freelist from existing database file
func (c *Coordinator) loadExistingDB() error {
	// Read both meta pages
	page0, err := c.store.ReadPage(0)
	if err != nil {
		return err
	}
	page1, err := c.store.ReadPage(1)
	if err != nil {
		return err
	}

	// Validate and pick the best meta Page
	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	err0 := meta0.Validate()
	err1 := meta1.Validate()

	// Both invalid - corrupted database
	if err0 != nil && err1 != nil {
		return fmt.Errorf("both meta pages corrupted: %v, %v", err0, err1)
	}

	// Set both in-memory copies with .Meta and .Root (Root will be loaded by DB later)
	if err0 == nil {
		c.meta0.Meta = *meta0
		c.meta0.Root = nil // Will be set by DB.Open()
	}
	if err1 == nil {
		c.meta1.Meta = *meta1
		c.meta1.Root = nil // Will be set by DB.Open()
	}

	// Point active to the one with higher TxID
	if err0 != nil {
		c.active.Store(&c.meta1)
	} else if err1 != nil {
		c.active.Store(&c.meta0)
	} else {
		// Both valid - pick highest TxID
		if meta0.TxID > meta1.TxID {
			c.active.Store(&c.meta0)
		} else {
			c.active.Store(&c.meta1)
		}
	}

	// Load freelist using active meta
	activeMeta := c.active.Load()
	freelistPages := make([]*base.Page, activeMeta.Meta.FreelistPages)
	for i := uint64(0); i < activeMeta.Meta.FreelistPages; i++ {
		page, err := c.store.ReadPage(activeMeta.Meta.FreelistID + base.PageID(i))
		if err != nil {
			return err
		}
		freelistPages[i] = page
	}
	c.freelist.Deserialize(freelistPages)

	// Release any pending pages that are safe to reclaim
	// On startup, no readers exist, so all pending pages with txnID <= current can be released
	c.freelist.Release(activeMeta.Meta.TxID)

	// Initialize atomic pages counter from active meta
	c.pages.Store(activeMeta.Meta.NumPages)

	return nil
}

// GetNode retrieves a node, checking cache first then loading from disk.
func (c *Coordinator) GetNode(pageID base.PageID) (*base.Node, error) {
	// Check cache first
	if node, hit := c.cache.Get(pageID); hit {
		return node, nil
	}

	// Load from disk
	page, err := c.store.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	// Return buffer to pool after deserialization (DirectIO only)
	// MMap mode allocates non-pooled, non-aligned buffers
	if c.store.GetMode() == storage.DirectIO {
		defer func() {
			buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)
			c.store.PutBuffer(buf)
		}()
	}

	node := &base.Node{
		PageID: pageID,
		Dirty:  false,
	}

	if err = node.Deserialize(page); err != nil {
		return nil, err
	}

	// Cache and return
	c.cache.Put(pageID, node)
	return node, nil
}

// LoadNode loads a node, coordinating cache and disk I/O.
// Routes TX calls through Coordinator instead of direct cache access.
func (c *Coordinator) LoadNode(pageID base.PageID) (*base.Node, bool) {
	node, err := c.GetNode(pageID)
	if err != nil {
		return nil, false
	}
	return node, true
}

// FlushNode serializes and writes a dirty node to disk.
// Used by cache to flush dirty pages during eviction or checkpoint.
// Returns error if serialization or write fails.
func (c *Coordinator) FlushNode(node *base.Node, txnID uint64, pageID base.PageID) error {
	buf := c.store.GetBuffer()
	defer c.store.PutBuffer(buf)
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	err := node.Serialize(txnID, page)
	if err != nil {
		return err
	}

	return c.store.WritePage(pageID, page)
}

// MapVirtualPageIDs allocates real page IDs for all virtual pages and updates node references.
// This is phase 1 of commit: mapping virtual IDs to real IDs without writing to disk.
// Returns a map of virtual->real page ID mappings.
// Caller must hold db.mu.
func (c *Coordinator) MapVirtualPageIDs(
	pages map[base.PageID]*base.Node,
	root *base.Node,
) (map[base.PageID]base.PageID, error) {
	// Pass 1: Allocate real page IDs for all virtual pages
	virtualToReal := make(map[base.PageID]base.PageID)

	for pageID := range pages {
		if int64(pageID) < 0 { // Virtual page ID
			realPageID, err := c.AssignPageID()
			if err != nil {
				// Rollback partial allocation
				for _, allocated := range virtualToReal {
					_ = c.FreePage(allocated)
				}
				return nil, err
			}
			virtualToReal[pageID] = realPageID
		}
	}

	// Pass 2: Update nodes' PageID fields and remap child pointers
	for pageID, node := range pages {
		// Update this node's PageID if map key was virtual
		if int64(pageID) < 0 {
			if realID, exists := virtualToReal[pageID]; exists {
				node.PageID = realID
			}
		}

		// Defensive check for node.PageID itself
		if int64(node.PageID) < 0 {
			if realID, exists := virtualToReal[node.PageID]; exists {
				node.PageID = realID
			}
		}

		// Remap child pointers in branch nodes
		if !node.IsLeaf() {
			for i, childID := range node.Children {
				if realID, isVirtual := virtualToReal[childID]; isVirtual {
					node.Children[i] = realID
				}
			}
		}
	}

	// Pass 3: Handle root separately
	if root != nil && int64(root.PageID) < 0 {
		realPageID, err := c.AssignPageID()
		if err != nil {
			// Rollback
			for _, allocated := range virtualToReal {
				_ = c.FreePage(allocated)
			}
			return nil, err
		}
		virtualToReal[root.PageID] = realPageID
		root.PageID = realPageID
	}

	// Remap root's children pointers
	if root != nil && !root.IsLeaf() {
		for i, childID := range root.Children {
			if realID, isVirtual := virtualToReal[childID]; isVirtual {
				root.Children[i] = realID
			}
		}
	}

	return virtualToReal, nil
}

// WriteTransaction writes all pages to disk, handles freed pages, updates metadata, and syncs.
// This is phase 2 of commit: writing pages with real IDs to disk.
// Caller must hold db.mu.
func (c *Coordinator) WriteTransaction(
	pages map[base.PageID]*base.Node,
	root *base.Node,
	freed map[base.PageID]struct{},
	txnID uint64,
	syncMode SyncMode,
) error {
	// Write all pages to disk
	buf := c.store.GetBuffer()
	defer c.store.PutBuffer(buf)
	for _, node := range pages {
		page := (*base.Page)(unsafe.Pointer(&buf[0]))
		if err := node.Serialize(txnID, page); err != nil {
			return err
		}

		if err := c.store.WritePage(node.PageID, page); err != nil {
			return err
		}

		node.Dirty = false
		c.cache.Put(node.PageID, node)
	}

	// Write root separately if dirty
	if root != nil && root.Dirty {
		page := (*base.Page)(unsafe.Pointer(&buf[0]))
		err := root.Serialize(txnID, page)
		if err != nil {
			return err
		}

		if err = c.store.WritePage(root.PageID, page); err != nil {
			return err
		}

		root.Dirty = false
		c.cache.Put(root.PageID, root)
	}

	// Add freed pages to pending
	if len(freed) > 0 {
		freedSlice := make([]base.PageID, 0, len(freed))
		for pageID := range freed {
			freedSlice = append(freedSlice, pageID)
		}
		if err := c.FreePending(txnID, freedSlice); err != nil {
			return err
		}
	}

	// Update meta
	meta := c.active.Load().Meta
	if root != nil {
		meta.RootPageID = root.PageID
	}
	meta.TxID = txnID
	meta.Checksum = meta.CalculateChecksum()

	if err := c.PutSnapshot(meta, root); err != nil {
		return err
	}

	// Conditional sync (this is the commit point!)
	if syncMode == SyncEveryCommit {
		if err := c.store.Sync(); err != nil {
			return err
		}
	}

	// Make meta visible to readers atomically
	c.CommitSnapshot()

	return nil
}

// CommitTransaction coordinates the full transaction commit (legacy compatibility).
// This combines MapVirtualPageIDs and WriteTransaction into a single operation.
// Preserved for backward compatibility.
func (c *Coordinator) CommitTransaction(
	pages map[base.PageID]*base.Node,
	root *base.Node,
	freed map[base.PageID]struct{},
	txnID uint64,
	syncMode SyncMode,
) error {
	// Phase 1: Map virtual to real page IDs
	_, err := c.MapVirtualPageIDs(pages, root)
	if err != nil {
		return err
	}

	// Phase 2: Write everything to disk
	return c.WriteTransaction(pages, root, freed, txnID, syncMode)
}

// AcquireBucket increments the reference count for a bucket's root page.
// Returns false if the bucket is marked for deletion (new transactions cannot access it).
// Returns true if successfully acquired.
func (c *Coordinator) AcquireBucket(rootID base.PageID) bool {
	// Quick check before acquiring lock
	if _, deleted := c.Deleted[rootID]; deleted {
		return false // Bucket is pending deletion, reject new access
	}

	c.DeletedMu.RLock()
	defer c.DeletedMu.RUnlock()

	// Check if bucket is marked for deletion WHILE holding read lock
	if _, deleted := c.Deleted[rootID]; deleted {
		return false // Bucket is pending deletion, reject new access
	}

	// Increment reference count while still holding the lock
	// This prevents the bucket from being marked deleted between check and increment
	counter := &atomic.Int32{}
	counter.Store(1)
	refCount, loaded := c.buckets.LoadOrStore(rootID, counter)
	if loaded {
		// We hold a reference to the atomic
		refCount.(*atomic.Int32).Add(1)
		if currentRefCount, ok := c.buckets.Load(rootID); !ok || currentRefCount != refCount {
			// In case the bucket was deleted in between loading it and incrementing
			// the ref count. The reference to this atomic does not matter since
			// we are rejecting access anyway in ReleaseBucket (in another goroutine).
			return false
		}
	}

	return true
}

// ReleaseBucket decrements the reference count for a bucket's root page.
// If the count reaches 0 and the bucket is marked for deletion, triggers background cleanup.
// The cleanup callback is provided by the caller (typically db.freeTree).
func (c *Coordinator) ReleaseBucket(rootID base.PageID, cleanupFunc func(base.PageID) error) {
	// Decrement reference count
	refCountVal, exists := c.buckets.Load(rootID)
	if !exists {
		panic("ref count decrement of bucket count that does not exist") // Should not occur
	}

	refCount := refCountVal.(*atomic.Int32)
	newCount := refCount.Add(-1)

	// If count reaches 0, check if this bucket is marked for deletion
	if newCount == 0 {
		c.DeletedMu.Lock()
		_, shouldDelete := c.Deleted[rootID]
		if shouldDelete {
			// Remove from deleted map
			delete(c.Deleted, rootID)
			c.DeletedMu.Unlock()

			// Trigger background cleanup
			go func() {
				_ = cleanupFunc(rootID)
			}()
		} else {
			c.DeletedMu.Unlock()
		}

		// Clean up empty reference count entry
		c.buckets.Delete(rootID)
	}
}

type Stats struct {
	Cache      cache.Stats
	Store      storage.Stats
	FreedPages int
}

// Stats returns disk I/O statistics
func (c *Coordinator) Stats() Stats {
	return Stats{
		Cache:      c.cache.Stats(),
		Store:      c.store.Stats(),
		FreedPages: c.freelist.Stats(),
	}
}

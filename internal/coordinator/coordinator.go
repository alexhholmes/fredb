package coordinator

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/storage"
)

// Coordinator coordinates storage, cache, meta, and freelist
type Coordinator struct {
	mu      sync.Mutex       // Protects meta and freelist access
	cache   *cache.Cache     // Simple LRU cache
	storage *storage.Storage // File I/O backend

	// Dual meta pages for atomic writes visible to readers stored at page IDs 0 and 1
	activeMeta atomic.Pointer[base.Snapshot]
	meta0      base.Snapshot
	meta1      base.Snapshot

	// Freelist tracking
	freedPages   []base.PageID
	pendingPages map[uint64][]base.PageID // txnID -> pages freed at that transaction
	preventAlloc uint64                   // temporarily prevent allocation of pages freed up to this txnID (0 = no prevention)

	// Page allocation tracking (separate from meta to avoid data races)
	numPages atomic.Uint64 // Total pages allocated (includes uncommitted allocations)
}

// NewCoordinator creates a coordinator with injected dependencies
func NewCoordinator(storage *storage.Storage, cache *cache.Cache) (*Coordinator, error) {
	pm := &Coordinator{
		storage:      storage,
		freedPages:   make([]base.PageID, 0),
		pendingPages: make(map[uint64][]base.PageID),
		cache:        cache,
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

// ReadPage reads a Page from disk.
// Used during checkpoint to read old disk versions before overwriting.
func (c *Coordinator) ReadPage(id base.PageID) (*base.Page, error) {
	return c.storage.ReadPage(id)
}

// WritePage writes a Page to a specific offset (with locking)
func (c *Coordinator) WritePage(id base.PageID, page *base.Page) error {
	return c.storage.WritePage(id, page)
}

// Sync flushes any buffered writes to disk
func (c *Coordinator) Sync() error {
	return c.storage.Sync()
}

// AllocatePage allocates a new Page (from freelist or grows file)
func (c *Coordinator) AllocatePage() (base.PageID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Try freelist first
	id := c.allocate()
	if id != 0 {
		return id, nil
	}

	// Grow file - use atomic numPages counter (includes uncommitted allocations)
	// Atomically increment and get the new page ID
	id = base.PageID(c.numPages.Add(1) - 1)

	return id, nil
}

// FreePage adds a Page to the freelist
func (c *Coordinator) FreePage(id base.PageID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.free(id)
	return nil
}

// FreePending adds pages to the pending freelist at the given transaction ID
func (c *Coordinator) FreePending(txnID uint64, pageIDs []base.PageID) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.freePending(txnID, pageIDs)
	return nil
}

// ReleasePages moves pages from pending to free for all transactions < minTxnID
func (c *Coordinator) ReleasePages(minTxnID uint64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.release(minTxnID)
}

// PreventAllocationUpTo prevents allocation of pages freed up to and including the specified txnID
func (c *Coordinator) PreventAllocationUpTo(txnID uint64) {
	// We want to hold a lock rather than use an atomic because this is held by
	// DB during checkpoint.
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preventAlloc = txnID
}

// AllowAllAllocations clears the allocation prevention
func (c *Coordinator) AllowAllAllocations() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preventAlloc = 0
}

// GetMeta returns the current metadata
func (c *Coordinator) GetMeta() base.MetaPage {
	return c.activeMeta.Load().Meta
}

// GetSnapshot returns a COPY of the bundled metadata and root pointer atomically
// Returns by value to prevent data races with concurrent PutSnapshot updates
func (c *Coordinator) GetSnapshot() base.Snapshot {
	return *c.activeMeta.Load()
}

// PutSnapshot updates the metadata and root pointer, persists metadata to disk
// Does NOT make it visible to readers - call CommitSnapshot after fsync
func (c *Coordinator) PutSnapshot(meta base.MetaPage, root *base.Node) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Sync NumPages from atomic counter (may have uncommitted allocations)
	meta.NumPages = c.numPages.Load()

	// Update checksum (after NumPages sync)
	meta.Checksum = meta.CalculateChecksum()

	// Determine which page to write to based on TxID
	metaPageID := base.PageID(meta.TxID % 2)

	// Write to disk
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)
	if err := c.writePageUnsafe(metaPageID, metaPage); err != nil {
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
		c.activeMeta.Swap(&c.meta0)
	} else {
		c.activeMeta.Swap(&c.meta1)
	}
}

// Close serializes freelist to disk and closes the file
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Get active meta for reading
	meta := c.activeMeta.Load().Meta

	// Serialize freelist to disk
	pagesNeeded := c.pagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxID ensures they're only released after this close() completes
		oldPages := make([]base.PageID, meta.FreelistPages)
		for i := uint64(0); i < meta.FreelistPages; i++ {
			oldPages[i] = meta.FreelistID + base.PageID(i)
		}
		c.freePending(meta.TxID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = c.pagesNeeded()

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
	c.serializeFreelist(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := c.writePageUnsafe(meta.FreelistID+base.PageID(i), freelistPages[i]); err != nil {
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
	if err := c.writePageUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// Update in-memory and swap pointer
	if metaPageID == 0 {
		c.meta0.Meta = meta
		c.activeMeta.Swap(&c.meta0)
	} else {
		c.meta1.Meta = meta
		c.activeMeta.Swap(&c.meta1)
	}

	return c.storage.Close()
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
	c.activeMeta.Store(&c.meta0)

	// Initialize atomic numPages counter
	c.numPages.Store(meta.NumPages)

	// Write meta to both disk pages 0 and 1
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)

	if err := c.WritePage(0, metaPage); err != nil {
		return err
	}
	if err := c.WritePage(1, metaPage); err != nil {
		return err
	}

	// Write empty freelist to Page 2
	freelistPages := []*base.Page{&base.Page{}}
	c.serializeFreelist(freelistPages)
	if err := c.WritePage(2, freelistPages[0]); err != nil {
		return err
	}

	// Fsync to ensure durability
	return c.storage.Sync()
}

// loadExistingDB loads meta and freelist from existing database file
func (c *Coordinator) loadExistingDB() error {
	// Read both meta pages
	page0, err := c.ReadPage(0)
	if err != nil {
		return err
	}
	page1, err := c.ReadPage(1)
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

	// Point activeMeta to the one with higher TxID
	if err0 != nil {
		c.activeMeta.Store(&c.meta1)
	} else if err1 != nil {
		c.activeMeta.Store(&c.meta0)
	} else {
		// Both valid - pick highest TxID
		if meta0.TxID > meta1.TxID {
			c.activeMeta.Store(&c.meta0)
		} else {
			c.activeMeta.Store(&c.meta1)
		}
	}

	// Load freelist using active meta
	activeMeta := c.activeMeta.Load()
	freelistPages := make([]*base.Page, activeMeta.Meta.FreelistPages)
	for i := uint64(0); i < activeMeta.Meta.FreelistPages; i++ {
		page, err := c.ReadPage(activeMeta.Meta.FreelistID + base.PageID(i))
		if err != nil {
			return err
		}
		freelistPages[i] = page
	}
	c.deserializeFreelist(freelistPages)

	// Release any pending pages that are safe to reclaim
	// On startup, no readers exist, so all pending pages with txnID <= current can be released
	c.release(activeMeta.Meta.TxID)

	// Initialize atomic numPages counter from active meta
	c.numPages.Store(activeMeta.Meta.NumPages)

	return nil
}

// writePageUnsafe writes a Page without acquiring pm.mu (caller must hold pm.mu)
// Delegates to storage which has its own locking
func (c *Coordinator) writePageUnsafe(id base.PageID, page *base.Page) error {
	return c.storage.WritePage(id, page)
}

const (
	// PendingMarker indicates transition from free IDs to pending entries
	PendingMarker = base.PageID(0xFFFFFFFFFFFFFFFF)
)

// allocate returns a free Page ID, or 0 if none available
func (c *Coordinator) allocate() base.PageID {
	// Caller must hold c.mu
	if len(c.freedPages) == 0 {
		// No free pages available
		// If prevention is active and there are pending pages that could be released,
		// return 0 to force allocation of new pages instead of using recently freed ones
		if c.preventAlloc > 0 {
			for txnID, pages := range c.pendingPages {
				if txnID <= c.preventAlloc && len(pages) > 0 {
					// There are pending pages that would normally be released
					// Don't wait for them - force new Page allocation
					return 0
				}
			}
		}
		return 0
	}
	// Pop from end
	id := c.freedPages[len(c.freedPages)-1]
	c.freedPages = c.freedPages[:len(c.freedPages)-1]

	// CRITICAL: Remove from pending to prevent double-allocation
	for txnID, pages := range c.pendingPages {
		for i := len(pages) - 1; i >= 0; i-- {
			if pages[i] == id {
				c.pendingPages[txnID] = append(pages[:i], pages[i+1:]...)
				pages = c.pendingPages[txnID]
			}
		}
		if len(c.pendingPages[txnID]) == 0 {
			delete(c.pendingPages, txnID)
		}
	}

	return id
}

// free adds a Page ID to the free list
func (c *Coordinator) free(id base.PageID) {
	// Caller must hold c.mu
	// Check if already in free list to prevent duplicates
	for _, existingID := range c.freedPages {
		if existingID == id {
			return
		}
	}

	c.freedPages = append(c.freedPages, id)
	// Keep sorted for deterministic behavior
	for i := len(c.freedPages) - 1; i > 0; i-- {
		if c.freedPages[i] < c.freedPages[i-1] {
			c.freedPages[i], c.freedPages[i-1] = c.freedPages[i-1], c.freedPages[i]
		} else {
			break
		}
	}
}

// freePending adds pages to the pending map at the given transaction ID
func (c *Coordinator) freePending(txnID uint64, pageIDs []base.PageID) {
	// Caller must hold c.mu
	if len(pageIDs) == 0 {
		return
	}
	c.pendingPages[txnID] = append(c.pendingPages[txnID], pageIDs...)
}

// release moves pages from pending to free for all transactions < minTxnID
func (c *Coordinator) release(minTxnID uint64) int {
	// Caller must hold c.mu
	released := 0
	for txnID, pages := range c.pendingPages {
		if txnID < minTxnID {
			for _, pageID := range pages {
				c.free(pageID)
				released++
			}
			delete(c.pendingPages, txnID)
		}
	}
	return released
}

// pagesNeeded returns number of pages needed to serialize this freelist
func (c *Coordinator) pagesNeeded() int {
	// Caller must hold c.mu
	freeBytes := 8 + len(c.freedPages)*8

	pendingBytes := 0
	if len(c.pendingPages) > 0 {
		pendingBytes = 8 + 8 // marker + count
		for _, pages := range c.pendingPages {
			pendingBytes += 8 + 8 + len(pages)*8 // txnID + count + page IDs
		}
	}

	totalBytes := freeBytes + pendingBytes
	if totalBytes == 0 {
		return 1
	}

	pagesNeeded := 0
	remainingBytes := totalBytes
	for remainingBytes > 0 {
		pagesNeeded++
		if remainingBytes <= base.PageSize {
			remainingBytes = 0
		} else {
			remainingBytes -= base.PageSize
		}
	}

	return pagesNeeded
}

// serializeFreelist writes freelist to pages starting at given slice
func (c *Coordinator) serializeFreelist(pages []*base.Page) {
	// Caller must hold c.mu
	buf := make([]byte, 0, base.PageSize*len(pages))

	// Write free count
	countBytes := make([]byte, 8)
	*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(c.freedPages))
	buf = append(buf, countBytes...)

	// Write free IDs
	for _, id := range c.freedPages {
		idBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&idBytes[0])) = id
		buf = append(buf, idBytes...)
	}

	// Write pending data if present
	if len(c.pendingPages) > 0 {
		// Write marker
		markerBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&markerBytes[0])) = PendingMarker
		buf = append(buf, markerBytes...)

		// Write pending count
		pendingCountBytes := make([]byte, 8)
		*(*uint64)(unsafe.Pointer(&pendingCountBytes[0])) = uint64(len(c.pendingPages))
		buf = append(buf, pendingCountBytes...)

		// Sort txnIDs for deterministic serialization
		txnIDs := make([]uint64, 0, len(c.pendingPages))
		for txnID := range c.pendingPages {
			txnIDs = append(txnIDs, txnID)
		}
		for i := 1; i < len(txnIDs); i++ {
			for j := i; j > 0 && txnIDs[j] < txnIDs[j-1]; j-- {
				txnIDs[j], txnIDs[j-1] = txnIDs[j-1], txnIDs[j]
			}
		}

		// Write each pending entry
		for _, txnID := range txnIDs {
			pages := c.pendingPages[txnID]

			// Write txnID
			txnBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&txnBytes[0])) = txnID
			buf = append(buf, txnBytes...)

			// Write Page count
			countBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(pages))
			buf = append(buf, countBytes...)

			// Write page IDs
			for _, pageID := range pages {
				pidBytes := make([]byte, 8)
				*(*base.PageID)(unsafe.Pointer(&pidBytes[0])) = pageID
				buf = append(buf, pidBytes...)
			}
		}
	}

	// Copy buffer to pages
	offset := 0
	for i := 0; i < len(pages); i++ {
		n := copy(pages[i].Data[:], buf[offset:])
		offset += n
	}
}

// deserializeFreelist reads freelist from pages
func (c *Coordinator) deserializeFreelist(pages []*base.Page) {
	// Caller must hold c.mu
	c.freedPages = make([]base.PageID, 0)
	c.pendingPages = make(map[uint64][]base.PageID)

	// Build linear buffer from pages
	buf := make([]byte, 0, base.PageSize*len(pages))
	for _, page := range pages {
		buf = append(buf, page.Data[:]...)
	}

	offset := 0

	// Read free count
	if len(buf) < 8 {
		return
	}
	freeCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
	offset += 8

	// Read free IDs
	for i := uint64(0); i < freeCount; i++ {
		if offset+8 > len(buf) {
			break
		}
		id := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
		c.freedPages = append(c.freedPages, id)
		offset += 8
	}

	// Check for pending marker
	if offset+8 > len(buf) {
		return
	}
	marker := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
	if marker != PendingMarker {
		return
	}
	offset += 8

	// Read pending count
	if offset+8 > len(buf) {
		return
	}
	pendingCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
	offset += 8

	// Read pending entries
	for i := uint64(0); i < pendingCount; i++ {
		// Read txnID
		if offset+8 > len(buf) {
			break
		}
		txnID := *(*uint64)(unsafe.Pointer(&buf[offset]))
		offset += 8

		// Read Page count
		if offset+8 > len(buf) {
			break
		}
		pageCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
		offset += 8

		// Read Page IDs
		pageIDs := make([]base.PageID, 0, pageCount)
		for j := uint64(0); j < pageCount; j++ {
			if offset+8 > len(buf) {
				break
			}
			pageID := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
			pageIDs = append(pageIDs, pageID)
			offset += 8
		}

		if len(pageIDs) > 0 {
			c.pendingPages[txnID] = pageIDs
		}
	}
}

// GetNode retrieves a node, checking cache first then loading from disk.
func (c *Coordinator) GetNode(pageID base.PageID, txnID uint64) (*base.Node, error) {
	// Check cache first
	if node, hit := c.cache.Get(pageID); hit {
		return node, nil
	}

	// Load from disk and cache
	return c.LoadNodeWithCache(pageID, txnID)
}

// LoadNodeWithCache loads a node from disk and caches it.
func (c *Coordinator) LoadNodeWithCache(pageID base.PageID, txnID uint64) (*base.Node, error) {
	// Load from disk
	node, _, err := c.LoadNodeFromDisk(pageID)
	if err != nil {
		return nil, err
	}

	// Cache and return
	c.cache.Put(pageID, node)
	return node, nil
}

// LoadNode loads a node, coordinating cache and disk I/O.
// Routes TX calls through Coordinator instead of direct cache access.
func (c *Coordinator) LoadNode(pageID base.PageID, txnID uint64) (*base.Node, bool) {
	node, err := c.GetNode(pageID, txnID)
	if err != nil {
		return nil, false
	}
	return node, true
}

// LoadNodeFromDisk reads a page from disk and deserializes it to a Node.
// This centralizes disk I/O + deserialization in coordinator layer.
// Returns (node, diskTxnID, error).
func (c *Coordinator) LoadNodeFromDisk(pageID base.PageID) (*base.Node, uint64, error) {
	page, err := c.storage.ReadPage(pageID)
	if err != nil {
		return nil, 0, err
	}

	node := &base.Node{
		PageID: pageID,
		Dirty:  false,
	}

	header := page.Header()
	if err := node.Deserialize(page); err != nil {
		return nil, 0, err
	}

	return node, header.TxnID, nil
}

// FlushNode serializes and writes a dirty node to disk.
// Used by cache to flush dirty pages during eviction or checkpoint.
// Returns error if serialization or write fails.
func (c *Coordinator) FlushNode(node *base.Node, txnID uint64, pageID base.PageID) error {
	buf := c.storage.GetBuffer()
	defer c.storage.PutBuffer(buf)
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	err := node.Serialize(txnID, page)
	if err != nil {
		return err
	}

	return c.storage.WritePage(pageID, page)
}

// SyncMode controls when to fsync (copied from main package to avoid import cycle)
type SyncMode int

const (
	SyncEveryCommit SyncMode = iota
	SyncOff
)

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
			realPageID, err := c.AllocatePage()
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
		realPageID, err := c.AllocatePage()
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
	buf := c.storage.GetBuffer()
	defer c.storage.PutBuffer(buf)
	for _, node := range pages {
		page := (*base.Page)(unsafe.Pointer(&buf[0]))
		if err := node.Serialize(txnID, page); err != nil {
			return err
		}

		if err := c.storage.WritePage(node.PageID, page); err != nil {
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

		if err = c.storage.WritePage(root.PageID, page); err != nil {
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
	meta := c.GetMeta()
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
		if err := c.storage.Sync(); err != nil {
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

type Stats struct {
	Cache        cache.Stats
	Store        storage.Stats
	FreedPages   int
	PendingPages map[uint64]int
}

// Stats returns disk I/O statistics
func (c *Coordinator) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := make(map[uint64]int)
	for txnID, pages := range c.pendingPages {
		pending[txnID] = len(pages)
	}

	return Stats{
		Cache:        c.cache.Stats(),
		Store:        c.storage.Stats(),
		FreedPages:   len(c.freedPages),
		PendingPages: pending,
	}
}

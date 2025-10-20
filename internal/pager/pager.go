package pager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/cache"
	"github.com/alexhholmes/fredb/internal/storage"
)

// SyncMode controls when to fsync (copied from main package to avoid import cycle)
type SyncMode int

const (
	SyncEveryCommit SyncMode = iota
	SyncOff
)

// Pager coordinates store, cache, meta, and freelist
type Pager struct {
	cache *cache.Cache    // Simple LRU cache
	store storage.Storage // File I/O backend

	// Dual meta pages for atomic writes visible to readers stored at page IDs 0 and 1
	active atomic.Pointer[base.Snapshot]
	meta0  base.Snapshot
	meta1  base.Snapshot

	// Page allocation tracking (separate from meta to avoid data races)
	pages atomic.Uint64 // Total pages allocated (includes uncommitted allocations)

	// Freelist management (owns its own mutex)
	freelist *Freelist

	// Bucket reference count tracking
	// This tracks number of references to root pages of buckets by transactions.
	// The deleted map tracks which buckets have been deleted and can be cleaned
	// up since the last transaction that referenced them has completed.
	buckets   sync.Map                 // Bucket root PageID -> atomic.Int32 ref count
	DeletedMu sync.RWMutex             // Protects Deleted map
	Deleted   map[base.PageID]struct{} // Buckets pending deletion, 0 ref count

	// Track background cleanup goroutines to wait for them during close
	cleanup sync.WaitGroup
}

// NewPager creates a pager with injected dependencies
func NewPager(storage storage.Storage, cache *cache.Cache) (*Pager, error) {
	c := &Pager{
		store: storage,
		cache: cache,
		freelist: &Freelist{
			freed:   make(map[base.PageID]struct{}),
			pending: make(map[uint64][]base.PageID),
		},
		Deleted: make(map[base.PageID]struct{}),
	}

	// Check if new file (empty)
	empty, err := storage.Empty()
	if err != nil {
		return nil, err
	}

	if empty {
		// New database - initialize dual meta pages and empty freelist
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
			return nil, err
		}
		if err := c.store.WritePage(1, metaPage); err != nil {
			return nil, err
		}

		// Write empty freelist to Page 2
		freelistPages := []*base.Page{&base.Page{}}
		c.freelist.Serialize(freelistPages)
		if err := c.store.WritePage(2, freelistPages[0]); err != nil {
			return nil, err
		}

		// Fsync to ensure durability
		if err := c.store.Sync(); err != nil {
			return nil, err
		}
	} else {
		// Existing database - load meta and freelist
		// Read both meta pages
		page0, err := c.store.ReadPage(0)
		if err != nil {
			return nil, err
		}
		page1, err := c.store.ReadPage(1)
		if err != nil {
			return nil, err
		}

		// Validate and pick the best meta Page
		meta0 := page0.ReadMeta()
		meta1 := page1.ReadMeta()

		err0 := meta0.Validate()
		err1 := meta1.Validate()

		// Both invalid - corrupted database
		if err0 != nil && err1 != nil {
			return nil, fmt.Errorf("both meta pages corrupted: %v, %v", err0, err1)
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
				return nil, err
			}
			freelistPages[i] = page
		}
		c.freelist.Deserialize(freelistPages)

		// Release any pending pages that are safe to reclaim
		// On startup, no readers exist, so all pending pages with txnID <= current can be released
		// No cache invalidation needed on startup (cache is empty)
		c.freelist.Release(activeMeta.Meta.TxID, nil)

		// Initialize atomic pages counter from active meta
		c.pages.Store(activeMeta.Meta.NumPages)
	}

	return c, nil
}

// AssignPageID allocates a new Page (from freelist or grows file)
func (p *Pager) AssignPageID() (base.PageID, bool) {
	// Try freelist first
	id := p.freelist.Allocate()
	if id != 0 {
		return id, false
	}

	// Grow file - use atomic pages counter (includes uncommitted allocations)
	// Atomically increment and get the new page ID
	id = base.PageID(p.pages.Add(1) - 1)

	return id, true
}

// FreePage adds a Page to the freelist
func (p *Pager) FreePage(id base.PageID) {
	p.freelist.Free(id)
}

// ReleasePages moves pages from pending to free for all transactions < minTxnID
// Invalidates cache entries atomically (under freelist lock) to prevent races.
func (p *Pager) ReleasePages(minTxnID uint64) int {
	// Pass cache invalidation callback - runs atomically under freelist lock
	return p.freelist.Release(minTxnID, func(pageID base.PageID) {
		p.cache.Remove(pageID)
	})
}

// GetMeta returns the current metadata
func (p *Pager) GetMeta() base.MetaPage {
	return p.active.Load().Meta
}

// GetSnapshot returns a COPY of the bundled metadata and root pointer atomically
// Returns by value to prevent data races with concurrent PutSnapshot updates
func (p *Pager) GetSnapshot() base.Snapshot {
	return *p.active.Load()
}

// PutSnapshot updates the metadata and root pointer, persists metadata to disk
// Does NOT make it visible to readers - call CommitSnapshot after fsync
func (p *Pager) PutSnapshot(meta base.MetaPage, root *base.Node) error {
	// Sync NumPages from atomic counter (may have uncommitted allocations)
	meta.NumPages = p.pages.Load()

	// Update checksum (after NumPages sync)
	meta.Checksum = meta.CalculateChecksum()

	// Determine which page to write to based on TxID
	metaPageID := base.PageID(meta.TxID % 2)

	// Write to disk
	var buf []byte
	if store, ok := p.store.(*storage.DirectIO); ok {
		// DirectIO requires aligned buffers
		buf = store.GetBuffer()
		defer store.PutBuffer(buf)
	} else {
		// MMap can use normal buffers
		buf = make([]byte, base.PageSize)
	}
	metaPage := (*base.Page)(unsafe.Pointer(&buf[0]))
	metaPage.WriteMeta(&meta)
	if err := p.store.WritePage(metaPageID, metaPage); err != nil {
		return err
	}

	// Update inactive in-memory copy with both meta and root (don't swap pointer yet)
	if metaPageID == 0 {
		p.meta0.Meta = meta
		p.meta0.Root = root
	} else {
		p.meta1.Meta = meta
		p.meta1.Root = root
	}

	return nil
}

// CommitSnapshot atomically makes the last PutSnapshot visible to readers
// Call AFTER fsync to ensure durability before visibility
// Lock-free: single writer guarantee + atomic swap
func (p *Pager) CommitSnapshot() {
	// Swap to the meta with higher or equal TxID
	// Use >= to handle initial case where both metas have TxID=0
	if p.meta0.Meta.TxID >= p.meta1.Meta.TxID {
		p.active.Swap(&p.meta0)
	} else {
		p.active.Swap(&p.meta1)
	}
}

// GetNode retrieves a node, checking cache first then loading from disk.
func (p *Pager) GetNode(pageID base.PageID) (*base.Node, error) {
	// Check cache first
	if node, hit := p.cache.Get(pageID); hit {
		return node, nil
	}

	// Load from disk
	page, err := p.store.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	node := &base.Node{
		PageID: pageID,
		Dirty:  false,
	}

	if err = node.Deserialize(page, p.store.ReadPage); err != nil {
		return nil, err
	}

	// Cache and return
	p.cache.Put(pageID, node)
	return node, nil
}

// LoadNode loads a node, coordinating cache and disk I/O.
// Routes TX calls through Pager instead of direct cache access.
func (p *Pager) LoadNode(pageID base.PageID) (*base.Node, bool) {
	node, err := p.GetNode(pageID)
	if err != nil {
		return nil, false
	}
	return node, true
}

// WriteTransaction writes all pages to disk, handles freed pages, updates metadata, and syncs.
// This is phase 2 of commit: writing pages with real IDs to disk.
// Caller must hold db.mu.
func (p *Pager) WriteTransaction(
	pages map[base.PageID]*base.Node,
	root *base.Node,
	freed map[base.PageID]struct{},
	txnID uint64,
	syncMode SyncMode,
) error {
	// Track overflow pages allocated during serialization
	overflowPages := make(map[base.PageID]*base.Page)

	// Callback for allocating overflow pages
	allocPage := func() *base.Page {
		// Past point of rollback, no need to track if this was allocated from
		// freelist. Won't cause a double-free.
		pageID, _ := p.AssignPageID()

		page := &base.Page{}
		header := &base.PageHeader{
			PageID: pageID,
		}
		page.WriteHeader(header)

		overflowPages[pageID] = page
		return page
	}

	// Write all pages to disk
	var buf []byte
	if store, ok := p.store.(*storage.DirectIO); ok {
		buf = store.GetBuffer()
		defer store.PutBuffer(buf)
	} else {
		// MMap can use normal buffers
		buf = make([]byte, base.PageSize)
	}
	for _, node := range pages {
		page := (*base.Page)(unsafe.Pointer(&buf[0]))
		if err := node.Serialize(txnID, page, allocPage); err != nil {
			return err
		}

		if err := p.store.WritePage(node.PageID, page); err != nil {
			return err
		}

		node.Dirty = false
		p.cache.Put(node.PageID, node)
	}

	// Write root separately if dirty
	if root != nil && root.Dirty {
		page := (*base.Page)(unsafe.Pointer(&buf[0]))
		err := root.Serialize(txnID, page, allocPage)
		if err != nil {
			return err
		}

		if err = p.store.WritePage(root.PageID, page); err != nil {
			return err
		}

		root.Dirty = false
		p.cache.Put(root.PageID, root)
	}

	// Write overflow pages to disk
	for pageID, overflowPage := range overflowPages {
		if err := p.store.WritePage(pageID, overflowPage); err != nil {
			return err
		}
	}

	// Add freed pages to pending
	if len(freed) > 0 {
		freedSlice := make([]base.PageID, 0, len(freed))
		for pageID := range freed {
			freedSlice = append(freedSlice, pageID)
		}
		p.freelist.Pending(txnID, freedSlice)
	}

	// Update meta
	meta := p.active.Load().Meta
	if root != nil {
		meta.RootPageID = root.PageID
	}
	meta.TxID = txnID
	meta.Checksum = meta.CalculateChecksum()

	if err := p.PutSnapshot(meta, root); err != nil {
		return err
	}

	// Conditional sync (this is the commit point!)
	if syncMode == SyncEveryCommit {
		if err := p.store.Sync(); err != nil {
			return err
		}
	}

	// Make meta visible to readers atomically
	p.CommitSnapshot()

	return nil
}

// AcquireBucket increments the reference count for a bucket's root page.
// Returns false if the bucket is marked for deletion (new transactions cannot access it).
// Returns true if successfully acquired.
func (p *Pager) AcquireBucket(rootID base.PageID) bool {
	// CRITICAL: Check deletion status BEFORE incrementing to avoid refcount leak
	// If we increment first, a concurrent delete can see non-zero refcount and skip cleanup
	p.DeletedMu.RLock()
	_, deleted := p.Deleted[rootID]
	p.DeletedMu.RUnlock()

	if deleted {
		return false // Bucket marked for deletion - reject acquisition
	}

	// Safe to increment - bucket not marked deleted at this point
	counter := &atomic.Int32{}
	counter.Store(1)
	refCountVal, loaded := p.buckets.LoadOrStore(rootID, counter)

	if loaded {
		refCount := refCountVal.(*atomic.Int32)
		refCount.Add(1)
	}

	return true
}

// ReleaseBucket decrements the reference count for a bucket's root page.
// If the count reaches 0 and the bucket is marked for deletion, triggers background cleanup.
// The cleanup callback is provided by the caller (typically db.freeTree).
func (p *Pager) ReleaseBucket(rootID base.PageID, cleanupFunc func(base.PageID) error) {
	// Decrement reference count
	refCountVal, exists := p.buckets.Load(rootID)
	if !exists {
		// Race: bucket was already released to 0 by another goroutine
		// This can happen if AcquireBucket failed after incrementing
		return
	}

	refCount := refCountVal.(*atomic.Int32)
	newCount := refCount.Add(-1)

	// If count reaches 0, check if this bucket is marked for deletion
	if newCount == 0 {
		p.DeletedMu.Lock()
		_, shouldDelete := p.Deleted[rootID]
		if shouldDelete {
			// Remove from deleted map
			delete(p.Deleted, rootID)
			p.DeletedMu.Unlock()

			// Trigger background cleanup
			p.cleanup.Add(1)
			go func() {
				defer p.cleanup.Done()
				_ = cleanupFunc(rootID)
			}()
		} else {
			p.DeletedMu.Unlock()
		}

		// Clean up empty reference count entry
		p.buckets.Delete(rootID)
	}
}

// Close serializes freelist to disk and closes the file
func (p *Pager) Close() error {
	// Wait for all background cleanup goroutines to complete
	p.cleanup.Wait()

	// Get active meta for reading
	meta := p.active.Load().Meta

	// Serialize freelist to disk
	pagesNeeded := p.freelist.PagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxID ensures they're only released after this close() completes
		oldPages := make([]base.PageID, meta.FreelistPages)
		for i := uint64(0); i < meta.FreelistPages; i++ {
			oldPages[i] = meta.FreelistID + base.PageID(i)
		}
		p.freelist.Pending(meta.TxID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = p.freelist.PagesNeeded()

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
	p.freelist.Serialize(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := p.store.WritePage(meta.FreelistID+base.PageID(i),
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
	if err := p.store.WritePage(metaPageID, metaPage); err != nil {
		return err
	}

	// Update in-memory and swap pointer
	if metaPageID == 0 {
		p.meta0.Meta = meta
		p.active.Swap(&p.meta0)
	} else {
		p.meta1.Meta = meta
		p.active.Swap(&p.meta1)
	}

	return p.store.Close()
}

type Stats struct {
	Cache      cache.Stats
	Store      storage.Stats
	FreedPages int
}

// Stats returns disk I/O statistics
func (p *Pager) Stats() Stats {
	return Stats{
		Cache:      p.cache.Stats(),
		Store:      p.store.Stats(),
		FreedPages: p.freelist.Stats(),
	}
}

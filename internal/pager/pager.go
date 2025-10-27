package pager

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/google/btree"

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
	cache *cache.Cache     // Simple LRU cache
	store *storage.Storage // File I/O backend
	mode  SyncMode         // Sync mode for commits

	// Dual meta pages for atomic writes visible to readers stored at page IDs 0 and 1
	active atomic.Pointer[Snapshot]
	meta0  Snapshot
	meta1  Snapshot

	// Page allocation tracking (separate from meta to avoid data races)
	pages       atomic.Uint64 // Total pages allocated (includes uncommitted allocations)
	pagesOnDisk atomic.Uint64 // Highest page ID actually written to disk

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
func NewPager(mode SyncMode, store *storage.Storage, cache *cache.Cache) (*Pager, error) {
	c := &Pager{
		mode:  mode,
		store: store,
		cache: cache,
		freelist: &Freelist{
			freed:   make(map[base.PageID]struct{}),
			pending: make(map[uint64][]base.PageID),
		},
		Deleted: make(map[base.PageID]struct{}),
	}

	// Check if new file (empty)
	empty, err := store.Empty()
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

		// Put both in-memory copies with Meta and RootiReiot is nil initially)
		c.meta0.Meta = meta
		c.meta0.Root = nil
		c.meta1.Meta = meta
		c.meta1.Root = nil
		c.active.Store(&c.meta0)

		// Initialize atomic pages counter and max written page
		c.pages.Store(meta.NumPages)
		c.pagesOnDisk.Store(2) // Pages 0,1,2 written (meta x2, freelist)

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

		// Put both in-memory copies with .Meta and .Root (Root will be loaded by DB later)
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

		// Initialize atomic pages counter and max written page from active meta
		c.pages.Store(activeMeta.Meta.NumPages)
		// NumPages is count (e.g., 8 pages = IDs 0-7), so max ID is NumPages-1
		if activeMeta.Meta.NumPages > 0 {
			c.pagesOnDisk.Store(activeMeta.Meta.NumPages - 1)
		}
	}

	return c, nil
}

// Allocate allocates a new Page (from freelist or grows file)
func (p *Pager) Allocate(count int) base.PageID {
	if count <= 0 {
		return 0
	}

	// For single page, try freelist first
	if count == 1 {
		id := p.freelist.Allocate()
		if id != 0 {
			return id
		}
	}

	// Grow file - allocate contiguous pages
	// Atomically add count and get the first page ID
	firstID := base.PageID(p.pages.Add(uint64(count)) - uint64(count))

	return firstID
}

// Free adds a Page to the freelist
func (p *Pager) Free(id base.PageID) {
	p.freelist.Free(id)
}

// TrackWrite updates pagesOnDisk after writing a page externally
func (p *Pager) TrackWrite(pageID base.PageID) {
	for {
		old := p.pagesOnDisk.Load()
		if uint64(pageID) <= old {
			break
		}
		if p.pagesOnDisk.CompareAndSwap(old, uint64(pageID)) {
			break
		}
	}
}

// Release moves pages from pending to free for all transactions < minTxnID
// Invalidates cache entries atomically (under freelist lock) to prevent races.
func (p *Pager) Release(minTxnID uint64) int {
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
func (p *Pager) GetSnapshot() Snapshot {
	return *p.active.Load()
}

// PutSnapshot updates the metadata and root pointer, persists metadata to disk
// Does NOT make it visible to readers - call CommitSnapshot after fsync
func (p *Pager) PutSnapshot(meta base.MetaPage, root *base.Node) error {
	// Put NumPages from max written page + 1 (NumPages is count, not max ID)
	meta.NumPages = p.pagesOnDisk.Load() + 1

	// Update checksum (after NumPages sync)
	meta.Checksum = meta.CalculateChecksum()

	// Determine which page to write to based on TxID
	metaPageID := base.PageID(meta.TxID % 2)

	// Write to disk
	buf := p.store.GetBuffer()
	defer p.store.PutBuffer(buf)
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

// LoadNode retrieves a node, checking cache first then loading from disk.
func (p *Pager) LoadNode(pageID base.PageID) (*base.Node, error) {
	// Check cache first
	if node, hit := p.cache.Get(pageID); hit {
		return node, nil
	}

	// Load from disk
	page, err := p.store.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	node := &base.Node{}
	if err = node.Deserialize(page); err != nil {
		return nil, err
	}

	// Sanity check
	if pageID != node.PageID {
		panic(fmt.Sprintf("invalid pageID: %d, expected %d", pageID, node.PageID))
	}

	// Cache and return
	p.cache.Put(pageID, node)
	return node, nil
}

// Commit writes all pages to disk, handles freed pages, updates metadata, and syncs.
// This is phase 2 of commit: writing pages with real IDs to disk.
// Caller must hold db.mu.
func (p *Pager) Commit(
	pages *btree.BTreeG[*base.Node],
	root *base.Node,
	freed map[base.PageID]struct{},
	txID uint64,
) error {
	// Write all pages to disk
	if pages.Len() == 1 {
		buf := p.store.GetBuffer()
		defer p.store.PutBuffer(buf)

		// Special case: single page - avoid run logic
		single, _ := pages.Min()
		if err := p.WriteRun([]*base.Node{single}, txID); err != nil {
			return err
		}
	} else {
		// Ascend the btree, forming contiguous runs of nodes to write at once
		var wg sync.WaitGroup
		var err error
		errFunc := sync.OnceFunc(func() {
			err = errors.New("write failed")
		})
		run := make([]*base.Node, 0, pages.Len())
		pages.Ascend(func(item *base.Node) bool {
			if len(run) == 0 {
				// First item, start new run
				run = append(run, item)
			} else if item.PageID == run[len(run)-1].PageID+1 {
				// Contiguous, add to current run
				run = append(run, item)
			} else {
				// Not contiguous, write current run and start new one
				runner := run
				wg.Add(1)

				go func() {
					defer wg.Done()

					err2 := p.WriteRun(runner, txID)
					if err2 != nil {
						errFunc()
					}
				}()

				// Start new run with current item
				run = make([]*base.Node, 0, pages.Len())
				run = append(run, item)
			}

			return true
		})

		// Write any remaining run
		if len(run) > 0 {
			wg.Add(1)
			runner := run
			go func() {
				defer wg.Done()

				err2 := p.WriteRun(runner, txID)
				if err2 != nil {
					errFunc()
				}
			}()
		}

		wg.Wait()

		if err != nil {
			return err
		}
	}

	// Write root separately if dirty
	if root != nil && root.Dirty {
		mainPage, overflowPages, err := root.Serialize(txID, p.Allocate)
		if err != nil {
			return err
		}

		// Write main page
		if err = p.store.WritePage(root.PageID, mainPage); err != nil {
			return err
		}
		p.TrackWrite(root.PageID)

		// Write overflow pages
		if err := p.WriteOverflow(overflowPages); err != nil {
			return err
		}

		root.Dirty = false
		p.cache.Put(root.PageID, root)
	}

	// Add freed pages to pending
	if len(freed) > 0 {
		freedSlice := make([]base.PageID, 0, len(freed))
		for pageID := range freed {
			freedSlice = append(freedSlice, pageID)
		}
		p.freelist.Pending(txID, freedSlice)
	}

	// Update meta
	meta := p.active.Load().Meta
	if root != nil {
		meta.RootPageID = root.PageID
	}
	meta.TxID = txID
	meta.Checksum = meta.CalculateChecksum()

	if err := p.PutSnapshot(meta, root); err != nil {
		return err
	}

	// Conditional sync (this is the commit point!)
	if p.mode == SyncEveryCommit {
		if err := p.store.Sync(); err != nil {
			return err
		}
	}

	// Make meta visible to readers atomically
	p.CommitSnapshot()

	return nil
}

func (p *Pager) WriteRun(run []*base.Node, txID uint64) error {
	// Write current run to disk
	if len(run) == 1 {
		node := run[0]
		mainPage, overflowPages, err := node.Serialize(txID, p.Allocate)
		if err != nil {
			return err
		}

		// Write main page
		if err := p.store.WritePage(node.PageID, mainPage); err != nil {
			return err
		}
		p.TrackWrite(node.PageID)

		// Write overflow pages
		if err := p.WriteOverflow(overflowPages); err != nil {
			return err
		}

		node.Dirty = false
		p.cache.Put(node.PageID, node)
	} else {
		// For contiguous runs, serialize all main pages and write together
		// Collect overflow pages to write separately
		mainBuf := make([]byte, len(run)*base.PageSize)
		var allOverflowPages [][]*base.Page

		for i, node := range run {
			mainPage, overflowPages, err := node.Serialize(txID, p.Allocate)
			if err != nil {
				return err
			}

			// Copy main page to contiguous buffer
			copy(mainBuf[i*base.PageSize:], mainPage.Data[:])
			allOverflowPages = append(allOverflowPages, overflowPages...)
		}

		// Write all main pages at once
		if err := p.store.WriteAt(run[0].PageID, mainBuf); err != nil {
			return err
		}
		for _, node := range run {
			p.TrackWrite(node.PageID)
		}

		// Write overflow pages
		if err := p.WriteOverflow(allOverflowPages); err != nil {
			return err
		}

		// Mark clean and cache only after successful write
		for _, node := range run {
			node.Dirty = false
			p.cache.Put(node.PageID, node)
		}
	}

	return nil
}

// WriteOverflow writes all overflow pages with a single WriteAt call
// since they are allocated contiguously by the single writer
func (p *Pager) WriteOverflow(overflowPages [][]*base.Page) error {
	if len(overflowPages) == 0 {
		return nil
	}

	// Count total overflow pages
	totalOverflowPages := 0
	for _, pages := range overflowPages {
		totalOverflowPages += len(pages)
	}

	if totalOverflowPages == 0 {
		return nil
	}

	// Use fixed 512KB buffer, reuse for multiple writes if needed
	const bufferSize = 512 * 1024 // 512KB (128 pages)
	const pagesPerBuffer = bufferSize / base.PageSize

	buf := make([]byte, bufferSize)
	firstPageID := overflowPages[0][0].Header().PageID

	// Flatten overflow pages into single slice for easier iteration
	var allPages []*base.Page
	for _, pages := range overflowPages {
		allPages = append(allPages, pages...)
	}

	// Write in chunks using reusable buffer
	for i := 0; i < totalOverflowPages; i += pagesPerBuffer {
		// Calculate chunk size
		pagesInChunk := pagesPerBuffer
		if i+pagesPerBuffer > totalOverflowPages {
			pagesInChunk = totalOverflowPages - i
		}
		chunkSize := pagesInChunk * base.PageSize

		// Copy pages into buffer
		offset := 0
		for j := 0; j < pagesInChunk; j++ {
			copy(buf[offset:], allPages[i+j].Data[:])
			offset += base.PageSize
		}

		// Write chunk
		chunkPageID := firstPageID + base.PageID(i)
		if err := p.store.WriteAt(chunkPageID, buf[:chunkSize]); err != nil {
			return err
		}

		// Track writes for this chunk
		for j := 0; j < pagesInChunk; j++ {
			p.TrackWrite(chunkPageID + base.PageID(j))
		}
	}

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

	// Release all pending pages (no readers at shutdown)
	p.freelist.Release(math.MaxUint64, nil)

	// serialize freelist to disk
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
		pageID := meta.FreelistID + base.PageID(i)
		if err := p.store.WritePage(pageID, freelistPages[i]); err != nil {
			return err
		}
		p.TrackWrite(pageID)
	}

	// Update meta (increment TxID, sync NumPages from pagesOnDisk, recalculate checksum)
	meta.TxID++

	// Sync NumPages from pagesOnDisk, but account for freelist relocation
	// If freelist was relocated, meta.NumPages was already updated to include those pages
	numPages := p.pagesOnDisk.Load() + 1
	if numPages > meta.NumPages {
		meta.NumPages = numPages
	}
	// Otherwise keep meta.NumPages which includes relocated freelist pages

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
	Cache     cache.Stats
	Store     storage.Stats
	FreePages int
}

// Stats returns disk I/O statistics
func (p *Pager) Stats() Stats {
	return Stats{
		Cache:     p.cache.Stats(),
		Store:     p.store.Stats(),
		FreePages: p.freelist.Stats(),
	}
}

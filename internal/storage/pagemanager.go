package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"fredb/internal/base"
	"fredb/internal/directio"
)

// Snapshot bundles metadata and root pointer for atomic visibility
type Snapshot struct {
	Meta base.MetaPage
	Root *base.Node
}

// PendingVersion tracks a page version waiting for readers to finish
type PendingVersion struct {
	OriginalPageID base.PageID // Logical page ID
	PhysicalPageID base.PageID // Where it actually lives (may differ if relocated)
}

// PageManager implements PageManager with disk-based storage
type PageManager struct {
	mu   sync.Mutex // Protects meta and freelist access
	file *os.File

	// Dual meta pages for atomic writes visible to readers stored at page IDs 0 and 1
	activeMeta atomic.Pointer[Snapshot]
	meta0      Snapshot
	meta1      Snapshot

	// Direct I/O buffer pool
	bufPool *sync.Pool // Pool of aligned []byte buffers for direct I/O

	// Freelist tracking
	freePages        []base.PageID               // sorted array of free Page IDs
	pendingVersions  map[uint64][]PendingVersion // txnID -> page versions freed at that transaction
	preventUpToTxnID uint64                      // temporarily prevent allocation of pages freed up to this txnID (0 = no prevention)

	// Page allocation tracking (separate from meta to avoid data races)
	numPages atomic.Uint64 // Total pages allocated (includes uncommitted allocations)

	// Stats
	diskReads  atomic.Uint64 // Number of actual disk reads
	diskWrites atomic.Uint64 // Number of actual disk writes
}

// NewPageManager opens or creates a database file
func NewPageManager(path string) (*PageManager, error) {
	// Use directio.OpenFile - falls back to regular I/O on unsupported platforms
	file, err := directio.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	dm := &PageManager{
		file:            file,
		freePages:       make([]base.PageID, 0),
		pendingVersions: make(map[uint64][]PendingVersion),
		bufPool: &sync.Pool{
			New: func() interface{} {
				return directio.AlignedBlock(base.PageSize)
			},
		},
	}

	// Check if new file (empty)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		// New database - initialize
		if err := dm.initializeNewDB(); err != nil {
			_ = file.Close()
			return nil, err
		}
	} else {
		// Existing database - load meta and freelist
		if err := dm.loadExistingDB(); err != nil {
			_ = file.Close()
			return nil, err
		}
	}

	return dm, nil
}

// ReadPage reads a Page from disk.
// Used during checkpoint to read old disk versions before overwriting.
func (pm *PageManager) ReadPage(id base.PageID) (*base.Page, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	offset := int64(id) * base.PageSize
	page := &base.Page{}

	// Get aligned buffer from pool
	buf := pm.bufPool.Get().([]byte)
	defer pm.bufPool.Put(buf)

	pm.diskReads.Add(1) // Track disk read
	n, err := pm.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n != base.PageSize {
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, base.PageSize)
	}

	// Copy from aligned buffer to page
	copy(page.Data[:], buf)

	return page, nil
}

// WritePage writes a Page to a specific offset (with locking)
func (pm *PageManager) WritePage(id base.PageID, page *base.Page) error {
	// Defensive check: virtual page IDs (negative when cast to int64) should never reach WritePage
	if int64(id) < 0 {
		return fmt.Errorf("FATAL: attempting to write virtual page ID %d (0x%x) to disk - page IDs must be remapped before writing", int64(id), id)
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	return pm.writePageUnsafe(id, page)
}

// Sync flushes any buffered writes to disk
func (pm *PageManager) Sync() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.file.Sync()
}

// AllocatePage allocates a new Page (from freelist or grows file)
func (pm *PageManager) AllocatePage() (base.PageID, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Try freelist first
	id := pm.allocate()
	if id != 0 {
		return id, nil
	}

	// Grow file - use atomic numPages counter (includes uncommitted allocations)
	// Atomically increment and get the new page ID
	id = base.PageID(pm.numPages.Add(1) - 1)

	// Initialize empty Page
	emptyPage := &base.Page{}
	if err := pm.writePageUnsafe(id, emptyPage); err != nil {
		return 0, err
	}

	return id, nil
}

// FreePage adds a Page to the freelist
func (pm *PageManager) FreePage(id base.PageID) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.free(id)
	return nil
}

// FreePending adds pages to the pending freelist at the given transaction ID
func (pm *PageManager) FreePending(txnID uint64, pageIDs []base.PageID) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.freePending(txnID, pageIDs)
	return nil
}

// ReleasePages moves pages from pending to free for all transactions < minTxnID
func (pm *PageManager) ReleasePages(minTxnID uint64) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.release(minTxnID)
}

// PreventAllocationUpTo prevents allocation of pages freed up to and including the specified txnID
func (pm *PageManager) PreventAllocationUpTo(txnID uint64) {
	// We want to hold a lock rather than use an atomic because this is held by
	// DB during checkpoint.
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.preventUpToTxnID = txnID
}

// AllowAllAllocations clears the allocation prevention
func (pm *PageManager) AllowAllAllocations() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.preventUpToTxnID = 0
}

// FreePendingRelocated adds a relocated page version to the pending map
func (pm *PageManager) FreePendingRelocated(txnID uint64, originalPageID base.PageID, relocatedPageID base.PageID) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.pendingVersions[txnID] = append(pm.pendingVersions[txnID], PendingVersion{
		OriginalPageID: originalPageID,
		PhysicalPageID: relocatedPageID,
	})
	return nil
}

// GetLatestVisible returns the relocated Page ID for the latest version visible to txnID
func (pm *PageManager) GetLatestVisible(originalPageID base.PageID, maxTxnID uint64) (base.PageID, uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var latestTxnID uint64
	var latestPageID base.PageID

	// Search pendingVersions for latest visible relocated version
	for txnID, versions := range pm.pendingVersions {
		if txnID > maxTxnID {
			continue
		}
		for _, v := range versions {
			if v.OriginalPageID == originalPageID && txnID > latestTxnID {
				latestTxnID = txnID
				latestPageID = v.PhysicalPageID
			}
		}
	}

	return latestPageID, latestTxnID
}

// GetMeta returns the current metadata
func (pm *PageManager) GetMeta() base.MetaPage {
	return pm.activeMeta.Load().Meta
}

// GetSnapshot returns a COPY of the bundled metadata and root pointer atomically
// Returns by value to prevent data races with concurrent PutSnapshot updates
func (pm *PageManager) GetSnapshot() Snapshot {
	return *pm.activeMeta.Load()
}

// PutSnapshot updates the metadata and root pointer, persists metadata to disk
// Does NOT make it visible to readers - call CommitSnapshot after fsync
func (pm *PageManager) PutSnapshot(meta base.MetaPage, root *base.Node) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Sync NumPages from atomic counter (may have uncommitted allocations)
	meta.NumPages = pm.numPages.Load()

	// Update checksum (after NumPages sync)
	meta.Checksum = meta.CalculateChecksum()

	// Determine which page to write to based on TxID
	metaPageID := base.PageID(meta.TxID % 2)

	// Write to disk
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)
	if err := pm.writePageUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// Update inactive in-memory copy with both meta and root (don't swap pointer yet)
	if metaPageID == 0 {
		pm.meta0.Meta = meta
		pm.meta0.Root = root
	} else {
		pm.meta1.Meta = meta
		pm.meta1.Root = root
	}

	return nil
}

// CommitSnapshot atomically makes the last PutSnapshot visible to readers
// Call AFTER fsync to ensure durability before visibility
// Lock-free: single writer guarantee + atomic swap
func (pm *PageManager) CommitSnapshot() {
	// Swap to the meta with higher or equal TxID
	// Use >= to handle initial case where both metas have TxID=0
	if pm.meta0.Meta.TxID >= pm.meta1.Meta.TxID {
		pm.activeMeta.Swap(&pm.meta0)
	} else {
		pm.activeMeta.Swap(&pm.meta1)
	}
}

// Close serializes freelist to disk and closes the file
func (pm *PageManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Get active meta for reading
	meta := pm.activeMeta.Load().Meta

	// Serialize freelist to disk
	pagesNeeded := pm.pagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxID ensures they're only released after this close() completes
		oldPages := make([]base.PageID, meta.FreelistPages)
		for i := uint64(0); i < meta.FreelistPages; i++ {
			oldPages[i] = meta.FreelistID + base.PageID(i)
		}
		pm.freePending(meta.TxID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = pm.pagesNeeded()

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
	pm.serializeFreelist(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := pm.writePageUnsafe(meta.FreelistID+base.PageID(i), freelistPages[i]); err != nil {
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
	if err := pm.writePageUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// Update in-memory and swap pointer
	if metaPageID == 0 {
		pm.meta0.Meta = meta
		pm.activeMeta.Swap(&pm.meta0)
	} else {
		pm.meta1.Meta = meta
		pm.activeMeta.Swap(&pm.meta1)
	}

	return pm.file.Close()
}

// initializeNewDB creates a new database with dual meta pages and empty freelist
func (pm *PageManager) initializeNewDB() error {
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
	pm.meta0.Meta = meta
	pm.meta0.Root = nil
	pm.meta1.Meta = meta
	pm.meta1.Root = nil
	pm.activeMeta.Store(&pm.meta0)

	// Initialize atomic numPages counter
	pm.numPages.Store(meta.NumPages)

	// Write meta to both disk pages 0 and 1
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)

	if err := pm.WritePage(0, metaPage); err != nil {
		return err
	}
	if err := pm.WritePage(1, metaPage); err != nil {
		return err
	}

	// Write empty freelist to Page 2
	freelistPages := []*base.Page{&base.Page{}}
	pm.serializeFreelist(freelistPages)
	if err := pm.WritePage(2, freelistPages[0]); err != nil {
		return err
	}

	// Fsync to ensure durability
	return pm.file.Sync()
}

// loadExistingDB loads meta and freelist from existing database file
func (pm *PageManager) loadExistingDB() error {
	// Read both meta pages
	page0, err := pm.ReadPage(0)
	if err != nil {
		return err
	}
	page1, err := pm.ReadPage(1)
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
		pm.meta0.Meta = *meta0
		pm.meta0.Root = nil // Will be set by DB.Open()
	}
	if err1 == nil {
		pm.meta1.Meta = *meta1
		pm.meta1.Root = nil // Will be set by DB.Open()
	}

	// Point activeMeta to the one with higher TxID
	if err0 != nil {
		pm.activeMeta.Store(&pm.meta1)
	} else if err1 != nil {
		pm.activeMeta.Store(&pm.meta0)
	} else {
		// Both valid - pick highest TxID
		if meta0.TxID > meta1.TxID {
			pm.activeMeta.Store(&pm.meta0)
		} else {
			pm.activeMeta.Store(&pm.meta1)
		}
	}

	// Load freelist using active meta
	activeMeta := pm.activeMeta.Load()
	freelistPages := make([]*base.Page, activeMeta.Meta.FreelistPages)
	for i := uint64(0); i < activeMeta.Meta.FreelistPages; i++ {
		page, err := pm.ReadPage(activeMeta.Meta.FreelistID + base.PageID(i))
		if err != nil {
			return err
		}
		freelistPages[i] = page
	}
	pm.deserializeFreelist(freelistPages)

	// Release any pending pages that are safe to reclaim
	// On startup, no readers exist, so all pending pages with txnID <= current can be released
	pm.release(activeMeta.Meta.TxID)

	// Initialize atomic numPages counter from active meta
	pm.numPages.Store(activeMeta.Meta.NumPages)

	return nil
}

// writePageUnsafe writes a Page without acquiring the lock (caller must hold lock)
func (pm *PageManager) writePageUnsafe(id base.PageID, page *base.Page) error {
	offset := int64(id) * base.PageSize

	// Get aligned buffer from pool
	buf := pm.bufPool.Get().([]byte)
	defer pm.bufPool.Put(buf)

	// Copy page to aligned buffer
	copy(buf, page.Data[:])

	pm.diskWrites.Add(1) // Track disk write
	n, err := pm.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != base.PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, base.PageSize)
	}

	return nil
}

const (
	// PendingMarker indicates transition from free IDs to pending entries
	PendingMarker = base.PageID(0xFFFFFFFFFFFFFFFF)
)

// allocate returns a free Page ID, or 0 if none available
func (pm *PageManager) allocate() base.PageID {
	// Caller must hold pm.mu
	if len(pm.freePages) == 0 {
		// No free pages available
		// If prevention is active and there are pending pages that could be released,
		// return 0 to force allocation of new pages instead of using recently freed ones
		if pm.preventUpToTxnID > 0 {
			for txnID := range pm.pendingVersions {
				if txnID <= pm.preventUpToTxnID && len(pm.pendingVersions[txnID]) > 0 {
					// There are pending pages that would normally be released
					// Don't wait for them - force new Page allocation
					return 0
				}
			}
		}
		return 0
	}
	// Pop from end
	id := pm.freePages[len(pm.freePages)-1]
	pm.freePages = pm.freePages[:len(pm.freePages)-1]

	// CRITICAL: Remove from pending to prevent double-allocation
	removedCount := 0
	for txnID, versions := range pm.pendingVersions {
		for i := len(versions) - 1; i >= 0; i-- {
			if versions[i].PhysicalPageID == id {
				pm.pendingVersions[txnID] = append(versions[:i], versions[i+1:]...)
				versions = pm.pendingVersions[txnID]
				removedCount++
			}
		}
		if len(pm.pendingVersions[txnID]) == 0 {
			delete(pm.pendingVersions, txnID)
		}
	}

	return id
}

// free adds a Page ID to the free list
func (pm *PageManager) free(id base.PageID) {
	// Caller must hold pm.mu
	// Check if already in free list to prevent duplicates
	for _, existingID := range pm.freePages {
		if existingID == id {
			return
		}
	}

	pm.freePages = append(pm.freePages, id)
	// Keep sorted for deterministic behavior
	for i := len(pm.freePages) - 1; i > 0; i-- {
		if pm.freePages[i] < pm.freePages[i-1] {
			pm.freePages[i], pm.freePages[i-1] = pm.freePages[i-1], pm.freePages[i]
		} else {
			break
		}
	}
}

// freePending adds pages to the pending map at the given transaction ID
func (pm *PageManager) freePending(txnID uint64, pageIDs []base.PageID) {
	// Caller must hold pm.mu
	if len(pageIDs) == 0 {
		return
	}
	for _, id := range pageIDs {
		pm.pendingVersions[txnID] = append(pm.pendingVersions[txnID], PendingVersion{
			OriginalPageID: id,
			PhysicalPageID: id, // Same location for in-place frees
		})
	}
}

// release moves pages from pending to free for all transactions < minTxnID
func (pm *PageManager) release(minTxnID uint64) int {
	// Caller must hold pm.mu
	released := 0
	for txnID, versions := range pm.pendingVersions {
		if txnID < minTxnID {
			for _, v := range versions {
				pm.free(v.PhysicalPageID)
				released++
			}
			delete(pm.pendingVersions, txnID)
		}
	}
	return released
}

// pagesNeeded returns number of pages needed to serialize this freelist
func (pm *PageManager) pagesNeeded() int {
	// Caller must hold pm.mu
	freeBytes := 8 + len(pm.freePages)*8

	pendingBytes := 0
	if len(pm.pendingVersions) > 0 {
		pendingBytes = 8 + 8
		for _, versions := range pm.pendingVersions {
			pendingBytes += 8 + 8 + len(versions)*8
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
func (pm *PageManager) serializeFreelist(pages []*base.Page) {
	// Caller must hold pm.mu
	buf := make([]byte, 0, base.PageSize*len(pages))

	// Write free count
	countBytes := make([]byte, 8)
	*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(pm.freePages))
	buf = append(buf, countBytes...)

	// Write free IDs
	for _, id := range pm.freePages {
		idBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&idBytes[0])) = id
		buf = append(buf, idBytes...)
	}

	// Write pending data if present
	if len(pm.pendingVersions) > 0 {
		// Write marker
		markerBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&markerBytes[0])) = PendingMarker
		buf = append(buf, markerBytes...)

		// Write pending count
		pendingCountBytes := make([]byte, 8)
		*(*uint64)(unsafe.Pointer(&pendingCountBytes[0])) = uint64(len(pm.pendingVersions))
		buf = append(buf, pendingCountBytes...)

		// Sort txnIDs for deterministic serialization
		txnIDs := make([]uint64, 0, len(pm.pendingVersions))
		for txnID := range pm.pendingVersions {
			txnIDs = append(txnIDs, txnID)
		}
		for i := 1; i < len(txnIDs); i++ {
			for j := i; j > 0 && txnIDs[j] < txnIDs[j-1]; j-- {
				txnIDs[j], txnIDs[j-1] = txnIDs[j-1], txnIDs[j]
			}
		}

		// Write each pending entry
		for _, txnID := range txnIDs {
			versions := pm.pendingVersions[txnID]

			// Write txnID
			txnBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&txnBytes[0])) = txnID
			buf = append(buf, txnBytes...)

			// Write Page count
			countBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(versions))
			buf = append(buf, countBytes...)

			// Write PhysicalPageIDs (backward compatible)
			for _, v := range versions {
				pidBytes := make([]byte, 8)
				*(*base.PageID)(unsafe.Pointer(&pidBytes[0])) = v.PhysicalPageID
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
func (pm *PageManager) deserializeFreelist(pages []*base.Page) {
	// Caller must hold pm.mu
	pm.freePages = make([]base.PageID, 0)
	pm.pendingVersions = make(map[uint64][]PendingVersion)

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
		pm.freePages = append(pm.freePages, id)
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
		versions := make([]PendingVersion, 0, pageCount)
		for j := uint64(0); j < pageCount; j++ {
			if offset+8 > len(buf) {
				break
			}
			pageID := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
			versions = append(versions, PendingVersion{
				OriginalPageID: pageID, // Backward compat: same ID
				PhysicalPageID: pageID,
			})
			offset += 8
		}

		if len(versions) > 0 {
			pm.pendingVersions[txnID] = versions
		}
	}
}

// Stats returns disk I/O statistics
func (pm *PageManager) Stats() (reads, writes uint64) {
	return pm.diskReads.Load(), pm.diskWrites.Load()
}

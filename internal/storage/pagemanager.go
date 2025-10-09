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

// PageManager implements PageManager with disk-based storage
type PageManager struct {
	mu   sync.Mutex // Protects meta and freelist access
	file *os.File
	meta base.MetaPage

	// Direct I/O buffer pool
	bufPool *sync.Pool // Pool of aligned []byte buffers for direct I/O

	// Freelist tracking
	freePages        []base.PageID            // sorted array of free Page IDs
	pendingPages     map[uint64][]base.PageID // txnID -> pages freed at that transaction
	preventUpToTxnID uint64                   // temporarily prevent allocation of pages freed up to this txnID (0 = no prevention)

	// Version relocation tracking
	relocations map[base.PageID]map[uint64]base.PageID // originalPageID -> (txnID -> relocatedPageID)

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
		file:         file,
		freePages:    make([]base.PageID, 0),
		pendingPages: make(map[uint64][]base.PageID),
		relocations:  make(map[base.PageID]map[uint64]base.PageID),
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

	// Grow file
	id = base.PageID(pm.meta.NumPages)
	pm.meta.NumPages++

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

// TrackRelocation records that a Page version has been relocated to a new location
func (pm *PageManager) TrackRelocation(originalPageID base.PageID, txnID uint64, relocatedPageID base.PageID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if pm.relocations[originalPageID] == nil {
		pm.relocations[originalPageID] = make(map[uint64]base.PageID)
	}
	pm.relocations[originalPageID][txnID] = relocatedPageID
}

// GetLatestVisible returns the relocated Page ID for the latest version visible to txnID
func (pm *PageManager) GetLatestVisible(originalPageID base.PageID, maxTxnID uint64) (base.PageID, uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	versions := pm.relocations[originalPageID]
	if versions == nil {
		return 0, 0
	}

	// Find latest version where versionTxnID <= maxTxnID
	var latestTxnID uint64
	var latestPageID base.PageID

	for versionTxnID, relocatedPageID := range versions {
		if versionTxnID <= maxTxnID && versionTxnID > latestTxnID {
			latestTxnID = versionTxnID
			latestPageID = relocatedPageID
		}
	}

	return latestPageID, latestTxnID
}

// CleanupVersions removes all relocations for versions older than minReaderTxn
func (pm *PageManager) CleanupVersions(minReaderTxn uint64) []base.PageID {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var toFree []base.PageID

	for originalPageID, versions := range pm.relocations {
		for txnID, relocatedPageID := range versions {
			if txnID < minReaderTxn {
				toFree = append(toFree, relocatedPageID)
				delete(versions, txnID)
			}
		}

		if len(versions) == 0 {
			delete(pm.relocations, originalPageID)
		}
	}

	return toFree
}

// GetMeta returns the current metadata
func (pm *PageManager) GetMeta() base.MetaPage {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.meta
}

// PutMeta updates the metadata and persists it to disk
// Writes to the inactive meta Page and fsyncs for durability
func (pm *PageManager) PutMeta(meta base.MetaPage) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Update checksum
	meta.Checksum = meta.CalculateChecksum()

	// Write to inactive meta Page (alternates based on TxnID)
	// TxnID % 2 determines which Page: 0 or 1
	metaPage := &base.Page{}
	metaPage.WriteMeta(&meta)
	metaPageID := base.PageID(meta.TxnID % 2)

	// Write meta Page to disk (unsafe - already holding lock)
	if err := pm.writePageUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// Only update in-memory meta after successful disk write
	pm.meta = meta

	return nil
}

// Close serializes freelist to disk and closes the file
func (pm *PageManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Serialize freelist to disk
	pagesNeeded := pm.pagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > pm.meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxnID ensures they're only released after this close() completes
		oldPages := make([]base.PageID, pm.meta.FreelistPages)
		for i := uint64(0); i < pm.meta.FreelistPages; i++ {
			oldPages[i] = pm.meta.FreelistID + base.PageID(i)
		}
		pm.freePending(pm.meta.TxnID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = pm.pagesNeeded()

		// Move freelist to new pages at end of file
		pm.meta.FreelistID = base.PageID(pm.meta.NumPages)
		pm.meta.FreelistPages = uint64(pagesNeeded)
		pm.meta.NumPages += uint64(pagesNeeded)
	}

	// Write freelist
	freelistPages := make([]*base.Page, pagesNeeded)
	for i := 0; i < pagesNeeded; i++ {
		freelistPages[i] = &base.Page{}
	}
	pm.serializeFreelist(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := pm.writePageUnsafe(pm.meta.FreelistID+base.PageID(i), freelistPages[i]); err != nil {
			return err
		}
	}

	// Update meta (increment TxnID, recalculate checksum)
	pm.meta.TxnID++
	pm.meta.Checksum = pm.meta.CalculateChecksum()

	// Write meta to alternating Page
	metaPage := &base.Page{}
	metaPage.WriteMeta(&pm.meta)
	metaPageID := base.PageID(pm.meta.TxnID % 2)
	if err := pm.writePageUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	return pm.file.Close()
}

// initializeNewDB creates a new database with dual meta pages and empty freelist
func (pm *PageManager) initializeNewDB() error {
	// Initialize meta Page
	pm.meta = base.MetaPage{
		Magic:           base.MagicNumber,
		Version:         base.FormatVersion,
		PageSize:        base.PageSize,
		RootPageID:      0, // Will be set by BTree
		FreelistID:      2, // Page 2
		FreelistPages:   1, // One freelist Page
		TxnID:           0, // First transaction
		CheckpointTxnID: 0, // No checkpoint yet
		NumPages:        3, // Pages 0-1 (meta), 2 (freelist) reserved
	}
	pm.meta.Checksum = pm.meta.CalculateChecksum()

	// Write meta to both pages 0 and 1
	metaPage := &base.Page{}
	metaPage.WriteMeta(&pm.meta)

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

	// validate and pick the best meta Page
	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	err0 := meta0.Validate()
	err1 := meta1.Validate()

	// Both invalid - corrupted database
	if err0 != nil && err1 != nil {
		return fmt.Errorf("both meta pages corrupted: %v, %v", err0, err1)
	}

	// Pick the valid one with highest TxnID
	if err0 != nil {
		pm.meta = *meta1
	} else if err1 != nil {
		pm.meta = *meta0
	} else {
		// Both valid, pick highest TxnID
		if meta0.TxnID > meta1.TxnID {
			pm.meta = *meta0
		} else {
			pm.meta = *meta1
		}
	}

	// Load freelist
	freelistPages := make([]*base.Page, pm.meta.FreelistPages)
	for i := uint64(0); i < pm.meta.FreelistPages; i++ {
		page, err := pm.ReadPage(pm.meta.FreelistID + base.PageID(i))
		if err != nil {
			return err
		}
		freelistPages[i] = page
	}
	pm.deserializeFreelist(freelistPages)

	// Release any pending pages that are safe to reclaim
	// On startup, no readers exist, so all pending pages with txnID <= current can be released
	pm.release(pm.meta.TxnID)

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
			for txnID := range pm.pendingPages {
				if txnID <= pm.preventUpToTxnID && len(pm.pendingPages[txnID]) > 0 {
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
	for txnID, pageIDs := range pm.pendingPages {
		for i := len(pageIDs) - 1; i >= 0; i-- {
			if pageIDs[i] == id {
				pm.pendingPages[txnID] = append(pageIDs[:i], pageIDs[i+1:]...)
				pageIDs = pm.pendingPages[txnID]
				removedCount++
			}
		}
		if len(pm.pendingPages[txnID]) == 0 {
			delete(pm.pendingPages, txnID)
		}
	}

	// DEBUG: Check if Page still exists in pending after removal
	for txnID, pageIDs := range pm.pendingPages {
		for _, pid := range pageIDs {
			if pid == id {
				panic(fmt.Sprintf("BUG: Allocated PageID %d still in pending[%d] after removal (removed %d occurrences)",
					id, txnID, removedCount))
			}
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
	pm.pendingPages[txnID] = append(pm.pendingPages[txnID], pageIDs...)
}

// release moves pages from pending to free for all transactions < minTxnID
func (pm *PageManager) release(minTxnID uint64) int {
	// Caller must hold pm.mu
	released := 0
	for txnID, pageIDs := range pm.pendingPages {
		if txnID < minTxnID {
			for _, id := range pageIDs {
				pm.free(id)
				released++
			}
			delete(pm.pendingPages, txnID)
		}
	}
	return released
}

// pagesNeeded returns number of pages needed to serialize this freelist
func (pm *PageManager) pagesNeeded() int {
	// Caller must hold pm.mu
	freeBytes := 8 + len(pm.freePages)*8

	pendingBytes := 0
	if len(pm.pendingPages) > 0 {
		pendingBytes = 8 + 8
		for _, pageIDs := range pm.pendingPages {
			pendingBytes += 8 + 8 + len(pageIDs)*8
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
	if len(pm.pendingPages) > 0 {
		// Write marker
		markerBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&markerBytes[0])) = PendingMarker
		buf = append(buf, markerBytes...)

		// Write pending count
		pendingCountBytes := make([]byte, 8)
		*(*uint64)(unsafe.Pointer(&pendingCountBytes[0])) = uint64(len(pm.pendingPages))
		buf = append(buf, pendingCountBytes...)

		// Sort txnIDs for deterministic serialization
		txnIDs := make([]uint64, 0, len(pm.pendingPages))
		for txnID := range pm.pendingPages {
			txnIDs = append(txnIDs, txnID)
		}
		for i := 1; i < len(txnIDs); i++ {
			for j := i; j > 0 && txnIDs[j] < txnIDs[j-1]; j-- {
				txnIDs[j], txnIDs[j-1] = txnIDs[j-1], txnIDs[j]
			}
		}

		// Write each pending entry
		for _, txnID := range txnIDs {
			pageIDs := pm.pendingPages[txnID]

			// Write txnID
			txnBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&txnBytes[0])) = txnID
			buf = append(buf, txnBytes...)

			// Write Page count
			countBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(pageIDs))
			buf = append(buf, countBytes...)

			// Write Page IDs
			for _, pageID := range pageIDs {
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
func (pm *PageManager) deserializeFreelist(pages []*base.Page) {
	// Caller must hold pm.mu
	pm.freePages = make([]base.PageID, 0)
	pm.pendingPages = make(map[uint64][]base.PageID)

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
			pm.pendingPages[txnID] = pageIDs
		}
	}
}

// Stats returns disk I/O statistics
func (pm *PageManager) Stats() (reads, writes uint64) {
	return pm.diskReads.Load(), pm.diskWrites.Load()
}

package storage

import (
	"fmt"
	"os"
	"sync"
	"unsafe"
)

// PageManager implements PageManager with disk-based storage
type PageManager struct {
	mu         sync.Mutex // Protects meta and freelist access
	file       *os.File
	meta       MetaPage
	freelist   *FreeList
	versionMap *VersionMap // Version relocation tracking for old versions needed by long-running readers
}

// NewPageManager opens or creates a database file
func NewPageManager(path string) (*PageManager, error) {
	// Open file with read/write, create if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	dm := &PageManager{
		file:       file,
		freelist:   NewFreeList(),
		versionMap: NewVersionMap(),
	}

	// Check if new file (empty)
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		// New database - initialize
		if err := dm.initializeNewDB(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		// Existing database - load meta and freelist
		if err := dm.loadExistingDB(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return dm, nil
}

// ReadPage reads a Page from disk
func (dm *PageManager) ReadPage(id PageID) (*Page, error) {
	return dm.readPageAt(id)
}

// AllocatePage allocates a new Page (from freelist or grows file)
func (dm *PageManager) AllocatePage() (PageID, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Try freelist first
	id := dm.freelist.Allocate()
	if id != 0 {
		return id, nil
	}

	// Grow file
	id = PageID(dm.meta.NumPages)
	dm.meta.NumPages++

	// Initialize empty Page
	emptyPage := &Page{}
	if err := dm.writePageAtUnsafe(id, emptyPage); err != nil {
		return 0, err
	}

	return id, nil
}

// FreePage adds a Page to the freelist
func (dm *PageManager) FreePage(id PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.freelist.Free(id)
	return nil
}

// FreePending adds pages to the pending freelist at the given transaction ID
func (dm *PageManager) FreePending(txnID uint64, pageIDs []PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.freelist.FreePending(txnID, pageIDs)
	return nil
}

// GetMeta returns the current metadata
func (dm *PageManager) GetMeta() MetaPage {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.meta
}

// PutMeta updates the metadata and persists it to disk
// Writes to the inactive meta Page and fsyncs for durability
func (dm *PageManager) PutMeta(meta MetaPage) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Update checksum
	meta.Checksum = meta.CalculateChecksum()

	// Write to inactive meta Page (alternates based on TxnID)
	// TxnID % 2 determines which Page: 0 or 1
	metaPage := &Page{}
	metaPage.WriteMeta(&meta)
	metaPageID := PageID(meta.TxnID % 2)

	// Write meta Page to disk (unsafe - already holding lock)
	if err := dm.writePageAtUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// Only update in-memory meta after successful disk write
	dm.meta = meta

	return nil
}

func (dm *PageManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Serialize freelist to disk
	pagesNeeded := dm.freelist.PagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > dm.meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxnID ensures they're only released after this close() completes
		oldPages := make([]PageID, dm.meta.FreelistPages)
		for i := uint64(0); i < dm.meta.FreelistPages; i++ {
			oldPages[i] = PageID(dm.meta.FreelistID) + PageID(i)
		}
		dm.freelist.FreePending(dm.meta.TxnID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = dm.freelist.PagesNeeded()

		// Move freelist to new pages at end of file
		dm.meta.FreelistID = PageID(dm.meta.NumPages)
		dm.meta.FreelistPages = uint64(pagesNeeded)
		dm.meta.NumPages += uint64(pagesNeeded)
	}

	// Write freelist
	freelistPages := make([]*Page, pagesNeeded)
	for i := 0; i < pagesNeeded; i++ {
		freelistPages[i] = &Page{}
	}
	dm.freelist.Serialize(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := dm.writePageAtUnsafe(PageID(dm.meta.FreelistID)+PageID(i), freelistPages[i]); err != nil {
			return err
		}
	}

	// Update meta (increment TxnID, recalculate checksum)
	dm.meta.TxnID++
	dm.meta.Checksum = dm.meta.CalculateChecksum()

	// Write meta to alternating Page
	metaPage := &Page{}
	metaPage.WriteMeta(&dm.meta)
	metaPageID := PageID(dm.meta.TxnID % 2)
	if err := dm.writePageAtUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	return dm.file.Close()
}

// initializeNewDB creates a new database with dual meta pages and empty freelist
func (dm *PageManager) initializeNewDB() error {
	// Initialize meta Page
	dm.meta = MetaPage{
		Magic:           MagicNumber,
		Version:         FormatVersion,
		PageSize:        PageSize,
		RootPageID:      0, // Will be set by BTree
		FreelistID:      2, // Page 2
		FreelistPages:   1, // One freelist Page
		TxnID:           0, // First transaction
		CheckpointTxnID: 0, // No checkpoint yet
		NumPages:        3, // Pages 0-1 (meta), 2 (freelist) reserved
	}
	dm.meta.Checksum = dm.meta.CalculateChecksum()

	// Write meta to both pages 0 and 1
	metaPage := &Page{}
	metaPage.WriteMeta(&dm.meta)

	if err := dm.WritePage(0, metaPage); err != nil {
		return err
	}
	if err := dm.WritePage(1, metaPage); err != nil {
		return err
	}

	// Write empty freelist to Page 2
	freelistPages := []*Page{&Page{}}
	dm.freelist.Serialize(freelistPages)
	if err := dm.WritePage(2, freelistPages[0]); err != nil {
		return err
	}

	// Fsync to ensure durability
	return dm.file.Sync()
}

// loadExistingDB loads meta and freelist from existing database file
func (dm *PageManager) loadExistingDB() error {
	// Read both meta pages
	page0, err := dm.readPageAt(0)
	if err != nil {
		return err
	}
	page1, err := dm.readPageAt(1)
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
		dm.meta = *meta1
	} else if err1 != nil {
		dm.meta = *meta0
	} else {
		// Both valid, pick highest TxnID
		if meta0.TxnID > meta1.TxnID {
			dm.meta = *meta0
		} else {
			dm.meta = *meta1
		}
	}

	// Load freelist
	freelistPages := make([]*Page, dm.meta.FreelistPages)
	for i := uint64(0); i < dm.meta.FreelistPages; i++ {
		page, err := dm.readPageAt(dm.meta.FreelistID + PageID(i))
		if err != nil {
			return err
		}
		freelistPages[i] = page
	}
	dm.freelist.Deserialize(freelistPages)

	// Release any pending pages that are safe to reclaim
	// On startup, no readers exist, so all pending pages with txnID <= current can be released
	dm.freelist.Release(dm.meta.TxnID)

	return nil
}

// readPageAt reads a Page from a specific offset
// Note: Caller should check db.wal.pages before calling this to avoid reading stale data
func (dm *PageManager) readPageAt(id PageID) (*Page, error) {
	return dm.ReadPageAtUnsafe(id)
}

// ReadPageAtUnsafe reads a Page from disk without WAL latch check
// Used during checkpoint to read old disk versions before overwriting
func (dm *PageManager) ReadPageAtUnsafe(id PageID) (*Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(id) * PageSize
	page1 := &Page{}
	n, err := dm.file.ReadAt(page1.Data[:], offset)
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, PageSize)
	}

	return page1, nil
}

// WritePage writes a Page to a specific offset (with locking)
func (dm *PageManager) WritePage(id PageID, page *Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	return dm.writePageAtUnsafe(id, page)
}

// writePageAtUnsafe writes a Page without acquiring the lock (caller must hold lock)
func (dm *PageManager) writePageAtUnsafe(id PageID, page1 *Page) error {
	offset := int64(id) * PageSize
	n, err := dm.file.WriteAt(page1.Data[:], offset)
	if err != nil {
		return err
	}
	if n != PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, PageSize)
	}

	return nil
}

const (
	// PendingMarker indicates transition from free IDs to pending entries
	PendingMarker = PageID(0xFFFFFFFFFFFFFFFF)
)

// FreeList tracks free pages for reuse
type FreeList struct {
	ids              []PageID            // sorted array of free Page IDs
	pending          map[uint64][]PageID // txnID -> pages freed at that transaction
	preventUpToTxnID uint64              // temporarily prevent allocation of pages freed up to this txnID (0 = no prevention)
}

// NewFreeList creates an empty freelist
func NewFreeList() *FreeList {
	return &FreeList{
		ids:     make([]PageID, 0),
		pending: make(map[uint64][]PageID),
	}
}

// Allocate returns a free Page ID, or 0 if none available
func (f *FreeList) Allocate() PageID {
	if len(f.ids) == 0 {
		// No free pages available
		// If prevention is active and there are pending pages that could be released,
		// return 0 to force allocation of new pages instead of using recently freed ones
		if f.preventUpToTxnID > 0 {
			for txnID := range f.pending {
				if txnID <= f.preventUpToTxnID && len(f.pending[txnID]) > 0 {
					// There are pending pages that would normally be released
					// Don't wait for them - force new Page allocation
					return 0
				}
			}
		}
		return 0
	}
	// Pop from end
	id := f.ids[len(f.ids)-1]
	f.ids = f.ids[:len(f.ids)-1]

	// CRITICAL: Remove from pending to prevent double-allocation
	// If this Page is in pending from a previous transaction, remove it
	// to prevent the background releaser from adding it back to free
	removedCount := 0
	for txnID, pageIDs := range f.pending {
		// Remove ALL occurrences of id from this txnID's pending list
		// Iterate backwards to handle removal correctly
		for i := len(pageIDs) - 1; i >= 0; i-- {
			if pageIDs[i] == id {
				// Remove from pending
				f.pending[txnID] = append(pageIDs[:i], pageIDs[i+1:]...)
				pageIDs = f.pending[txnID] // Update reference after modification
				removedCount++
			}
		}
		// Clean up empty entries
		if len(f.pending[txnID]) == 0 {
			delete(f.pending, txnID)
		}
	}

	// DEBUG: Check if Page still exists in pending after removal
	for txnID, pageIDs := range f.pending {
		for _, pid := range pageIDs {
			if pid == id {
				panic(fmt.Sprintf("BUG: Allocated PageID %d still in pending[%d] after removal (removed %d occurrences)",
					id, txnID, removedCount))
			}
		}
	}

	return id
}

// Free adds a Page ID to the free list
func (f *FreeList) Free(id PageID) {
	// Check if already in free list to prevent duplicates
	for _, existingID := range f.ids {
		if existingID == id {
			return // Already free, don't add duplicate
		}
	}

	f.ids = append(f.ids, id)
	// Keep sorted for deterministic behavior
	for i := len(f.ids) - 1; i > 0; i-- {
		if f.ids[i] < f.ids[i-1] {
			f.ids[i], f.ids[i-1] = f.ids[i-1], f.ids[i]
		} else {
			break
		}
	}
}

// Size returns number of free pages
func (f *FreeList) Size() int {
	return len(f.ids)
}

// FreePending adds pages to the pending map at the given transaction ID.
// These pages are not immediately reusable - they'll be moved to free
// when all readers with txnID < this have finished.
func (f *FreeList) FreePending(txnID uint64, pageIDs []PageID) {
	if len(pageIDs) == 0 {
		return
	}
	f.pending[txnID] = append(f.pending[txnID], pageIDs...)
}

// Release moves pages from pending to free for all transactions < minTxnID.
// Returns the number of pages released.
func (f *FreeList) Release(minTxnID uint64) int {
	released := 0

	// Find all pending entries that can be released
	for txnID, pageIDs := range f.pending {
		if txnID < minTxnID {
			// Move these pages to free list
			for _, id := range pageIDs {
				f.Free(id)
				released++
			}
			// Remove from pending
			delete(f.pending, txnID)
		}
	}

	return released
}

// PendingSize returns the total number of pages in pending state
func (f *FreeList) PendingSize() int {
	total := 0
	for _, pageIDs := range f.pending {
		total += len(pageIDs)
	}
	return total
}

// PagesNeeded returns number of pages needed to Serialize this freelist
func (f *FreeList) PagesNeeded() int {
	// Calculate bytes for free IDs
	freeBytes := 8 + len(f.ids)*8 // count + IDs

	// Calculate bytes for pending entries
	pendingBytes := 0
	if len(f.pending) > 0 {
		pendingBytes = 8 + 8 // marker + pending_count
		for _, pageIDs := range f.pending {
			pendingBytes += 8 + 8 + len(pageIDs)*8 // txnID + count + pageIDs
		}
	}

	totalBytes := freeBytes + pendingBytes

	// Always need at least 1 Page
	if totalBytes == 0 {
		return 1
	}

	// Calculate pages needed (accounting for count per Page)
	// Each Page has 8 bytes overhead for count
	pagesNeeded := 0
	remainingBytes := totalBytes
	for remainingBytes > 0 {
		pagesNeeded++
		// First Page can hold PageSize bytes
		// But we use 8 bytes for count, so PageSize - 8 for data
		if remainingBytes <= PageSize {
			remainingBytes = 0
		} else {
			remainingBytes -= PageSize
		}
	}

	return pagesNeeded
}

// Serialize writes freelist to pages starting at given slice
func (f *FreeList) Serialize(pages []*Page) {
	// Build linear buffer with all data
	buf := make([]byte, 0, PageSize*len(pages))

	// Write free count
	countBytes := make([]byte, 8)
	*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(f.ids))
	buf = append(buf, countBytes...)

	// Write free IDs
	for _, id := range f.ids {
		idBytes := make([]byte, 8)
		*(*PageID)(unsafe.Pointer(&idBytes[0])) = id
		buf = append(buf, idBytes...)
	}

	// Write pending data if present
	if len(f.pending) > 0 {
		// Write marker
		markerBytes := make([]byte, 8)
		*(*PageID)(unsafe.Pointer(&markerBytes[0])) = PendingMarker
		buf = append(buf, markerBytes...)

		// Write pending count
		pendingCountBytes := make([]byte, 8)
		*(*uint64)(unsafe.Pointer(&pendingCountBytes[0])) = uint64(len(f.pending))
		buf = append(buf, pendingCountBytes...)

		// Sort txnIDs for deterministic serialization
		txnIDs := make([]uint64, 0, len(f.pending))
		for txnID := range f.pending {
			txnIDs = append(txnIDs, txnID)
		}
		// Simple insertion sort for small maps
		for i := 1; i < len(txnIDs); i++ {
			for j := i; j > 0 && txnIDs[j] < txnIDs[j-1]; j-- {
				txnIDs[j], txnIDs[j-1] = txnIDs[j-1], txnIDs[j]
			}
		}

		// Write each pending entry
		for _, txnID := range txnIDs {
			pageIDs := f.pending[txnID]

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
				*(*PageID)(unsafe.Pointer(&pidBytes[0])) = pageID
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

// Deserialize reads freelist from pages
func (f *FreeList) Deserialize(pages []*Page) {
	f.ids = make([]PageID, 0)
	f.pending = make(map[uint64][]PageID)

	// Build linear buffer from pages
	buf := make([]byte, 0, PageSize*len(pages))
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
		id := *(*PageID)(unsafe.Pointer(&buf[offset]))
		f.ids = append(f.ids, id)
		offset += 8
	}

	// Check for pending marker
	if offset+8 > len(buf) {
		return
	}
	marker := *(*PageID)(unsafe.Pointer(&buf[offset]))
	if marker != PendingMarker {
		return // No pending data
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
		pageIDs := make([]PageID, 0, pageCount)
		for j := uint64(0); j < pageCount; j++ {
			if offset+8 > len(buf) {
				break
			}
			pageID := *(*PageID)(unsafe.Pointer(&buf[offset]))
			pageIDs = append(pageIDs, pageID)
			offset += 8
		}

		if len(pageIDs) > 0 {
			f.pending[txnID] = pageIDs
		}
	}
}

// PreventAllocationUpTo prevents allocation of pages freed up to and including the specified txnID.
// This is used during checkpoint to prevent allocating pages that were freed by transactions
// being checkpointed, as those pages might still be needed by readers at older transaction IDs.
func (f *FreeList) PreventAllocationUpTo(txnID uint64) {
	f.preventUpToTxnID = txnID
}

// AllowAllAllocations clears the allocation prevention, allowing all free pages to be allocated again.
func (f *FreeList) AllowAllAllocations() {
	f.preventUpToTxnID = 0
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
	relocations map[PageID]map[uint64]PageID
	mu          sync.RWMutex
}

// NewVersionMap creates a new version relocation tracker
func NewVersionMap() *VersionMap {
	return &VersionMap{
		relocations: make(map[PageID]map[uint64]PageID),
	}
}

// Track records that a Page version has been relocated to a new location
func (vm *VersionMap) Track(originalPageID PageID, txnID uint64, relocatedPageID PageID) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.relocations[originalPageID] == nil {
		vm.relocations[originalPageID] = make(map[uint64]PageID)
	}
	vm.relocations[originalPageID][txnID] = relocatedPageID
}

// Get returns the relocated Page ID for a specific version, or 0 if not relocated
func (vm *VersionMap) Get(originalPageID PageID, txnID uint64) PageID {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if versions := vm.relocations[originalPageID]; versions != nil {
		return versions[txnID]
	}
	return 0
}

// GetLatestVisible returns the relocated Page ID for the latest version visible to txnID,
// or 0 if no visible version is relocated. Used for MVCC snapshot isolation.
func (vm *VersionMap) GetLatestVisible(originalPageID PageID, maxTxnID uint64) (PageID, uint64) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	versions := vm.relocations[originalPageID]
	if versions == nil {
		return 0, 0
	}

	// Find latest version where versionTxnID <= maxTxnID
	var latestTxnID uint64
	var latestPageID PageID

	for versionTxnID, relocatedPageID := range versions {
		if versionTxnID <= maxTxnID && versionTxnID > latestTxnID {
			latestTxnID = versionTxnID
			latestPageID = relocatedPageID
		}
	}

	return latestPageID, latestTxnID
}

// Remove removes a relocation entry (called when version is no longer needed)
func (vm *VersionMap) Remove(originalPageID PageID, txnID uint64) {
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
func (vm *VersionMap) Cleanup(minReaderTxn uint64) []PageID {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	var toFree []PageID

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

// GetLatestVisible returns the relocated Page ID for the latest version visible to txnID
func (pm *PageManager) GetLatestVisible(originalPageID PageID, maxTxnID uint64) (PageID, uint64) {
	return pm.versionMap.GetLatestVisible(originalPageID, maxTxnID)
}

// CleanupVersions removes all relocations for versions older than minReaderTxn
func (pm *PageManager) CleanupVersions(minReaderTxn uint64) []PageID {
	return pm.versionMap.Cleanup(minReaderTxn)
}

// TrackRelocation records that a Page version has been relocated to a new location
func (pm *PageManager) TrackRelocation(originalPageID PageID, txnID uint64, relocatedPageID PageID) {
	pm.versionMap.Track(originalPageID, txnID, relocatedPageID)
}

// PreventAllocationUpTo prevents allocation of pages freed up to and including the specified txnID.
// This is used during checkpoint to prevent allocating pages that were freed by transactions
// being checkpointed, as those pages might still be needed by readers at older transaction IDs.
func (pm *PageManager) PreventAllocationUpTo(txnID uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.freelist.PreventAllocationUpTo(txnID)
}

// AllowAllAllocations clears the allocation prevention, allowing all free pages to be allocated again.
func (pm *PageManager) AllowAllAllocations() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.freelist.AllowAllAllocations()
}

// Sync flushes any buffered writes to disk
func (pm *PageManager) Sync() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.file.Sync()
}

// ReleasePages moves pages from pending to free for all transactions < minTxnID
func (pm *PageManager) ReleasePages(minTxnID uint64) int {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	return pm.freelist.Release(minTxnID)
}

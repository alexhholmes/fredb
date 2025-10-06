package fredb

import (
	"fmt"
	"os"
	"sync"

	"fredb/internal"
)

// PageManager implements PageManager with disk-based storage
type PageManager struct {
	mu       sync.Mutex // Protects meta and freelist access
	file     *os.File
	meta     internal.MetaPage
	freelist *FreeList
}

// NewPageManager opens or creates a database file
func NewPageManager(path string) (*PageManager, error) {
	// Open file with read/write, create if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	dm := &PageManager{
		file:     file,
		freelist: NewFreeList(),
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
func (dm *PageManager) ReadPage(id internal.PageID) (*internal.Page, error) {
	return dm.readPageAt(id)
}

// AllocatePage allocates a new Page (from freelist or grows file)
func (dm *PageManager) AllocatePage() (internal.PageID, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Try freelist first
	id := dm.freelist.Allocate()
	if id != 0 {
		return id, nil
	}

	// Grow file
	id = internal.PageID(dm.meta.NumPages)
	dm.meta.NumPages++

	// Initialize empty Page
	emptyPage := &internal.Page{}
	if err := dm.writePageAtUnsafe(id, emptyPage); err != nil {
		return 0, err
	}

	return id, nil
}

// FreePage adds a Page to the freelist
func (dm *PageManager) FreePage(id internal.PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.freelist.Free(id)
	return nil
}

// FreePending adds pages to the pending freelist at the given transaction ID
func (dm *PageManager) FreePending(txnID uint64, pageIDs []internal.PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.freelist.FreePending(txnID, pageIDs)
	return nil
}

// GetMeta returns the current metadata
func (dm *PageManager) GetMeta() internal.MetaPage {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.meta
}

// PutMeta updates the metadata and persists it to disk
// Writes to the inactive meta Page and fsyncs for durability
func (dm *PageManager) PutMeta(meta internal.MetaPage) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Update checksum
	meta.Checksum = meta.CalculateChecksum()

	// Write to inactive meta Page (alternates based on TxnID)
	// TxnID % 2 determines which Page: 0 or 1
	metaPage := &internal.Page{}
	metaPage.WriteMeta(&meta)
	metaPageID := internal.PageID(meta.TxnID % 2)

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
		oldPages := make([]internal.PageID, dm.meta.FreelistPages)
		for i := uint64(0); i < dm.meta.FreelistPages; i++ {
			oldPages[i] = internal.PageID(dm.meta.FreelistID) + internal.PageID(i)
		}
		dm.freelist.FreePending(dm.meta.TxnID, oldPages)

		// Recalculate pages needed after adding old freelist pages to pending
		pagesNeeded = dm.freelist.PagesNeeded()

		// Move freelist to new pages at end of file
		dm.meta.FreelistID = internal.PageID(dm.meta.NumPages)
		dm.meta.FreelistPages = uint64(pagesNeeded)
		dm.meta.NumPages += uint64(pagesNeeded)
	}

	// Write freelist
	freelistPages := make([]*internal.Page, pagesNeeded)
	for i := 0; i < pagesNeeded; i++ {
		freelistPages[i] = &internal.Page{}
	}
	dm.freelist.Serialize(freelistPages)

	for i := 0; i < pagesNeeded; i++ {
		if err := dm.writePageAtUnsafe(internal.PageID(dm.meta.FreelistID)+internal.PageID(i), freelistPages[i]); err != nil {
			return err
		}
	}

	// Update meta (increment TxnID, recalculate checksum)
	dm.meta.TxnID++
	dm.meta.Checksum = dm.meta.CalculateChecksum()

	// Write meta to alternating Page
	metaPage := &internal.Page{}
	metaPage.WriteMeta(&dm.meta)
	metaPageID := internal.PageID(dm.meta.TxnID % 2)
	if err := dm.writePageAtUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	return dm.file.Close()
}

// initializeNewDB creates a new database with dual meta pages and empty freelist
func (dm *PageManager) initializeNewDB() error {
	// Initialize meta Page
	dm.meta = internal.MetaPage{
		Magic:           internal.MagicNumber,
		Version:         internal.FormatVersion,
		PageSize:        internal.PageSize,
		RootPageID:      0, // Will be set by BTree
		FreelistID:      2, // Page 2
		FreelistPages:   1, // One freelist Page
		TxnID:           0, // First transaction
		CheckpointTxnID: 0, // No checkpoint yet
		NumPages:        3, // Pages 0-1 (meta), 2 (freelist) reserved
	}
	dm.meta.Checksum = dm.meta.CalculateChecksum()

	// Write meta to both pages 0 and 1
	metaPage := &internal.Page{}
	metaPage.WriteMeta(&dm.meta)

	if err := dm.WritePage(0, metaPage); err != nil {
		return err
	}
	if err := dm.WritePage(1, metaPage); err != nil {
		return err
	}

	// Write empty freelist to Page 2
	freelistPages := []*internal.Page{&internal.Page{}}
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
	freelistPages := make([]*internal.Page, dm.meta.FreelistPages)
	for i := uint64(0); i < dm.meta.FreelistPages; i++ {
		page, err := dm.readPageAt(dm.meta.FreelistID + internal.PageID(i))
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
func (dm *PageManager) readPageAt(id internal.PageID) (*internal.Page, error) {
	return dm.readPageAtUnsafe(id)
}

// readPageAtUnsafe reads a Page from disk without WAL latch check
// Used during checkpoint to read old disk versions before overwriting
func (dm *PageManager) readPageAtUnsafe(id internal.PageID) (*internal.Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(id) * internal.PageSize
	page := &internal.Page{}
	n, err := dm.file.ReadAt(page.Data[:], offset)
	if err != nil {
		return nil, err
	}
	if n != internal.PageSize {
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, internal.PageSize)
	}

	return page, nil
}

// WritePage writes a Page to a specific offset (with locking)
func (dm *PageManager) WritePage(id internal.PageID, page *internal.Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	return dm.writePageAtUnsafe(id, page)
}

// writePageAtUnsafe writes a Page without acquiring the lock (caller must hold lock)
func (dm *PageManager) writePageAtUnsafe(id internal.PageID, page *internal.Page) error {
	offset := int64(id) * internal.PageSize
	n, err := dm.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return err
	}
	if n != internal.PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, internal.PageSize)
	}

	return nil
}

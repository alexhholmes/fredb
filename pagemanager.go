package fredb

import (
	"fmt"
	"os"
	"sync"
)

// PageManager handles disk I/O
type PageManager interface {
	ReadPage(id PageID) (*Page, error)
	WritePage(id PageID, page *Page) error
	AllocatePage() (PageID, error)
	FreePage(id PageID) error
	FreePending(txnID uint64, pageIDs []PageID) error
	GetMeta() *MetaPage
	PutMeta(meta *MetaPage) error
	Close() error
}

var _ PageManager = (*DiskPageManager)(nil)

// DiskPageManager implements PageManager with disk-based storage
type DiskPageManager struct {
	mu       sync.Mutex // Protects meta and freelist access
	file     *os.File
	meta     MetaPage
	freelist *FreeList
	wal      *WAL // Write-ahead log for durability
}

// NewDiskPageManager opens or creates a database file
func NewDiskPageManager(path string) (*DiskPageManager, error) {
	// Open file with read/write, create if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	// Open WAL
	walPath := path + ".wal"
	wal, err := NewWAL(walPath)
	if err != nil {
		file.Close()
		return nil, err
	}

	dm := &DiskPageManager{
		file:     file,
		freelist: NewFreeList(),
		wal:      wal,
	}

	// Check if new file (empty)
	info, err := file.Stat()
	if err != nil {
		wal.Close()
		file.Close()
		return nil, err
	}

	if info.Size() == 0 {
		// New database - initialize
		if err := dm.initializeNewDB(); err != nil {
			wal.Close()
			file.Close()
			return nil, err
		}
	} else {
		// Existing database - load meta and freelist
		if err := dm.loadExistingDB(); err != nil {
			wal.Close()
			file.Close()
			return nil, err
		}
		// TODO: Replay WAL after loading (Phase 6)
	}

	return dm, nil
}

// ReadPage reads a page from disk
func (dm *DiskPageManager) ReadPage(id PageID) (*Page, error) {
	return dm.readPageAt(id)
}

// AllocatePage allocates a new page (from freelist or grows file)
func (dm *DiskPageManager) AllocatePage() (PageID, error) {
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

	// Initialize empty page
	emptyPage := &Page{}
	if err := dm.writePageAtUnsafe(id, emptyPage); err != nil {
		return 0, err
	}

	return id, nil
}

// FreePage adds a page to the freelist
func (dm *DiskPageManager) FreePage(id PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.freelist.Free(id)
	return nil
}

// FreePending adds pages to the pending freelist at the given transaction ID
func (dm *DiskPageManager) FreePending(txnID uint64, pageIDs []PageID) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.freelist.FreePending(txnID, pageIDs)
	return nil
}

// AppendPageWAL writes a page to the WAL
func (dm *DiskPageManager) AppendPageWAL(txnID uint64, pageID PageID, page *Page) error {
	return dm.wal.AppendPage(txnID, pageID, page)
}

// CommitWAL writes a commit marker to the WAL
func (dm *DiskPageManager) CommitWAL(txnID uint64) error {
	return dm.wal.AppendCommit(txnID)
}

// SyncWAL fsyncs the WAL
func (dm *DiskPageManager) SyncWAL() error {
	return dm.wal.Sync()
}

// TruncateWAL truncates the WAL up to the given transaction ID
func (dm *DiskPageManager) TruncateWAL(upToTxnID uint64) error {
	return dm.wal.Truncate(upToTxnID)
}

// ReplayWAL replays WAL transactions from the given transaction ID
func (dm *DiskPageManager) ReplayWAL(fromTxnID uint64, applyFn func(PageID, *Page) error) error {
	return dm.wal.Replay(fromTxnID, applyFn)
}

// GetMeta returns the current metadata
func (dm *DiskPageManager) GetMeta() *MetaPage {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Return a copy to prevent external modifications
	metaCopy := dm.meta
	return &metaCopy
}

// PutMeta updates the metadata and persists it to disk
// Writes to the inactive meta page and fsyncs for durability
func (dm *DiskPageManager) PutMeta(meta *MetaPage) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Update checksum
	meta.Checksum = meta.CalculateChecksum()

	// Write to inactive meta page (alternates based on TxnID)
	// TxnID % 2 determines which page: 0 or 1
	metaPage := &Page{}
	metaPage.WriteMeta(meta)
	metaPageID := PageID(meta.TxnID % 2)

	// Write meta page to disk (unsafe - already holding lock)
	if err := dm.writePageAtUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// Only update in-memory meta after successful disk write
	dm.meta = *meta

	return nil
}

func (dm *DiskPageManager) Close() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Serialize freelist to disk
	pagesNeeded := dm.freelist.PagesNeeded()

	// If freelist grew beyond reserved space, relocate to end to avoid overwriting data
	if uint64(pagesNeeded) > dm.meta.FreelistPages {
		// Mark old freelist pages as pending (not immediately reusable)
		// Using current TxnID ensures they're only released after this Close() completes
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

	// Write meta to alternating page
	metaPage := &Page{}
	metaPage.WriteMeta(&dm.meta)
	metaPageID := PageID(dm.meta.TxnID % 2)
	if err := dm.writePageAtUnsafe(metaPageID, metaPage); err != nil {
		return err
	}

	// TODO: Reimplement with batching once WAL is implemented
	// Fsync disabled for performance during development
	// if err := dm.file.Sync(); err != nil {
	// 	return err
	// }

	// Close WAL
	if dm.wal != nil {
		if err := dm.wal.Close(); err != nil {
			return err
		}
	}

	return dm.file.Close()
}

// initializeNewDB creates a new database with dual meta pages and empty freelist
func (dm *DiskPageManager) initializeNewDB() error {
	// Initialize meta page
	dm.meta = MetaPage{
		Magic:           MagicNumber,
		Version:         FormatVersion,
		PageSize:        PageSize,
		RootPageID:      0, // Will be set by BTree
		FreelistID:      2, // Page 2
		FreelistPages:   1, // One freelist page
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

	// Write empty freelist to page 2
	freelistPages := []*Page{&Page{}}
	dm.freelist.Serialize(freelistPages)
	if err := dm.WritePage(2, freelistPages[0]); err != nil {
		return err
	}

	// Fsync to ensure durability
	return dm.file.Sync()
}

// loadExistingDB loads meta and freelist from existing database file
func (dm *DiskPageManager) loadExistingDB() error {
	// Read both meta pages
	page0, err := dm.readPageAt(0)
	if err != nil {
		return err
	}
	page1, err := dm.readPageAt(1)
	if err != nil {
		return err
	}

	// Validate and pick best meta page
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
		page, err := dm.readPageAt(PageID(dm.meta.FreelistID + PageID(i)))
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

// readPageAt reads a page from a specific offset
func (dm *DiskPageManager) readPageAt(id PageID) (*Page, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(id) * PageSize
	page := &Page{}
	n, err := dm.file.ReadAt(page.data[:], offset)
	if err != nil {
		return nil, err
	}
	if n != PageSize {
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, PageSize)
	}

	return page, nil
}

// WritePage writes a page to a specific offset (with locking)
func (dm *DiskPageManager) WritePage(id PageID, page *Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	return dm.writePageAtUnsafe(id, page)
}

// writePageAtUnsafe writes a page without acquiring the lock (caller must hold lock)
func (dm *DiskPageManager) writePageAtUnsafe(id PageID, page *Page) error {
	offset := int64(id) * PageSize
	n, err := dm.file.WriteAt(page.data[:], offset)
	if err != nil {
		return err
	}
	if n != PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, PageSize)
	}

	return nil
}

// Close flushes all changes and closes the database file

var _ PageManager = (*InMemoryPageManager)(nil)

// InMemoryPageManager implements PageManager with in-memory storage
type InMemoryPageManager struct {
	pages      map[PageID]*Page
	nextPageID PageID
	meta       MetaPage
}

// NewInMemoryPageManager creates a new in-memory page manager
func NewInMemoryPageManager() *InMemoryPageManager {
	meta := MetaPage{
		Magic:           MagicNumber,
		Version:         FormatVersion,
		PageSize:        PageSize,
		RootPageID:      0, // Will be set by BTree
		FreelistID:      0, // Not used in memory
		FreelistPages:   0, // Not used in memory
		TxnID:           0, // First transaction
		CheckpointTxnID: 0, // Not used in memory
		NumPages:        1, // Start at 1
	}
	meta.Checksum = meta.CalculateChecksum()

	return &InMemoryPageManager{
		pages:      make(map[PageID]*Page),
		nextPageID: 1, // Start at 1, 0 reserved for nil
		meta:       meta,
	}
}

// ReadPage reads a page from memory
func (m *InMemoryPageManager) ReadPage(id PageID) (*Page, error) {
	page, exists := m.pages[id]
	if !exists {
		return nil, fmt.Errorf("page %d not found", id)
	}

	// Return a copy to simulate disk read
	pageCopy := &Page{}
	copy(pageCopy.data[:], page.data[:])
	return pageCopy, nil
}

// WritePage writes a page to memory
func (m *InMemoryPageManager) WritePage(id PageID, page *Page) error {
	if page == nil {
		return fmt.Errorf("cannot write nil page")
	}

	// Store a copy to simulate disk write
	pageCopy := &Page{}
	copy(pageCopy.data[:], page.data[:])
	m.pages[id] = pageCopy
	return nil
}

// AllocatePage allocates a new page
func (m *InMemoryPageManager) AllocatePage() (PageID, error) {
	id := m.nextPageID
	m.nextPageID++
	m.meta.NumPages++

	// Initialize empty page
	m.pages[id] = &Page{}
	return id, nil
}

// FreePage marks a page as free (just removes from map)
func (m *InMemoryPageManager) FreePage(id PageID) error {
	delete(m.pages, id)
	return nil
}

// FreePending is a no-op for in-memory storage (no pending management needed)
func (m *InMemoryPageManager) FreePending(txnID uint64, pageIDs []PageID) error {
	// No-op: in-memory doesn't need pending management
	return nil
}

// GetMeta returns the current metadata
func (m *InMemoryPageManager) GetMeta() *MetaPage {
	return &m.meta
}

// PutMeta updates the metadata
func (m *InMemoryPageManager) PutMeta(meta *MetaPage) error {
	m.meta = *meta
	m.meta.Checksum = m.meta.CalculateChecksum()
	return nil
}

// Close closes the in-memory page manager (no-op for memory)
func (m *InMemoryPageManager) Close() error {
	return nil
}

package src

import "fmt"

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
		Magic:         MagicNumber,
		Version:       FormatVersion,
		PageSize:      PageSize,
		RootPageID:    0, // Will be set by BTree
		FreelistID:    0, // Not used in memory
		FreelistPages: 0, // Not used in memory
		TxnID:         0, // First transaction
		NumPages:      1, // Start at 1
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
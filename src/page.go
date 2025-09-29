package src

import (
	"hash/crc32"
	"unsafe"
)

const (
	PageSize = 4096

	LeafPageFlag   uint16 = 0x01
	BranchPageFlag uint16 = 0x02
	MetaPageFlag   uint16 = 0x04

	PageHeaderSize    = 16
	LeafElementSize   = 12
	BranchElementSize = 16

	// MagicNumber for file format identification ("frdb" in hex)
	MagicNumber uint32 = 0x66726462

	FormatVersion uint16 = 1
)

type PageID uint64

// Page is raw disk page
type Page struct {
	data [PageSize]byte
}

// PageHeader represents the fixed-size header at the start of each page
// Layout: [PageID: 8][Flags: 2][NumKeys: 2][Reserved: 4]
type PageHeader struct {
	PageID   PageID // 8 bytes
	Flags    uint16 // 2 bytes (leaf/branch)
	NumKeys  uint16 // 2 bytes
	Reserved uint32 // 4 bytes (for future use)
}

// LeafElement represents metadata for a key-value pair in a leaf page
// Layout: [KeyOffset: 2][KeySize: 2][ValueOffset: 2][ValueSize: 2][Reserved: 4]
type LeafElement struct {
	KeyOffset   uint16 // 2 bytes: offset from data area start
	KeySize     uint16 // 2 bytes
	ValueOffset uint16 // 2 bytes: offset from data area start
	ValueSize   uint16 // 2 bytes
	Reserved    uint32 // 4 bytes
}

// BranchElement represents metadata for a key and child pointer in a branch page
// Layout: [KeyOffset: 2][KeySize: 2][Reserved1: 4][ChildID: 8]
type BranchElement struct {
	KeyOffset uint16 // 2 bytes: offset from data area start
	KeySize   uint16 // 2 bytes
	Reserved1 uint32 // 4 bytes
	ChildID   PageID // 8 bytes
}

// Header returns the page header decoded from page data
func (p *Page) Header() *PageHeader {
	return (*PageHeader)(unsafe.Pointer(&p.data[0]))
}

// LeafElements returns the array of leaf elements starting after the header
func (p *Page) LeafElements() []LeafElement {
	h := p.Header()
	if h.NumKeys == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&p.data[PageHeaderSize])
	return unsafe.Slice((*LeafElement)(ptr), h.NumKeys)
}

// BranchElements returns the array of branch elements starting after the header
func (p *Page) BranchElements() []BranchElement {
	h := p.Header()
	if h.NumKeys == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&p.data[PageHeaderSize])
	return unsafe.Slice((*BranchElement)(ptr), h.NumKeys)
}

// DataAreaStart returns the offset where variable-length data begins
func (p *Page) DataAreaStart() int {
	h := p.Header()
	if h.Flags&LeafPageFlag != 0 {
		return PageHeaderSize + int(h.NumKeys)*LeafElementSize
	}
	return PageHeaderSize + int(h.NumKeys)*BranchElementSize
}

// WriteHeader writes the page header to the page data
func (p *Page) WriteHeader(h *PageHeader) {
	*p.Header() = *h
}

// WriteLeafElement writes a leaf element at the specified index
func (p *Page) WriteLeafElement(idx int, e *LeafElement) {
	ptr := unsafe.Pointer(&p.data[PageHeaderSize+idx*LeafElementSize])
	*(*LeafElement)(ptr) = *e
}

// WriteBranchElement writes a branch element at the specified index
func (p *Page) WriteBranchElement(idx int, e *BranchElement) {
	ptr := unsafe.Pointer(&p.data[PageHeaderSize+idx*BranchElementSize])
	*(*BranchElement)(ptr) = *e
}

// GetKey retrieves a key from the data area given an offset and size
func (p *Page) GetKey(offset, size uint16) ([]byte, error) {
	dataStart := p.DataAreaStart()
	start := dataStart + int(offset)
	end := start + int(size)

	if start < dataStart {
		return nil, ErrInvalidOffset
	}
	if end > PageSize {
		return nil, ErrPageOverflow
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.data[start:end], nil
}

// GetValue retrieves a value from the data area given an offset and size
func (p *Page) GetValue(offset, size uint16) ([]byte, error) {
	dataStart := p.DataAreaStart()
	start := dataStart + int(offset)
	end := start + int(size)

	if start < dataStart {
		return nil, ErrInvalidOffset
	}
	if end > PageSize {
		return nil, ErrPageOverflow
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.data[start:end], nil
}

// WriteBranchFirstChild writes the first child PageID to the start of data area
func (p *Page) WriteBranchFirstChild(childID PageID) {
	dataStart := p.DataAreaStart()
	*(*PageID)(unsafe.Pointer(&p.data[dataStart])) = childID
}

// ReadBranchFirstChild reads the first child PageID from the start of data area
func (p *Page) ReadBranchFirstChild() PageID {
	dataStart := p.DataAreaStart()
	return *(*PageID)(unsafe.Pointer(&p.data[dataStart]))
}

// MetaPage represents database metadata stored in pages 0 and 1
// Layout: [Magic: 4][Version: 2][PageSize: 2][RootPageID: 8][FreelistID: 8][FreelistPages: 8][TxnID: 8][NumPages: 8][Checksum: 4]
// Total: 52 bytes
type MetaPage struct {
	Magic          uint32 // 4 bytes: 0x66726462 ("frdb")
	Version        uint16 // 2 bytes: format version (1)
	PageSize       uint16 // 2 bytes: page size (4096)
	RootPageID     PageID // 8 bytes: root of B-tree
	FreelistID     PageID // 8 bytes: start of freelist
	FreelistPages  uint64 // 8 bytes: number of contiguous freelist pages
	TxnID          uint64 // 8 bytes: transaction counter
	NumPages       uint64 // 8 bytes: total pages allocated
	Checksum       uint32 // 4 bytes: CRC32 of above fields
}

// WriteMeta writes metadata to the page starting at PageHeaderSize
func (p *Page) WriteMeta(m *MetaPage) {
	offset := PageHeaderSize
	ptr := unsafe.Pointer(&p.data[offset])
	*(*MetaPage)(ptr) = *m
}

// ReadMeta reads metadata from the page starting at PageHeaderSize
func (p *Page) ReadMeta() *MetaPage {
	offset := PageHeaderSize
	ptr := unsafe.Pointer(&p.data[offset])
	return (*MetaPage)(ptr)
}

// CalculateChecksum computes CRC32 checksum of all fields except Checksum itself
func (m *MetaPage) CalculateChecksum() uint32 {
	// Create byte slice of all fields except Checksum
	// MetaPage is 52 bytes, Checksum is last 4 bytes, so we hash first 48 bytes
	data := make([]byte, 48)
	offset := 0

	// Magic (4 bytes)
	data[offset] = byte(m.Magic)
	data[offset+1] = byte(m.Magic >> 8)
	data[offset+2] = byte(m.Magic >> 16)
	data[offset+3] = byte(m.Magic >> 24)
	offset += 4

	// Version (2 bytes)
	data[offset] = byte(m.Version)
	data[offset+1] = byte(m.Version >> 8)
	offset += 2

	// PageSize (2 bytes)
	data[offset] = byte(m.PageSize)
	data[offset+1] = byte(m.PageSize >> 8)
	offset += 2

	// RootPageID (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.RootPageID >> (i * 8))
	}
	offset += 8

	// FreelistID (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.FreelistID >> (i * 8))
	}
	offset += 8

	// FreelistPages (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.FreelistPages >> (i * 8))
	}
	offset += 8

	// TxnID (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.TxnID >> (i * 8))
	}
	offset += 8

	// NumPages (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.NumPages >> (i * 8))
	}

	return crc32.ChecksumIEEE(data)
}

// Validate checks if the metadata is valid
func (m *MetaPage) Validate() error {
	if m.Magic != MagicNumber {
		return ErrInvalidMagicNumber
	}
	if m.Version != FormatVersion {
		return ErrInvalidVersion
	}
	if m.PageSize != PageSize {
		return ErrInvalidPageSize
	}
	expectedChecksum := m.CalculateChecksum()
	if m.Checksum != expectedChecksum {
		return ErrInvalidChecksum
	}
	return nil
}

const (
	// FreeListPageCapacity is max number of PageIDs per freelist page
	// (PageSize - 8 bytes for count) / 8 bytes per PageID
	FreeListPageCapacity = (PageSize - 8) / 8
)

// FreeList tracks free pages for reuse
type FreeList struct {
	ids []PageID // sorted array of free page IDs
}

// NewFreeList creates an empty freelist
func NewFreeList() *FreeList {
	return &FreeList{
		ids: make([]PageID, 0),
	}
}

// Allocate returns a free page ID, or 0 if none available
func (f *FreeList) Allocate() PageID {
	if len(f.ids) == 0 {
		return 0
	}
	// Pop from end
	id := f.ids[len(f.ids)-1]
	f.ids = f.ids[:len(f.ids)-1]
	return id
}

// Free adds a page ID to the free list
func (f *FreeList) Free(id PageID) {
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

// PagesNeeded returns number of pages needed to serialize this freelist
func (f *FreeList) PagesNeeded() int {
	if len(f.ids) == 0 {
		return 1
	}
	return (len(f.ids) + FreeListPageCapacity - 1) / FreeListPageCapacity
}

// Serialize writes freelist to pages starting at given slice
func (f *FreeList) Serialize(pages []*Page) {
	offset := 0
	for pageIdx := 0; pageIdx < len(pages); pageIdx++ {
		page := pages[pageIdx]

		// How many IDs can fit in this page?
		remaining := len(f.ids) - offset
		count := remaining
		if count > FreeListPageCapacity {
			count = FreeListPageCapacity
		}

		// Write count at start of page
		*(*uint64)(unsafe.Pointer(&page.data[0])) = uint64(count)

		// Write PageID array
		for i := 0; i < count; i++ {
			dataOffset := 8 + i*8
			*(*PageID)(unsafe.Pointer(&page.data[dataOffset])) = f.ids[offset+i]
		}

		offset += count
	}
}

// Deserialize reads freelist from pages
func (f *FreeList) Deserialize(pages []*Page) {
	f.ids = make([]PageID, 0)

	for _, page := range pages {
		// Read count from start of page
		count := *(*uint64)(unsafe.Pointer(&page.data[0]))

		// Read PageID array
		for i := 0; i < int(count); i++ {
			dataOffset := 8 + i*8
			id := *(*PageID)(unsafe.Pointer(&page.data[dataOffset]))
			f.ids = append(f.ids, id)
		}
	}
}

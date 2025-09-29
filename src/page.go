package src

import (
	"unsafe"
)

const (
	PageSize = 4096

	// Page type flags
	LeafPageFlag   uint16 = 0x01
	BranchPageFlag uint16 = 0x02

	// Page header size
	PageHeaderSize = 16

	// Element sizes
	LeafElementSize   = 12
	BranchElementSize = 16
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
func (p *Page) GetKey(offset, size uint16) []byte {
	dataStart := p.DataAreaStart()
	start := dataStart + int(offset)
	end := start + int(size)
	return p.data[start:end]
}

// GetValue retrieves a value from the data area given an offset and size
func (p *Page) GetValue(offset, size uint16) []byte {
	dataStart := p.DataAreaStart()
	start := dataStart + int(offset)
	end := start + int(size)
	return p.data[start:end]
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

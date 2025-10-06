package fredb

import (
	"hash/crc32"
	"unsafe"
)

const (
	PageSize = 4096

	LeafPageFlag   uint16 = 0x01
	BranchPageFlag uint16 = 0x02

	pageHeaderSize    = 40 // pageID(8) + Flags(2) + NumKeys(2) + Padding(4) + TxnID(8) + _NextLeaf(8) + _PrevLeaf(8)
	leafElementSize   = 12
	branchElementSize = 16

	// MagicNumber for file format identification ("frdb" in hex)
	MagicNumber uint32 = 0x66726462

	FormatVersion uint16 = 1
)

type pageID uint64

// Page is raw disk Page (4096 bytes)
//
// LEAF PAGE LAYOUT:
// ┌─────────────────────────────────────────────────────────────────────┐
// │ header (40 bytes)                                                   │
// │ pageID, Flags, NumKeys, Padding, TxnID, _NextLeaf, _PrevLeaf        │
// ├─────────────────────────────────────────────────────────────────────┤
// │ leafElement[0] (12 bytes)                                           │
// │ KeyOffset, KeySize, ValueOffset, ValueSize, Reserved                │
// ├─────────────────────────────────────────────────────────────────────┤
// │ leafElement[1] (12 bytes)                                           │
// ├─────────────────────────────────────────────────────────────────────┤
// │ ...                                                                 │
// ├─────────────────────────────────────────────────────────────────────┤
// │ leafElement[N-1] (12 bytes)                                         │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Data Area (variable, packed from end backward):                     │
// │   ← key[0] | value[0] | key[1] | value[1] | ... | key[N-1] | val[N] │
// │   Elements grow forward →              Data grows backward ←        │
// └─────────────────────────────────────────────────────────────────────┘
//
// BRANCH PAGE LAYOUT:
// ┌─────────────────────────────────────────────────────────────────────┐
// │ header (40 bytes)                                                   │
// │ pageID, Flags, NumKeys, Padding, TxnID, _NextLeaf, _PrevLeaf        │
// ├─────────────────────────────────────────────────────────────────────┤
// │ branchElement[0] (16 bytes)                                         │
// │ KeyOffset, KeySize, Reserved, ChildID                               │
// ├─────────────────────────────────────────────────────────────────────┤
// │ branchElement[1] (16 bytes)                                         │
// ├─────────────────────────────────────────────────────────────────────┤
// │ ...                                                                 │
// ├─────────────────────────────────────────────────────────────────────┤
// │ branchElement[N-1] (16 bytes)                                       │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Data Area (variable, packed from end backward, reserve last 8):     │
// │   ← key[0] | key[1] | ... | key[N-1]        children[0] (8 bytes)→  │
// │   Elements grow forward →   Data grows backward ← (reserve end 8)   │
// │   branchElement[0..N-1].ChildID stores children[1..N]               │
// │   children[0] is at FIXED location: last 8 bytes (PageSize-8)       │
// └─────────────────────────────────────────────────────────────────────┘
type Page struct {
	data [PageSize]byte
}

// pageHeader represents the fixed-size header at the start of each Page
// Layout: [pageID: 8][Flags: 2][NumKeys: 2][Padding: 4][TxnID: 8][_NextLeaf: 8][_PrevLeaf: 8]
type pageHeader struct {
	PageID    pageID // 8 bytes
	Flags     uint16 // 2 bytes (leaf/branch)
	NumKeys   uint16 // 2 bytes
	Padding   uint32 // 4 bytes (alignment)
	TxnID     uint64 // 8 bytes - transaction that committed this Page version
	_NextLeaf pageID // 8 bytes - next leaf in linked list (0 if none) (Reserved)
	_PrevLeaf pageID // 8 bytes - prev leaf in linked list (0 if none) (Reserved)
}

// leafElement represents metadata for a key-value pair in a leaf Page
// Layout: [KeyOffset: 2][KeySize: 2][ValueOffset: 2][ValueSize: 2][Reserved: 4]
type leafElement struct {
	KeyOffset   uint16 // 2 bytes: offset from data area start
	KeySize     uint16 // 2 bytes
	ValueOffset uint16 // 2 bytes: offset from data area start
	ValueSize   uint16 // 2 bytes
	Reserved    uint32 // 4 bytes
}

// branchElement represents metadata for a routing key and child pointer in a branch Page
// B+ tree: branch nodes only store keys for routing, no values
// Layout: [KeyOffset: 2][KeySize: 2][Reserved: 4][ChildID: 8]
type branchElement struct {
	KeyOffset uint16 // 2 bytes: offset from data area start
	KeySize   uint16 // 2 bytes
	Reserved  uint32 // 4 bytes: unused (for future use or alignment)
	ChildID   pageID // 8 bytes
}

// header returns the Page header decoded from Page data
func (p *Page) header() *pageHeader {
	return (*pageHeader)(unsafe.Pointer(&p.data[0]))
}

// leafElements returns the array of leaf elements starting after the header
func (p *Page) leafElements() []leafElement {
	h := p.header()
	if h.NumKeys == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&p.data[pageHeaderSize])
	return unsafe.Slice((*leafElement)(ptr), h.NumKeys)
}

// branchElements returns the array of branch elements starting after the header
func (p *Page) branchElements() []branchElement {
	h := p.header()
	if h.NumKeys == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&p.data[pageHeaderSize])
	return unsafe.Slice((*branchElement)(ptr), h.NumKeys)
}

// dataAreaStart returns the offset where variable-length data begins
func (p *Page) dataAreaStart() int {
	h := p.header()
	if h.Flags&LeafPageFlag != 0 {
		return pageHeaderSize + int(h.NumKeys)*leafElementSize
	}
	return pageHeaderSize + int(h.NumKeys)*branchElementSize
}

// writeHeader writes the Page header to the Page data
func (p *Page) writeHeader(h *pageHeader) {
	*p.header() = *h
}

// writeLeafElement writes a leaf element at the specified index
func (p *Page) writeLeafElement(idx int, e *leafElement) {
	ptr := unsafe.Pointer(&p.data[pageHeaderSize+idx*leafElementSize])
	*(*leafElement)(ptr) = *e
}

// writeBranchElement writes a branch element at the specified index
func (p *Page) writeBranchElement(idx int, e *branchElement) {
	ptr := unsafe.Pointer(&p.data[pageHeaderSize+idx*branchElementSize])
	*(*branchElement)(ptr) = *e
}

// getKey retrieves a key from the data area given an absolute offset and size
func (p *Page) getKey(offset, size uint16) ([]byte, error) {
	start := int(offset)
	end := start + int(size)

	if start < 0 || end > PageSize {
		return nil, ErrPageOverflow
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.data[start:end], nil
}

// getValue retrieves a value from the data area given an absolute offset and size
func (p *Page) getValue(offset, size uint16) ([]byte, error) {
	start := int(offset)
	end := start + int(size)

	if start < 0 || end > PageSize {
		return nil, ErrPageOverflow
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.data[start:end], nil
}

// writeBranchFirstChild writes the first child pageID to a fixed location at the end of the Page
func (p *Page) writeBranchFirstChild(childID pageID) {
	offset := PageSize - 8
	*(*pageID)(unsafe.Pointer(&p.data[offset])) = childID
}

// readBranchFirstChild reads the first child pageID from a fixed location at the end of the Page
func (p *Page) readBranchFirstChild() pageID {
	offset := PageSize - 8
	return *(*pageID)(unsafe.Pointer(&p.data[offset]))
}

// MetaPage represents database metadata stored in pages 0 and 1
// Layout: [Magic: 4][Version: 2][PageSize: 2][RootPageID: 8][FreelistID: 8][FreelistPages: 8][TxnID: 8][CheckpointTxnID: 8][NumPages: 8][Checksum: 4]
// Total: 60 bytes
type MetaPage struct {
	Magic           uint32 // 4 bytes: 0x66726462 ("frdb")
	Version         uint16 // 2 bytes: format version (1)
	PageSize        uint16 // 2 bytes: Page size (4096)
	RootPageID      pageID // 8 bytes: root of B-tree
	FreelistID      pageID // 8 bytes: start of freelist
	FreelistPages   uint64 // 8 bytes: number of contiguous freelist pages
	TxnID           uint64 // 8 bytes: transaction counter
	CheckpointTxnID uint64 // 8 bytes: last checkpointed transaction
	NumPages        uint64 // 8 bytes: total pages allocated
	Checksum        uint32 // 4 bytes: CRC32 of above fields
}

// writeMeta writes metadata to the Page starting at pageHeaderSize
func (p *Page) writeMeta(m *MetaPage) {
	offset := pageHeaderSize
	ptr := unsafe.Pointer(&p.data[offset])
	*(*MetaPage)(ptr) = *m
}

// readMeta reads metadata from the Page starting at pageHeaderSize
func (p *Page) readMeta() *MetaPage {
	offset := pageHeaderSize
	ptr := unsafe.Pointer(&p.data[offset])
	return (*MetaPage)(ptr)
}

// calculateChecksum computes CRC32 checksum of all fields except Checksum itself
func (m *MetaPage) calculateChecksum() uint32 {
	// Create byte slice of all fields except Checksum
	// MetaPage is 60 bytes, Checksum is last 4 bytes, so we hash first 56 bytes
	data := make([]byte, 56)
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

	// CheckpointTxnID (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.CheckpointTxnID >> (i * 8))
	}
	offset += 8

	// NumPages (8 bytes)
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(m.NumPages >> (i * 8))
	}

	return crc32.ChecksumIEEE(data)
}

// validate checks if the metadata is valid
func (m *MetaPage) validate() error {
	if m.Magic != MagicNumber {
		return ErrInvalidMagicNumber
	}
	if m.Version != FormatVersion {
		return ErrInvalidVersion
	}
	if m.PageSize != PageSize {
		return ErrInvalidPageSize
	}
	expectedChecksum := m.calculateChecksum()
	if m.Checksum != expectedChecksum {
		return ErrInvalidChecksum
	}
	return nil
}

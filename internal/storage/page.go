package storage

import (
	"errors"
	"hash/crc32"
	"unsafe"
)

var (
	ErrPageOverflow       = errors.New("pages overflow: serialized data exceeds pages size")
	ErrInvalidOffset      = errors.New("invalid offset: out of bounds")
	ErrInvalidMagicNumber = errors.New("invalid magic number")
	ErrInvalidVersion     = errors.New("invalid format version")
	ErrInvalidPageSize    = errors.New("invalid Page size")
	ErrInvalidChecksum    = errors.New("invalid checksum")
)

const (
	PageSize = 4096

	LeafPageFlag   uint16 = 0x01
	BranchPageFlag uint16 = 0x02

	PageHeaderSize    = 40 // PageID(8) + Flags(2) + NumKeys(2) + Padding(4) + TxnID(8) + _NextLeaf(8) + _PrevLeaf(8)
	LeafElementSize   = 12
	BranchElementSize = 16

	// MagicNumber for file format identification ("frdb" in hex)
	MagicNumber uint32 = 0x66726462

	FormatVersion uint16 = 1
)

type PageID uint64

// Page is raw disk Page (4096 bytes)
//
// LEAF PAGE LAYOUT:
// ┌─────────────────────────────────────────────────────────────────────┐
// │ Header (40 bytes)                                                   │
// │ PageID, Flags, NumKeys, Padding, TxnID, _NextLeaf, _PrevLeaf        │
// ├─────────────────────────────────────────────────────────────────────┤
// │ LeafElement[0] (12 bytes)                                           │
// │ KeyOffset, KeySize, ValueOffset, ValueSize, Reserved                │
// ├─────────────────────────────────────────────────────────────────────┤
// │ LeafElement[1] (12 bytes)                                           │
// ├─────────────────────────────────────────────────────────────────────┤
// │ ...                                                                 │
// ├─────────────────────────────────────────────────────────────────────┤
// │ LeafElement[N-1] (12 bytes)                                         │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Data Area (variable, packed from end backward):                     │
// │   ← key[0] | value[0] | key[1] | value[1] | ... | key[N-1] | val[N] │
// │   Elements grow forward →              Data grows backward ←        │
// └─────────────────────────────────────────────────────────────────────┘
//
// BRANCH PAGE LAYOUT:
// ┌─────────────────────────────────────────────────────────────────────┐
// │ Header (40 bytes)                                                   │
// │ PageID, Flags, NumKeys, Padding, TxnID, _NextLeaf, _PrevLeaf        │
// ├─────────────────────────────────────────────────────────────────────┤
// │ BranchElement[0] (16 bytes)                                         │
// │ KeyOffset, KeySize, Reserved, ChildID                               │
// ├─────────────────────────────────────────────────────────────────────┤
// │ BranchElement[1] (16 bytes)                                         │
// ├─────────────────────────────────────────────────────────────────────┤
// │ ...                                                                 │
// ├─────────────────────────────────────────────────────────────────────┤
// │ BranchElement[N-1] (16 bytes)                                       │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Data Area (variable, packed from end backward, reserve last 8):     │
// │   ← key[0] | key[1] | ... | key[N-1]        Children[0] (8 bytes)→  │
// │   Elements grow forward →   Data grows backward ← (reserve end 8)   │
// │   BranchElement[0..N-1].ChildID stores Children[1..N]               │
// │   Children[0] is at FIXED location: last 8 bytes (PageSize-8)       │
// └─────────────────────────────────────────────────────────────────────┘
type Page struct {
	Data [PageSize]byte
}

// PageHeader represents the fixed-size Header at the start of each Page
// Layout: [PageID: 8][Flags: 2][NumKeys: 2][Padding: 4][TxnID: 8][_NextLeaf: 8][_PrevLeaf: 8]
type PageHeader struct {
	PageID    PageID // 8 bytes
	Flags     uint16 // 2 bytes (leaf/branch)
	NumKeys   uint16 // 2 bytes
	Padding   uint32 // 4 bytes (alignment)
	TxnID     uint64 // 8 bytes - transaction that committed this Page version
	_NextLeaf PageID // 8 bytes - next leaf in linked list (0 if none) (Reserved)
	_PrevLeaf PageID // 8 bytes - prev leaf in linked list (0 if none) (Reserved)
}

// LeafElement represents metadata for a key-value pair in a leaf Page
// Layout: [KeyOffset: 2][KeySize: 2][ValueOffset: 2][ValueSize: 2][Reserved: 4]
type LeafElement struct {
	KeyOffset   uint16 // 2 bytes: offset from data area start
	KeySize     uint16 // 2 bytes
	ValueOffset uint16 // 2 bytes: offset from data area start
	ValueSize   uint16 // 2 bytes
	Reserved    uint32 // 4 bytes
}

// BranchElement represents metadata for a routing key and child pointer in a branch Page
// B+ tree: branch nodes only store Keys for routing, no Values
// Layout: [KeyOffset: 2][KeySize: 2][Reserved: 4][ChildID: 8]
type BranchElement struct {
	KeyOffset uint16 // 2 bytes: offset from data area start
	KeySize   uint16 // 2 bytes
	Reserved  uint32 // 4 bytes: unused (for future use or alignment)
	ChildID   PageID // 8 bytes
}

// Header returns the Page Header decoded from Page Data
func (p *Page) Header() *PageHeader {
	return (*PageHeader)(unsafe.Pointer(&p.Data[0]))
}

// LeafElements returns the array of leaf elements starting after the Header
func (p *Page) LeafElements() []LeafElement {
	h := p.Header()
	if h.NumKeys == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&p.Data[PageHeaderSize])
	return unsafe.Slice((*LeafElement)(ptr), h.NumKeys)
}

// BranchElements returns the array of branch elements starting after the Header
func (p *Page) BranchElements() []BranchElement {
	h := p.Header()
	if h.NumKeys == 0 {
		return nil
	}
	ptr := unsafe.Pointer(&p.Data[PageHeaderSize])
	return unsafe.Slice((*BranchElement)(ptr), h.NumKeys)
}

// WriteHeader writes the Page Header to the Page data
func (p *Page) WriteHeader(h *PageHeader) {
	*p.Header() = *h
}

// WriteLeafElement writes a leaf element at the specified index
func (p *Page) WriteLeafElement(idx int, e *LeafElement) {
	ptr := unsafe.Pointer(&p.Data[PageHeaderSize+idx*LeafElementSize])
	*(*LeafElement)(ptr) = *e
}

// WriteBranchElement writes a branch element at the specified index
func (p *Page) WriteBranchElement(idx int, e *BranchElement) {
	ptr := unsafe.Pointer(&p.Data[PageHeaderSize+idx*BranchElementSize])
	*(*BranchElement)(ptr) = *e
}

// GetKey retrieves a key from the data area given an absolute offset and size
func (p *Page) GetKey(offset, size uint16) ([]byte, error) {
	start := int(offset)
	end := start + int(size)

	if start < 0 || end > PageSize {
		return nil, ErrPageOverflow
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.Data[start:end], nil
}

// GetValue retrieves a value from the data area given an absolute offset and size
func (p *Page) GetValue(offset, size uint16) ([]byte, error) {
	start := int(offset)
	end := start + int(size)

	if start < 0 || end > PageSize {
		return nil, ErrPageOverflow
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.Data[start:end], nil
}

// WriteBranchFirstChild writes the first child PageID to a fixed location at the end of the Page
func (p *Page) WriteBranchFirstChild(childID PageID) {
	offset := PageSize - 8
	*(*PageID)(unsafe.Pointer(&p.Data[offset])) = childID
}

// ReadBranchFirstChild reads the first child PageID from a fixed location at the end of the Page
func (p *Page) ReadBranchFirstChild() PageID {
	offset := PageSize - 8
	return *(*PageID)(unsafe.Pointer(&p.Data[offset]))
}

// dataAreaStart returns the offset where variable-length data begins
func (p *Page) dataAreaStart() int {
	h := p.Header()
	if h.Flags&LeafPageFlag != 0 {
		return PageHeaderSize + int(h.NumKeys)*LeafElementSize
	}
	return PageHeaderSize + int(h.NumKeys)*BranchElementSize
}

// MetaPage represents database metadata stored in pages 0 and 1
// Layout: [Magic: 4][Version: 2][PageSize: 2][RootPageID: 8][FreelistID: 8][FreelistPages: 8][TxnID: 8][CheckpointTxnID: 8][NumPages: 8][Checksum: 4]
// Total: 60 bytes
type MetaPage struct {
	Magic           uint32 // 4 bytes: 0x66726462 ("frdb")
	Version         uint16 // 2 bytes: format version (1)
	PageSize        uint16 // 2 bytes: Page size (4096)
	RootPageID      PageID // 8 bytes: root of B-tree
	FreelistID      PageID // 8 bytes: start of freelist
	FreelistPages   uint64 // 8 bytes: number of contiguous freelist pages
	TxnID           uint64 // 8 bytes: transaction counter
	CheckpointTxnID uint64 // 8 bytes: last checkpointed transaction
	NumPages        uint64 // 8 bytes: total pages allocated
	Checksum        uint32 // 4 bytes: CRC32 of above fields
}

// WriteMeta writes metadata to the Page starting at PageHeaderSize
func (p *Page) WriteMeta(m *MetaPage) {
	offset := PageHeaderSize
	ptr := unsafe.Pointer(&p.Data[offset])
	*(*MetaPage)(ptr) = *m
}

// ReadMeta reads metadata from the Page starting at PageHeaderSize
func (p *Page) ReadMeta() *MetaPage {
	offset := PageHeaderSize
	ptr := unsafe.Pointer(&p.Data[offset])
	return (*MetaPage)(ptr)
}

// CalculateChecksum computes CRC32 checksum of all fields except Checksum itself
func (m *MetaPage) CalculateChecksum() uint32 {
	// MetaPage is 60 bytes, Checksum is last 4 bytes, so we hash first 56 bytes
	data := unsafe.Slice((*byte)(unsafe.Pointer(m)), 56)
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

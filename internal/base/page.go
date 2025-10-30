package base

import (
	"encoding/binary"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const (
	PageSize = 4096

	LeafPageFlag     uint32 = 0x01
	BranchPageFlag   uint32 = 0x02
	OverflowPageFlag uint32 = 0x04

	PageHeaderSize    = 24 // PageID(8) + Flags(4) + NumKeys(4) + TxID(8)
	LeafElementSize   = 8
	BranchElementSize = 16

	// MagicNumber for file format identification ("frdb" in hex)
	MagicNumber uint32 = 0x66726462

	FormatVersion uint16 = 1

	OverflowThreshold                   = 3072                      // 3KB
	MaxValueSize                        = 268435456                 // 256MB cap
	OverflowFirstPageDataSize           = PageSize - PageHeaderSize // 4072 bytes
	OverflowContinuationPageSize        = PageSize                  // 4096 bytes (no header)
	LeafOverflowFlag             uint16 = 0x01
)

type PageID uint64

// Page is raw disk Page (4096 bytes)
//
// LEAF PAGE LAYOUT (CoW: consecutive KV pairs, forward growth):
// ┌─────────────────────────────────────────────────────────────────────┐
// │ Header (24 bytes)                                                   │
// │ PageID, Flags, NumKeys, TxID                                        │
// ├─────────────────────────────────────────────────────────────────────┤
// │ LeafElement[0] (8 bytes)                                            │
// │ KVOffset, KeySize, ValueSize, Reserved                              │
// ├─────────────────────────────────────────────────────────────────────┤
// │ LeafElement[1] (8 bytes)                                            │
// ├─────────────────────────────────────────────────────────────────────┤
// │ ...                                                                 │
// ├─────────────────────────────────────────────────────────────────────┤
// │ LeafElement[N-1] (8 bytes)                                          │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Data Area (consecutive KV pairs, forward growth):                   │
// │   Key[0] | Value[0] | Key[1] | Value[1] | ... | Key[N-1] | Val[N] → │
// │   Everything grows forward →                                        │
// └─────────────────────────────────────────────────────────────────────┘
//
// BRANCH PAGE LAYOUT (CoW: forward growth):
// ┌─────────────────────────────────────────────────────────────────────┐
// │ Header (24 bytes)                                                   │
// │ PageID, Flags, NumKeys, TxID                                        │
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
// │ FirstChild (8 bytes) - Children[0]                                  │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Data Area (consecutive keys, forward growth):                       │
// │   Key[0] | Key[1] | ... | Key[N-1] →                                │
// │   BranchElement[0..N-1].ChildID stores Children[1..N]               │
// └─────────────────────────────────────────────────────────────────────┘
//
// OVERFLOW PAGE LAYOUT (Contiguous):
// First Page:
// ┌─────────────────────────────────────────────────────────────────────┐
// │ Header (24 bytes)                                                   │
// │ PageID, Flags (Overflow), NumKeys (bytes in this page), TxID        │
// ├─────────────────────────────────────────────────────────────────────┤
// │ Raw Data Payload (4072 bytes)                                       │
// └─────────────────────────────────────────────────────────────────────┘
//
// Continuation Pages (PageID+1, PageID+2, ...):
// ┌─────────────────────────────────────────────────────────────────────┐
// │ Raw Data Payload (4096 bytes, no header)                            │
// └─────────────────────────────────────────────────────────────────────┘
type Page struct {
	Data [PageSize]byte
}

// PageHeader represents the fixed-Size Header at the start of each Page
// Layout: [PageID: 8][Flags: 4][NumKeys: 4][TxID: 8]
type PageHeader struct {
	PageID  PageID // 8 bytes
	Flags   uint32 // 4 bytes: leaf/branch/overflow
	NumKeys uint32 // 4 bytes: number of key-value pairs or keys
	TxnID   uint64 // 8 bytes - transaction that committed this Page version
}

// LeafElement represents metadata for a key-value pair in a leaf Page
// CoW model: key and value stored consecutively, value follows key immediately
// Layout: [KVOffset: 2][KeySize: 2][ValueSize: 2][Reserved: 2]
type LeafElement struct {
	KVOffset  uint16 // 2 bytes: offset to key start, value at KVOffset+KeySize
	KeySize   uint16 // 2 bytes
	ValueSize uint16 // 2 bytes
	Reserved  uint16 // 2 bytes: unused (for future flags/metadata)
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

// GetKey retrieves a key from the data area given an absolute offset and Size
func (p *Page) GetKey(offset, size uint16) ([]byte, error) {
	start := int(offset)
	end := start + int(size)

	if start < 0 || end > PageSize {
		return nil, ErrInvalidOffset
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.Data[start:end], nil
}

// GetValue retrieves a value from the data area given an absolute offset and Size
func (p *Page) GetValue(offset, size uint16) ([]byte, error) {
	start := int(offset)
	end := start + int(size)

	if start < 0 || end > PageSize {
		return nil, ErrInvalidOffset
	}
	if start > end {
		return nil, ErrInvalidOffset
	}

	return p.Data[start:end], nil
}

// WriteBranchFirstChild writes the first child PageID right after branch elements
func (p *Page) WriteBranchFirstChild(childID PageID) {
	h := p.Header()
	offset := PageHeaderSize + int(h.NumKeys)*BranchElementSize
	*(*PageID)(unsafe.Pointer(&p.Data[offset])) = childID
}

// ReadBranchFirstChild reads the first child PageID from right after branch elements
func (p *Page) ReadBranchFirstChild() PageID {
	h := p.Header()
	offset := PageHeaderSize + int(h.NumKeys)*BranchElementSize
	return *(*PageID)(unsafe.Pointer(&p.Data[offset]))
}

// dataAreaStart returns the offset where variable-length data begins
func (p *Page) dataAreaStart() int {
	h := p.Header()
	if h.Flags&LeafPageFlag != 0 {
		return PageHeaderSize + int(h.NumKeys)*LeafElementSize
	}
	// Branch: Header + Elements + FirstChild (8 bytes)
	return PageHeaderSize + int(h.NumKeys)*BranchElementSize + 8
}

// MetaPage represents database metadata stored in pages 0 and 1
// Layout: [Magic: 4][Version: 2][PageSize: 2][RootPageID: 8][FreelistID: 8][FreelistPages: 8][TxID: 8][CheckpointTxnID: 8][NumPages: 8][Checksum: 8]
// Total: 64 bytes
type MetaPage struct {
	Magic           uint32 // 4 bytes: 0x66726462 ("frdb")
	Version         uint16 // 2 bytes: format version (1)
	PageSize        uint16 // 2 bytes: Page Size (4096)
	RootPageID      PageID // 8 bytes: root of B-tree
	FreelistID      PageID // 8 bytes: start of freelist
	FreelistPages   uint64 // 8 bytes: number of contiguous freelist pages
	TxID            uint64 // 8 bytes: transaction counter
	CheckpointTxnID uint64 // 8 bytes: last checkpointed transaction
	NumPages        uint64 // 8 bytes: total pages allocated
	Checksum        uint64 // 8 bytes: CRC32 of above fields
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
func (m *MetaPage) CalculateChecksum() uint64 {
	// MetaPage is 60 bytes, Checksum is last 4 bytes, so we hash first 56 bytes
	data := unsafe.Slice((*byte)(unsafe.Pointer(m)), 56)
	return xxhash.Sum64(data)
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

// WriteNextPageID writes the next overflow page ID to the last 8 bytes
func (p *Page) WriteNextPageID(next PageID) {
	offset := PageSize - 8
	binary.LittleEndian.PutUint64(p.Data[offset:], uint64(next))
}

// ReadNextPageID reads the next overflow page ID from the last 8 bytes
func (p *Page) ReadNextPageID() PageID {
	offset := PageSize - 8
	return PageID(binary.LittleEndian.Uint64(p.Data[offset:]))
}

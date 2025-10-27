package base

import (
	"bytes"
	"encoding/binary"
	"maps"
)

const (
	// MinFillRatio is the minimum page utilization (25%)
	MinFillRatio = 0.25
)

type NodeType int

const (
	BranchType NodeType = iota
	LeafType
)

// OverflowMeta stores metadata about an overflow value chain
type OverflowMeta struct {
	FirstPageID PageID
	PageCount   uint16
}

// Node represents a B-tree Node with decoded Page data
type Node struct {
	PageID PageID
	Dirty  bool

	// Decoded Node data
	NumKeys  uint32
	Keys     [][]byte // Allocated copies
	Values   [][]byte // If nil, this is a branch Node
	Children []PageID

	// Overflow metadata (maps index to overflow meta)
	OverflowInfo map[int]OverflowMeta
}

// PageAllocator is a function type for allocating new pages
// count: number of contiguous pages to allocate
// Returns the first PageID of the allocated range
type PageAllocator func(count int) PageID

// Serialize allocates and writes the node to pages, including overflow chains
// Returns main page, overflow pages (one slice per overflow value), and error
func (n *Node) Serialize(txID uint64, allocPage PageAllocator) (*Page, [][]*Page, error) {
	// Allocate main page
	if n.PageID == 0 {
		n.PageID = allocPage(1)
	}

	mainPage := &Page{}

	// Write header
	header := &PageHeader{
		PageID:  n.PageID,
		NumKeys: n.NumKeys,
		TxnID:   txID,
	}
	if n.Type() == LeafType {
		header.Flags = LeafPageFlag
	} else {
		header.Flags = BranchPageFlag
	}
	mainPage.WriteHeader(header)

	var overflowPages [][]*Page

	if n.Type() == LeafType {
		// serialize leaf Node - pack forward from dataAreaStart
		dataOffset := uint16(mainPage.dataAreaStart())

		for i := 0; i < int(n.NumKeys); i++ {
			key := n.Keys[i]
			value := n.Values[i]
			kvOffset := dataOffset

			// Copy key
			copy(mainPage.Data[dataOffset:], key)
			dataOffset += uint16(len(key))

			var elem *LeafElement

			// Check if value should go to overflow
			if len(value) > OverflowThreshold {
				// Allocate and write overflow chain
				pages, firstPageID := n.writeOverflowChain(value, txID, allocPage)
				overflowPages = append(overflowPages, pages)

				// Store overflow metadata
				if n.OverflowInfo == nil {
					n.OverflowInfo = make(map[int]OverflowMeta)
				}
				n.OverflowInfo[i] = OverflowMeta{
					FirstPageID: firstPageID,
					PageCount:   uint16(len(pages)),
				}

				// Write PageID pointer in data area (8 bytes)
				binary.LittleEndian.PutUint64(mainPage.Data[dataOffset:], uint64(firstPageID))
				dataOffset += 8

				elem = &LeafElement{
					KVOffset:  kvOffset,
					KeySize:   uint16(len(key)),
					ValueSize: uint16(len(pages)), // page count
					Reserved:  LeafOverflowFlag,
				}
			} else {
				// Inline value
				copy(mainPage.Data[dataOffset:], value)
				dataOffset += uint16(len(value))

				elem = &LeafElement{
					KVOffset:  kvOffset,
					KeySize:   uint16(len(key)),
					ValueSize: uint16(len(value)),
					Reserved:  0,
				}
			}

			mainPage.WriteLeafElement(i, elem)
		}
	} else {
		// serialize branch Node (B+ tree: only Keys, no Values)
		// Write Children[0] right after elements
		if len(n.Children) > 0 {
			mainPage.WriteBranchFirstChild(n.Children[0])
		}

		// Pack Keys forward from dataAreaStart
		dataOffset := uint16(mainPage.dataAreaStart())
		for i := 0; i < int(n.NumKeys); i++ {
			key := n.Keys[i]

			// Write key
			keyOffset := dataOffset
			copy(mainPage.Data[dataOffset:], key)
			dataOffset += uint16(len(key))

			elem := &BranchElement{
				KeyOffset: keyOffset,
				KeySize:   uint16(len(key)),
				Reserved:  0,
				ChildID:   n.Children[i+1],
			}
			mainPage.WriteBranchElement(i, elem)
		}
	}

	return mainPage, overflowPages, nil
}

// writeOverflowChain allocates contiguous pages and writes value across them
// First page has header, continuation pages are raw data
// Returns the allocated pages and the first page ID
func (n *Node) writeOverflowChain(value []byte, txID uint64, allocPage PageAllocator) ([]*Page, PageID) {
	// Calculate page count: first page + continuation pages
	// First page: OverflowFirstPageDataSize (4072 bytes)
	// Continuation: OverflowContinuationPageSize each (4096 bytes)
	remaining := len(value)
	pageCount := 1 // At least one page for header
	if remaining > OverflowFirstPageDataSize {
		remaining -= OverflowFirstPageDataSize
		pageCount += (remaining + OverflowContinuationPageSize - 1) / OverflowContinuationPageSize
	}

	if pageCount > 65535 {
		panic("value too large for overflow chain")
	}

	// Allocate contiguous pages
	firstPageID := allocPage(pageCount)

	pages := make([]*Page, pageCount)
	for i := 0; i < pageCount; i++ {
		pages[i] = &Page{}
	}

	// Write first page with header
	firstPage := pages[0]
	chunkSize := len(value)
	if chunkSize > OverflowFirstPageDataSize {
		chunkSize = OverflowFirstPageDataSize
	}

	header := &PageHeader{
		PageID:  firstPageID,
		Flags:   OverflowPageFlag,
		NumKeys: uint32(len(value)), // Total value size in bytes
		TxnID:   txID,
	}
	firstPage.WriteHeader(header)
	copy(firstPage.Data[PageHeaderSize:], value[0:chunkSize])

	// Write continuation pages (raw data, no headers)
	offset := chunkSize
	for i := 1; i < pageCount; i++ {
		page := pages[i]
		remaining := len(value) - offset
		chunkSize := remaining
		if chunkSize > OverflowContinuationPageSize {
			chunkSize = OverflowContinuationPageSize
		}

		// Raw data only, no header
		copy(page.Data[0:], value[offset:offset+chunkSize])
		offset += chunkSize
	}

	return pages, firstPageID
}

// Deserialize decodes the Page data into Node fields
func (n *Node) Deserialize(p *Page) error {
	header := p.Header()
	n.PageID = header.PageID
	n.NumKeys = header.NumKeys
	n.Dirty = false

	if (header.Flags & LeafPageFlag) != 0 {
		// deserialize leaf Node
		n.Keys = make([][]byte, n.NumKeys)
		n.Values = make([][]byte, n.NumKeys)
		n.Children = nil
		n.OverflowInfo = nil

		elements := p.LeafElements()
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Read key
			keyStart := elem.KVOffset
			keyEnd := keyStart + elem.KeySize
			n.Keys[i] = make([]byte, elem.KeySize)
			copy(n.Keys[i], p.Data[keyStart:keyEnd])

			// Check if value is in overflow
			if elem.Reserved&LeafOverflowFlag != 0 {
				// Overflow value - read PageID pointer
				firstPageID := PageID(binary.LittleEndian.Uint64(p.Data[keyEnd : keyEnd+8]))

				// Store overflow metadata
				if n.OverflowInfo == nil {
					n.OverflowInfo = make(map[int]OverflowMeta)
				}
				n.OverflowInfo[i] = OverflowMeta{
					FirstPageID: firstPageID,
					PageCount:   elem.ValueSize, // ValueSize holds page count for overflow
				}

				// Leave value nil - will be loaded later via LoadOverflowValues
				n.Values[i] = nil
			} else {
				// Inline value - read directly
				valueStart := keyEnd
				valueEnd := valueStart + elem.ValueSize
				n.Values[i] = make([]byte, elem.ValueSize)
				copy(n.Values[i], p.Data[valueStart:valueEnd])
			}
		}
	} else {
		// deserialize branch Node (B+ tree: only Keys, no Values)
		n.Keys = make([][]byte, n.NumKeys)
		n.Values = nil // Branch nodes don't have Values
		n.Children = make([]PageID, n.NumKeys+1)

		// Read Children[0]
		n.Children[0] = p.ReadBranchFirstChild()

		elements := p.BranchElements()
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy keys to independent allocations
			n.Keys[i] = make([]byte, elem.KeySize)
			copy(n.Keys[i], p.Data[elem.KeyOffset:elem.KeyOffset+elem.KeySize])

			// Copy child pointer
			n.Children[i+1] = elem.ChildID
		}
	}

	return nil
}

// BatchPageReader is a function type for reading multiple contiguous pages
type BatchPageReader func(PageID, int) ([]byte, error)

// LoadOverflowValue loads a single overflow value at the given index from contiguous pages
// First page has header, continuation pages are raw data at contiguous page IDs
func (n *Node) LoadOverflowValue(index int, readAt BatchPageReader) error {
	if n.OverflowInfo == nil {
		return nil // No overflow info
	}

	meta, ok := n.OverflowInfo[index]
	if !ok || meta.PageCount == 0 {
		return nil // Not an overflow value
	}

	// Already loaded
	if n.Values[index] != nil {
		return nil
	}

	// Read all contiguous pages at once
	buf, err := readAt(meta.FirstPageID, int(meta.PageCount))
	if err != nil {
		return err
	}

	// Parse header from first page
	firstPage := &Page{}
	copy(firstPage.Data[:], buf[0:PageSize])
	header := firstPage.Header()
	totalSize := int(header.NumKeys) // Total value size in bytes

	value := make([]byte, 0, totalSize)

	// Extract data from first page
	firstChunkSize := totalSize
	if firstChunkSize > OverflowFirstPageDataSize {
		firstChunkSize = OverflowFirstPageDataSize
	}
	value = append(value, buf[PageHeaderSize:PageHeaderSize+firstChunkSize]...)

	// Extract data from continuation pages
	offset := PageSize
	bytesRead := firstChunkSize
	for i := uint16(1); i < meta.PageCount; i++ {
		remaining := totalSize - bytesRead
		chunkSize := remaining
		if chunkSize > OverflowContinuationPageSize {
			chunkSize = OverflowContinuationPageSize
		}

		value = append(value, buf[offset:offset+chunkSize]...)
		bytesRead += chunkSize
		offset += PageSize
	}

	n.Values[index] = value
	return nil
}

// FindKey returns the index of key in Node, or -1 if not found
func (n *Node) FindKey(key []byte) int {
	for i := 0; i < int(n.NumKeys); i++ {
		cmp := bytes.Compare(key, n.Keys[i])
		if cmp == 0 {
			return i
		}
		if cmp < 0 {
			return -1
		}
	}
	return -1
}

// Clone creates a shallow copy of this Node for copy-on-write
// The Clone is marked Dirty and does not have a PageID allocated yet
func (n *Node) Clone() *Node {
	cloned := &Node{
		PageID:  0,
		Dirty:   true,
		NumKeys: n.NumKeys,
	}

	// Shallow copy Keys - share backing arrays
	cloned.Keys = make([][]byte, len(n.Keys))
	copy(cloned.Keys, n.Keys)

	// Shallow copy Values (leaf nodes only)
	if n.Type() == LeafType && len(n.Values) > 0 {
		cloned.Values = make([][]byte, len(n.Values))
		copy(cloned.Values, n.Values)
	}

	// Shallow copy Children (branch nodes only) - PageIDs are copy-by-value
	if n.Type() == BranchType {
		cloned.Children = make([]PageID, len(n.Children))
		copy(cloned.Children, n.Children)
	}

	// Shallow copy OverflowInfo
	if n.OverflowInfo != nil {
		cloned.OverflowInfo = make(map[int]OverflowMeta, len(n.OverflowInfo))
		maps.Copy(cloned.OverflowInfo, n.OverflowInfo)
	}

	return cloned
}

// IsUnderflow checks if Node has insufficient fill ratio (doesn't apply to root)
func (n *Node) IsUnderflow() bool {
	minSize := int(float64(PageSize) * MinFillRatio)
	return n.Size() < minSize
}

func (n *Node) CheckOverflow() error {
	size := n.Size()
	if size > PageSize {
		return ErrPageOverflow
	}
	return nil
}

// IsFull checks if a Node is full
func (n *Node) IsFull(key, value []byte) bool {
	if n.Type() == LeafType {
		currentSize := n.Size()
		valueSize := len(value)
		// If value will overflow, only 8 bytes (PageID pointer) stored inline
		if valueSize > OverflowThreshold {
			valueSize = 8
		}
		projectedSize := currentSize + LeafElementSize + len(key) + valueSize
		isFull := projectedSize > PageSize
		return isFull
	}
	// Branch nodes only have keys, no values
	return n.Size()+BranchElementSize+len(key) > PageSize
}

// Size calculates the Size of the serialized Node
func (n *Node) Size() int {
	size := PageHeaderSize

	if n.Type() == LeafType {
		size += int(n.NumKeys) * LeafElementSize
		for i := 0; i < int(n.NumKeys); i++ {
			size += len(n.Keys[i])

			// Check if this value is or will be overflow
			isOverflow := false
			if n.OverflowInfo != nil {
				if meta, ok := n.OverflowInfo[i]; ok && meta.PageCount > 0 {
					isOverflow = true
				}
			}
			if !isOverflow && n.Values[i] != nil && len(n.Values[i]) > OverflowThreshold {
				isOverflow = true
			}

			if isOverflow {
				size += 8 // Only PageID pointer stored inline
			} else if n.Values[i] != nil {
				size += len(n.Values[i])
			}
		}
	} else {
		// B+ tree: branch nodes only store Keys (no Values)
		size += int(n.NumKeys) * BranchElementSize
		size += 8 // Children[0]
		for i := 0; i < int(n.NumKeys); i++ {
			size += len(n.Keys[i]) // Only Keys, no Values
		}
	}

	return size
}

// Type returns the node type (LeafType or BranchType)
func (n *Node) Type() NodeType {
	if n.Values != nil {
		return LeafType
	}
	return BranchType
}

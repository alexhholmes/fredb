package base

import (
	"bytes"
)

const (
	// MinKeysPerNode is the minimum Keys for non-root nodes
	MinKeysPerNode = 16

	// OverflowDataSize is the amount of data that fits in one overflow page
	// PageSize - PageHeaderSize - 8 bytes for next pointer
	OverflowDataSize = PageSize - PageHeaderSize - 8

	// MaxValueInLeaf is the threshold for storing values inline vs overflow
	// Values larger than half the usable data area go to overflow pages
	// Usable area = PageSize - PageHeaderSize = 4096 - 24 = 4072
	MaxValueInLeaf = (PageSize - PageHeaderSize) / 2
)

// Node represents a B-tree Node with decoded Page data
type Node struct {
	PageID PageID
	Dirty  bool

	// Decoded Node data
	NumKeys  uint16
	Keys     [][]byte // Allocated copies
	Values   [][]byte // If nil, this is a branch Node
	Children []PageID

	// Overflow chain tracking for values that use overflow pages
	// Map key is the index into Values slice, value is the chain of overflow PageIDs
	// Only populated for values with overflow (sparse map)
	OverflowChains map[int][]PageID
}

// Serialize encodes the Node data into a fresh Page
// allocPage is an optional callback for allocating overflow pages (can be nil if no overflow expected)
func (n *Node) Serialize(txID uint64, page *Page, allocPage func() *Page) error {
	if err := n.CheckOverflow(); err != nil {
		return err
	}

	// Write header
	header := &PageHeader{
		PageID:  n.PageID,
		NumKeys: n.NumKeys,
		TxnID:   txID,
	}
	if n.IsLeaf() {
		header.Flags = LeafPageFlag
	} else {
		header.Flags = BranchPageFlag
	}
	page.WriteHeader(header)

	if n.IsLeaf() {
		// Serialize leaf Node - pack from end backward
		dataOffset := uint16(PageSize)
		// Process in reverse order to pack from end
		for i := int(n.NumKeys) - 1; i >= 0; i-- {
			key := n.Keys[i]
			value := n.Values[i]

			var elem *LeafElement

			// Check if value needs overflow pages
			if len(value) > MaxValueInLeaf && allocPage != nil {
				// Build overflow chain for large value
				firstPageID, numPages, err := n.buildOverflowChain(txID, value, allocPage)
				if err != nil {
					return err
				}

				// Write key only (value is in overflow pages)
				dataOffset -= uint16(len(key))
				copy(page.Data[dataOffset:], key)
				keyOffset := dataOffset

				elem = &LeafElement{
					KeyOffset:   keyOffset,
					KeySize:     uint16(len(key)),
					ValueOffset: numPages,    // Repurposed: number of overflow pages
					ValueSize:   0,           // Unused for overflow (size derived from OverflowSize in each page)
					Overflow:    firstPageID, // First overflow page
				}
			} else {
				// Write value inline (at end)
				dataOffset -= uint16(len(value))
				copy(page.Data[dataOffset:], value)
				valueOffset := dataOffset

				// Write key before value
				dataOffset -= uint16(len(key))
				copy(page.Data[dataOffset:], key)
				keyOffset := dataOffset

				elem = &LeafElement{
					KeyOffset:   keyOffset,
					KeySize:     uint16(len(key)),
					ValueOffset: valueOffset,
					ValueSize:   uint16(len(value)),
					Overflow:    0, // No overflow
				}
			}

			page.WriteLeafElement(i, elem)
		}
	} else {
		// Serialize branch Node (B+ tree: only Keys, no Values)
		// Write Children[0] at fixed location (last 8 bytes)
		if len(n.Children) > 0 {
			page.WriteBranchFirstChild(n.Children[0])
		}

		// Pack Keys from end backward (reserve last 8 bytes for Children[0])
		dataOffset := uint16(PageSize - 8)
		// Process in reverse order to pack from end
		for i := int(n.NumKeys) - 1; i >= 0; i-- {
			key := n.Keys[i]

			// Write key
			dataOffset -= uint16(len(key))
			copy(page.Data[dataOffset:], key)

			elem := &BranchElement{
				KeyOffset: dataOffset,
				KeySize:   uint16(len(key)),
				Reserved:  0, // No Values in B+ tree branches
				ChildID:   n.Children[i+1],
			}
			page.WriteBranchElement(i, elem)
		}
	}

	return nil
}

// Deserialize decodes the Page data into Node fields
// readPage is an optional callback for reading overflow pages (can be nil if no overflow expected)
func (n *Node) Deserialize(p *Page, readPage func(PageID) (*Page, error)) error {
	header := p.Header()
	n.PageID = header.PageID
	n.NumKeys = header.NumKeys
	n.Dirty = false

	if (header.Flags & LeafPageFlag) != 0 {
		// Deserialize leaf Node
		n.Keys = make([][]byte, n.NumKeys)
		n.Values = make([][]byte, n.NumKeys)
		n.Children = nil
		n.OverflowChains = nil // Will be initialized if needed

		elements := p.LeafElements()
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy keys to independent allocations
			n.Keys[i] = make([]byte, elem.KeySize)
			copy(n.Keys[i], p.Data[elem.KeyOffset:elem.KeyOffset+elem.KeySize])

			// Check if value is in overflow pages
			if elem.Overflow != 0 {
				// Collect overflow chain metadata
				chain, err := n.collectOverflowChain(elem.Overflow, readPage)
				if err != nil {
					return err
				}

				// Store overflow chain in map
				if n.OverflowChains == nil {
					n.OverflowChains = make(map[int][]PageID)
				}
				n.OverflowChains[i] = chain

				// Read value from overflow chain
				value, err := n.readOverflowValue(&elem, readPage)
				if err != nil {
					return err
				}
				n.Values[i] = value
			} else {
				// Value stored inline in leaf page
				n.Values[i] = make([]byte, elem.ValueSize)
				copy(n.Values[i], p.Data[elem.ValueOffset:elem.ValueOffset+elem.ValueSize])
			}
		}
	} else {
		// Deserialize branch Node (B+ tree: only Keys, no Values)
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
	if n.IsLeaf() && len(n.Values) > 0 {
		cloned.Values = make([][]byte, len(n.Values))
		copy(cloned.Values, n.Values)
	}

	// Shallow copy Children (branch nodes only) - PageIDs are copy-by-value
	if !n.IsLeaf() {
		cloned.Children = make([]PageID, len(n.Children))
		copy(cloned.Children, n.Children)
	}

	// Copy overflow chain metadata - deep copy the map and slices
	// This is critical for CoW: we need to track old overflow chains to free them later
	if n.OverflowChains != nil {
		cloned.OverflowChains = make(map[int][]PageID, len(n.OverflowChains))
		for idx, chain := range n.OverflowChains {
			// Deep copy the chain slice
			chainCopy := make([]PageID, len(chain))
			copy(chainCopy, chain)
			cloned.OverflowChains[idx] = chainCopy
		}
	}

	return cloned
}

// IsUnderflow checks if Node has too few Keys (doesn't apply to root)
func (n *Node) IsUnderflow() bool {
	return int(n.NumKeys) < MinKeysPerNode
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
	if n.IsLeaf() {
		// For large values that will use overflow, only count key + metadata
		valueSize := len(value)
		if valueSize > MaxValueInLeaf {
			valueSize = 0 // Value goes to overflow, doesn't count toward page size
		}
		return n.Size()+LeafElementSize+len(key)+valueSize > PageSize
	}
	// Branch nodes only have keys, no values
	return n.Size()+BranchElementSize+len(key) > PageSize
}

// Size calculates the Size of the serialized Node
func (n *Node) Size() int {
	size := PageHeaderSize

	if n.IsLeaf() {
		size += int(n.NumKeys) * LeafElementSize
		for i := 0; i < int(n.NumKeys); i++ {
			keyLen := len(n.Keys[i])
			size += keyLen

			// For overflow values, don't count the value bytes (they're in overflow pages)
			valueSize := len(n.Values[i])
			if valueSize > MaxValueInLeaf {
				valueSize = 0 // Value will go to overflow, doesn't count
			}
			size += valueSize
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

// IsLeaf returns true if this is a leaf Node
func (n *Node) IsLeaf() bool {
	return n.Values != nil
}

// buildOverflowChain creates a chain of overflow pages for a large value
// Returns the first overflow page ID and the number of pages allocated
func (n *Node) buildOverflowChain(txID uint64, value []byte, allocPage func() *Page) (PageID, uint16, error) {
	if allocPage == nil {
		return 0, 0, ErrPageOverflow
	}

	var firstPageID PageID
	var prevPage *Page
	numPages := uint16(0)
	offset := 0

	for offset < len(value) {
		// Allocate new overflow page
		overflowPage := allocPage()
		if overflowPage == nil {
			return 0, 0, ErrPageOverflow
		}

		// Calculate chunk size for this page
		remaining := len(value) - offset
		chunkSize := remaining
		if chunkSize > OverflowDataSize {
			chunkSize = OverflowDataSize
		}

		// Write header
		header := &PageHeader{
			PageID:       overflowPage.Header().PageID,
			Flags:        OverflowPageFlag,
			NumKeys:      0,
			OverflowSize: uint16(chunkSize),
			TxnID:        txID,
		}
		overflowPage.WriteHeader(header)

		// Write data
		copy(overflowPage.Data[PageHeaderSize:], value[offset:offset+chunkSize])
		offset += chunkSize
		numPages++
		// Link previous page to this one
		if prevPage != nil {
			prevPage.WriteNextOverflow(header.PageID)
		} else {
			// This is the first page
			firstPageID = header.PageID
		}

		prevPage = overflowPage
	}

	// Last page points to 0 (end of chain)
	if prevPage != nil {
		prevPage.WriteNextOverflow(0)
	}

	return firstPageID, numPages, nil
}

// readOverflowValue reads a value from an overflow chain
// Total size is calculated from OverflowSize in each page header (not LeafElement.ValueSize)
func (n *Node) readOverflowValue(elem *LeafElement, readPage func(PageID) (*Page, error)) ([]byte, error) {
	if readPage == nil {
		return nil, ErrPageOverflow
	}

	// Collect data from all overflow pages
	var result []byte
	nextPageID := elem.Overflow

	for nextPageID != 0 {
		page, err := readPage(nextPageID)
		if err != nil {
			return nil, err
		}

		header := page.Header()
		chunkSize := int(header.OverflowSize)

		// Append chunk from this page
		result = append(result, page.Data[PageHeaderSize:PageHeaderSize+chunkSize]...)
		nextPageID = page.ReadNextOverflow()
	}

	return result, nil
}

// collectOverflowChain collects all PageIDs in an overflow chain
// Returns the chain of PageIDs starting from firstPageID
func (n *Node) collectOverflowChain(firstPageID PageID, readPage func(PageID) (*Page, error)) ([]PageID, error) {
	if readPage == nil {
		return nil, nil
	}

	chain := []PageID{}
	nextPageID := firstPageID

	for nextPageID != 0 {
		chain = append(chain, nextPageID)

		page, err := readPage(nextPageID)
		if err != nil {
			return nil, err
		}

		nextPageID = page.ReadNextOverflow()
	}

	return chain, nil
}

// FreeOverflowChain returns the overflow chain for a specific value index
// Returns nil if the value has no overflow chain
func (n *Node) FreeOverflowChain(valueIndex int) []PageID {
	if n.OverflowChains == nil {
		return nil
	}

	chain := n.OverflowChains[valueIndex]
	if chain != nil {
		// Remove from map
		delete(n.OverflowChains, valueIndex)
	}

	return chain
}

// FreeAllOverflowChains returns all overflow chains in this node
// Used when freeing an entire node (e.g., bucket delete, tree rebalance)
func (n *Node) FreeAllOverflowChains() [][]PageID {
	if n.OverflowChains == nil || len(n.OverflowChains) == 0 {
		return nil
	}

	chains := make([][]PageID, 0, len(n.OverflowChains))
	for _, chain := range n.OverflowChains {
		chains = append(chains, chain)
	}

	// Clear the map
	n.OverflowChains = nil

	return chains
}

package base

import (
	"bytes"
)

const (
	// MinKeysPerNode is the minimum Keys for non-root nodes
	MinKeysPerNode = 16
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
}

// Serialize encodes the Node data into a fresh Page
func (n *Node) Serialize(txID uint64, page *Page) error {
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
		// serialize leaf Node - pack from end backward
		dataOffset := uint16(PageSize)
		// Process in reverse order to pack from end
		for i := int(n.NumKeys) - 1; i >= 0; i-- {
			key := n.Keys[i]
			value := n.Values[i]

			// Write value inline (at end)
			dataOffset -= uint16(len(value))
			copy(page.Data[dataOffset:], value)
			valueOffset := dataOffset

			// Write key before value
			dataOffset -= uint16(len(key))
			copy(page.Data[dataOffset:], key)
			keyOffset := dataOffset

			elem := &LeafElement{
				KeyOffset:   keyOffset,
				KeySize:     uint16(len(key)),
				ValueOffset: valueOffset,
				ValueSize:   uint16(len(value)),
			}

			page.WriteLeafElement(i, elem)
		}
	} else {
		// serialize branch Node (B+ tree: only Keys, no Values)
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

		elements := p.LeafElements()
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy keys to independent allocations
			n.Keys[i] = make([]byte, elem.KeySize)
			copy(n.Keys[i], p.Data[elem.KeyOffset:elem.KeyOffset+elem.KeySize])

			// Value stored inline in leaf page
			n.Values[i] = make([]byte, elem.ValueSize)
			copy(n.Values[i], p.Data[elem.ValueOffset:elem.ValueOffset+elem.ValueSize])
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

	return cloned
}

// IsUnderflow checks if Node has too few Keys (doesn't apply to root)
func (n *Node) IsUnderflow() bool {
	return int(n.NumKeys) < MinKeysPerNode
}

func (n *Node) CheckOverflow() error {
	size := n.Size()
	if size > PageSize {
		println("DEBUG CheckOverflow: size=", size, "PageSize=", PageSize, "NumKeys=", n.NumKeys)
		return ErrPageOverflow
	}
	return nil
}

// IsFull checks if a Node is full
func (n *Node) IsFull(key, value []byte) bool {
	if n.IsLeaf() {
		currentSize := n.Size()
		projectedSize := currentSize + LeafElementSize + len(key) + len(value)
		isFull := projectedSize > PageSize
		return isFull
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
			size += len(n.Keys[i])
			size += len(n.Values[i])
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

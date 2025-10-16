package base

import (
	"bytes"
	"sync"
)

const (
	MaxKeysPerNode = 64
	// MinKeysPerNode is the minimum Keys for non-root nodes
	MinKeysPerNode = MaxKeysPerNode / 4
)

var Pool = sync.Pool{
	New: func() any {
		return &Node{
			Keys:     make([][]byte, 0, MaxKeysPerNode),
			Values:   make([][]byte, 0, MaxKeysPerNode),
			Children: make([]PageID, 0, MaxKeysPerNode+1),
		}
	},
}

// Node represents a B-tree Node with decoded Page data
type Node struct {
	PageID PageID
	Dirty  bool
	Leaf   bool // Explicit flag: true for leaf nodes, false for branch nodes

	// Decoded Node data
	NumKeys  uint16
	Keys     [][]byte
	Values   [][]byte
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
		// Serialize leaf Node - pack from end backward
		dataOffset := uint16(PageSize)
		// Process in reverse order to pack from end
		for i := int(n.NumKeys) - 1; i >= 0; i-- {
			key := n.Keys[i]
			value := n.Values[i]

			// Write value first (at end)
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
func (n *Node) Deserialize(p *Page) error {
	header := p.Header()
	n.PageID = header.PageID
	n.NumKeys = header.NumKeys

	if (header.Flags & LeafPageFlag) != 0 {
		// Deserialize leaf Node
		n.Leaf = true
		n.Keys = make([][]byte, n.NumKeys)
		n.Values = make([][]byte, n.NumKeys)
		n.Children = nil

		elements := p.LeafElements()
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := p.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.Keys[i] = make([]byte, len(keyData))
			copy(n.Keys[i], keyData)

			// Copy value
			valueData, err := p.GetValue(elem.ValueOffset, elem.ValueSize)
			if err != nil {
				return err
			}
			n.Values[i] = make([]byte, len(valueData))
			copy(n.Values[i], valueData)
		}
	} else {
		// Deserialize branch Node (B+ tree: only Keys, no Values)
		n.Leaf = false
		n.Keys = make([][]byte, n.NumKeys)
		n.Values = nil // Branch nodes don't have Values
		n.Children = make([]PageID, n.NumKeys+1)

		// Read Children[0]
		n.Children[0] = p.ReadBranchFirstChild()

		elements := p.BranchElements()
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := p.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.Keys[i] = make([]byte, len(keyData))
			copy(n.Keys[i], keyData)

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

// Clone creates a deep copy of this Node for copy-on-write
// The Clone is marked Dirty and does not have a PageID allocated yet
func (n *Node) Clone() *Node {
	cloned := Pool.Get().(*Node)
	cloned.Reset() // Ensure clean state from pool
	cloned.PageID = 0
	cloned.Dirty = true
	cloned.Leaf = n.Leaf
	cloned.NumKeys = n.NumKeys

	// Deep copy Keys
	cloned.Keys = cloned.Keys[:0] // Reset to 0 length, keep capacity
	for _, key := range n.Keys {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		cloned.Keys = append(cloned.Keys, keyCopy)
	}

	// Deep copy Values (leaf nodes only)
	if n.Leaf {
		cloned.Values = cloned.Values[:0] // Reset to 0 length, keep capacity
		for _, val := range n.Values {
			valCopy := make([]byte, len(val))
			copy(valCopy, val)
			cloned.Values = append(cloned.Values, valCopy)
		}
		cloned.Children = cloned.Children[:0] // Clear children for leaves
	} else {
		// Deep copy Children (branch nodes only)
		cloned.Values = cloned.Values[:0]     // Clear values for branches
		cloned.Children = cloned.Children[:0] // Reset to 0 length, keep capacity
		cloned.Children = append(cloned.Children, n.Children...)
	}

	return cloned
}

func (n *Node) Reset() {
	n.PageID = 0
	n.NumKeys = 0
	n.Leaf = false
	n.Keys = n.Keys[:0] // Keep capacity, zero length
	n.Values = n.Values[:0]
	n.Children = n.Children[:0]
	n.Dirty = false
}

// IsUnderflow checks if Node has too few Keys (doesn't apply to root)
func (n *Node) IsUnderflow() bool {
	return int(n.NumKeys) < MinKeysPerNode
}

func (n *Node) CheckOverflow() error {
	if n.Size() > PageSize {
		return ErrPageOverflow
	}
	return nil
}

// IsFull checks if a Node is full
func (n *Node) IsFull(key, value []byte) bool {
	if n.IsLeaf() {
		return n.Size()+LeafElementSize+len(key)+len(value) > PageSize
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
			size += len(n.Keys[i]) + len(n.Values[i])
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
	return n.Leaf
}

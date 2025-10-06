package fredb

import (
	"bytes"

	"fredb/internal"
)

const (
	// MaxKeysPerNode must be small enough that a full Node can serialize to PageSize
	MaxKeysPerNode = 64
	MinKeysPerNode = MaxKeysPerNode / 4 // Minimum keys for non-root nodes
)

// Node represents a B-tree Node with decoded Page data
type Node struct {
	pageID internal.PageID
	dirty  bool

	// Decoded Node data
	isLeaf   bool
	numKeys  uint16
	keys     [][]byte
	values   [][]byte // Empty and unused in branch nodes
	children []internal.PageID
}

// isFull checks if a Node is full
func (n *Node) isFull() bool {
	// Use key count active for both leaf and branch nodes
	// Overflow is detected during serialize with try-rollback
	return int(n.numKeys) >= MaxKeysPerNode
}

// size calculates the size of the serialized Node
func (n *Node) size() int {
	size := internal.PageHeaderSize

	if n.isLeaf {
		size += int(n.numKeys) * internal.LeafElementSize
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) + len(n.values[i])
		}
	} else {
		// B+ tree: branch nodes only store keys (no values)
		size += int(n.numKeys) * internal.BranchElementSize
		size += 8 // children[0]
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) // Only keys, no values
		}
	}

	return size
}

// serialize encodes the Node data into a fresh Page
func (n *Node) serialize(txnID uint64) (*internal.Page, error) {
	// Check size
	if n.size() > internal.PageSize {
		return nil, ErrPageOverflow
	}

	// Create fresh Page
	page := &internal.Page{}

	// Write header
	header := &internal.PageHeader{
		PageID:  n.pageID,
		NumKeys: n.numKeys,
		TxnID:   txnID,
	}
	if n.isLeaf {
		header.Flags = internal.LeafPageFlag
	} else {
		header.Flags = internal.BranchPageFlag
	}
	page.WriteHeader(header)

	if n.isLeaf {
		// serialize leaf Node - pack from end backward
		dataOffset := uint16(internal.PageSize)
		// Process in reverse order to pack from end
		for i := int(n.numKeys) - 1; i >= 0; i-- {
			key := n.keys[i]
			value := n.values[i]

			// Write value first (at end)
			dataOffset -= uint16(len(value))
			copy(page.Data[dataOffset:], value)
			valueOffset := dataOffset

			// Write key before value
			dataOffset -= uint16(len(key))
			copy(page.Data[dataOffset:], key)
			keyOffset := dataOffset

			elem := &internal.LeafElement{
				KeyOffset:   keyOffset,
				KeySize:     uint16(len(key)),
				ValueOffset: valueOffset,
				ValueSize:   uint16(len(value)),
			}
			page.WriteLeafElement(i, elem)
		}
	} else {
		// serialize branch Node (B+ tree: only keys, no values)
		// Write children[0] at fixed location (last 8 bytes)
		if len(n.children) > 0 {
			page.WriteBranchFirstChild(n.children[0])
		}

		// Pack keys from end backward (reserve last 8 bytes for children[0])
		dataOffset := uint16(internal.PageSize - 8)
		// Process in reverse order to pack from end
		for i := int(n.numKeys) - 1; i >= 0; i-- {
			key := n.keys[i]

			// Write key
			dataOffset -= uint16(len(key))
			copy(page.Data[dataOffset:], key)

			elem := &internal.BranchElement{
				KeyOffset: dataOffset,
				KeySize:   uint16(len(key)),
				Reserved:  0, // No values in B+ tree branches
				ChildID:   n.children[i+1],
			}
			page.WriteBranchElement(i, elem)
		}
	}

	return page, nil
}

// deserialize decodes the Page data into Node fields
func (n *Node) deserialize(p *internal.Page) error {
	header := p.Header()
	n.pageID = header.PageID
	n.numKeys = header.NumKeys
	n.isLeaf = (header.Flags & internal.LeafPageFlag) != 0

	if n.isLeaf {
		// deserialize leaf Node
		n.keys = make([][]byte, n.numKeys)
		n.values = make([][]byte, n.numKeys)
		n.children = nil

		elements := p.LeafElements()
		for i := 0; i < int(n.numKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := p.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy value
			valueData, err := p.GetValue(elem.ValueOffset, elem.ValueSize)
			if err != nil {
				return err
			}
			n.values[i] = make([]byte, len(valueData))
			copy(n.values[i], valueData)
		}
	} else {
		// deserialize branch Node (B+ tree: only keys, no values)
		n.keys = make([][]byte, n.numKeys)
		n.values = nil // Branch nodes don't have values
		n.children = make([]internal.PageID, n.numKeys+1)

		// Read children[0]
		n.children[0] = p.ReadBranchFirstChild()

		elements := p.BranchElements()
		for i := 0; i < int(n.numKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := p.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy child pointer
			n.children[i+1] = elem.ChildID
		}
	}

	return nil
}

// findKey returns the index of key in Node, or -1 if not found
func (n *Node) findKey(key []byte) int {
	for i := 0; i < int(n.numKeys); i++ {
		cmp := bytes.Compare(key, n.keys[i])
		if cmp == 0 {
			return i
		}
		if cmp < 0 {
			return -1
		}
	}
	return -1
}

// isUnderflow checks if Node has too few keys (doesn't apply to root)
func (n *Node) isUnderflow() bool {
	return int(n.numKeys) < MinKeysPerNode
}

// clone creates a deep copy of this Node for copy-on-write
// The clone is marked dirty and does not have a PageID allocated yet
func (n *Node) clone() *Node {
	cloned := &Node{
		pageID:  0,
		dirty:   true,
		isLeaf:  n.isLeaf,
		numKeys: n.numKeys,
	}

	// Deep copy keys
	cloned.keys = make([][]byte, len(n.keys))
	for i, key := range n.keys {
		cloned.keys[i] = make([]byte, len(key))
		copy(cloned.keys[i], key)
	}

	// Deep copy values (leaf nodes only)
	if n.isLeaf && len(n.values) > 0 {
		cloned.values = make([][]byte, len(n.values))
		for i, val := range n.values {
			cloned.values[i] = make([]byte, len(val))
			copy(cloned.values[i], val)
		}
	}

	// Deep copy children (branch nodes only)
	if !n.isLeaf && len(n.children) > 0 {
		cloned.children = make([]internal.PageID, len(n.children))
		copy(cloned.children, n.children)
	}

	return cloned
}

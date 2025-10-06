package fredb

import (
	"bytes"
)

const (
	// MaxKeysPerNode must be small enough that a full Node can serialize to PageSize
	MaxKeysPerNode = 64
	MinKeysPerNode = MaxKeysPerNode / 4 // Minimum keys for non-root nodes
)

// Node represents a B-tree Node with decoded Page data
type Node struct {
	pageID PageID
	dirty  bool

	// Decoded Node data
	isLeaf   bool
	numKeys  uint16
	keys     [][]byte
	values   [][]byte // Empty and unused in branch nodes
	children []PageID
}

// isFull checks if a Node is full
func (n *Node) isFull() bool {
	// Use key count active for both leaf and branch nodes
	// Overflow is detected during serialize with try-rollback
	return int(n.numKeys) >= MaxKeysPerNode
}

// size calculates the size of the serialized Node
func (n *Node) size() int {
	size := pageHeaderSize

	if n.isLeaf {
		size += int(n.numKeys) * leafElementSize
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) + len(n.values[i])
		}
	} else {
		// B+ tree: branch nodes only store keys (no values)
		size += int(n.numKeys) * branchElementSize
		size += 8 // children[0]
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) // Only keys, no values
		}
	}

	return size
}

// serialize encodes the Node data into a fresh Page
func (n *Node) serialize(txnID uint64) (*Page, error) {
	// Check size
	if n.size() > PageSize {
		return nil, ErrPageOverflow
	}

	// Create fresh Page
	page := &Page{}

	// Write header
	header := &pageHeader{
		PageID:    n.pageID,
		NumKeys:   n.numKeys,
		TxnID:     txnID,
		_NextLeaf: 0,
		_PrevLeaf: 0,
	}
	if n.isLeaf {
		header.Flags = LeafPageFlag
	} else {
		header.Flags = BranchPageFlag
	}
	page.writeHeader(header)

	if n.isLeaf {
		// serialize leaf Node - pack from end backward
		dataOffset := uint16(PageSize)
		// Process in reverse order to pack from end
		for i := int(n.numKeys) - 1; i >= 0; i-- {
			key := n.keys[i]
			value := n.values[i]

			// Write value first (at end)
			dataOffset -= uint16(len(value))
			copy(page.data[dataOffset:], value)
			valueOffset := dataOffset

			// Write key before value
			dataOffset -= uint16(len(key))
			copy(page.data[dataOffset:], key)
			keyOffset := dataOffset

			elem := &leafElement{
				KeyOffset:   keyOffset,
				KeySize:     uint16(len(key)),
				ValueOffset: valueOffset,
				ValueSize:   uint16(len(value)),
			}
			page.writeLeafElement(i, elem)
		}
	} else {
		// serialize branch Node (B+ tree: only keys, no values)
		// Write children[0] at fixed location (last 8 bytes)
		if len(n.children) > 0 {
			page.writeBranchFirstChild(n.children[0])
		}

		// Pack keys from end backward (reserve last 8 bytes for children[0])
		dataOffset := uint16(PageSize - 8)
		// Process in reverse order to pack from end
		for i := int(n.numKeys) - 1; i >= 0; i-- {
			key := n.keys[i]

			// Write key
			dataOffset -= uint16(len(key))
			copy(page.data[dataOffset:], key)

			elem := &branchElement{
				KeyOffset: dataOffset,
				KeySize:   uint16(len(key)),
				Reserved:  0, // No values in B+ tree branches
				ChildID:   n.children[i+1],
			}
			page.writeBranchElement(i, elem)
		}
	}

	return page, nil
}

// deserialize decodes the Page data into Node fields
func (n *Node) deserialize(p *Page) error {
	header := p.header()
	n.pageID = header.PageID
	n.numKeys = header.NumKeys
	n.isLeaf = (header.Flags & LeafPageFlag) != 0

	if n.isLeaf {
		// deserialize leaf Node
		n.keys = make([][]byte, n.numKeys)
		n.values = make([][]byte, n.numKeys)
		n.children = nil

		elements := p.leafElements()
		for i := 0; i < int(n.numKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := p.getKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy value
			valueData, err := p.getValue(elem.ValueOffset, elem.ValueSize)
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
		n.children = make([]PageID, n.numKeys+1)

		// Read children[0]
		n.children[0] = p.readBranchFirstChild()

		elements := p.branchElements()
		for i := 0; i < int(n.numKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := p.getKey(elem.KeyOffset, elem.KeySize)
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
		cloned.children = make([]PageID, len(n.children))
		copy(cloned.children, n.children)
	}

	return cloned
}

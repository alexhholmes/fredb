package base

import (
	"bytes"
	"sync"
)

const (
	// MinKeysPerNode is the minimum Keys for non-root nodes
	MinKeysPerNode = 16
)

var Pool = sync.Pool{
	New: func() any {
		return &Node{
			Keys:     make([][]byte, 0, MinKeysPerNode),
			Values:   make([][]byte, 0, MinKeysPerNode),
			Children: make([]PageID, 0, MinKeysPerNode),
			// arena: nil - lazy allocate only for read path (Deserialize)
		}
	},
}

// Node represents a B-tree Node with decoded Page data
type Node struct {
	PageID PageID
	Dirty  bool
	Leaf   bool
	TxID   uint64 // Transaction ID that last modified this node

	// Decoded Node data
	NumKeys  uint16
	Keys     [][]byte
	Values   [][]byte
	Children []PageID

	// Arena for key/value data - reduces allocations from ~128 to 2 per node
	arena []byte
}

func NewLeaf(id PageID, keys, values [][]byte) *Node {
	node := Pool.Get().(*Node)
	node.PageID = id
	node.Dirty = true
	node.Leaf = true
	node.NumKeys = uint16(len(keys))
	// Write path: direct slice append (fast, temporary allocations ok)
	node.Keys = append(node.Keys[:0], keys...)
	node.Values = append(node.Values[:0], values...)
	return node
}

func NewBranch(id PageID, keys [][]byte, children []PageID) *Node {
	node := Pool.Get().(*Node)
	node.PageID = id
	node.Dirty = true
	node.Leaf = false
	node.NumKeys = uint16(len(keys))
	// Write path: direct slice append (fast, temporary allocations ok)
	node.Keys = append(node.Keys[:0], keys...)
	node.Children = append(node.Children[:0], children...)
	return node
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
	if n.Leaf {
		header.Flags = LeafPageFlag
	} else {
		header.Flags = BranchPageFlag
	}
	page.WriteHeader(header)

	if n.Leaf {
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
	n.TxID = header.TxnID // Track transaction that wrote this node

	if (header.Flags & LeafPageFlag) != 0 {
		// Deserialize leaf Node using arena allocation
		n.Leaf = true
		n.Children = nil

		elements := p.LeafElements()

		// Calculate total space needed for all keys and values
		totalSize := 0
		for i := 0; i < int(n.NumKeys); i++ {
			totalSize += int(elements[i].KeySize) + int(elements[i].ValueSize)
		}

		// Resize arena if needed (reuse capacity when possible)
		if cap(n.arena) < totalSize {
			n.arena = make([]byte, totalSize)
		} else {
			n.arena = n.arena[:totalSize]
		}

		// Resize slice headers (cheap - no allocations)
		if cap(n.Keys) < int(n.NumKeys) {
			n.Keys = make([][]byte, n.NumKeys)
		} else {
			n.Keys = n.Keys[:n.NumKeys]
		}
		if cap(n.Values) < int(n.NumKeys) {
			n.Values = make([][]byte, n.NumKeys)
		} else {
			n.Values = n.Values[:n.NumKeys]
		}

		// Copy all data into arena and create slices
		offset := 0
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy key into arena
			keyData, err := p.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			keySize := int(elem.KeySize)
			copy(n.arena[offset:], keyData)
			n.Keys[i] = n.arena[offset : offset+keySize : offset+keySize]
			offset += keySize

			// Copy value into arena
			valueData, err := p.GetValue(elem.ValueOffset, elem.ValueSize)
			if err != nil {
				return err
			}
			valSize := int(elem.ValueSize)
			copy(n.arena[offset:], valueData)
			n.Values[i] = n.arena[offset : offset+valSize : offset+valSize]
			offset += valSize
		}
	} else {
		// Deserialize branch Node using arena allocation
		n.Leaf = false
		n.Values = nil

		elements := p.BranchElements()

		// Calculate total space needed for all keys
		totalSize := 0
		for i := 0; i < int(n.NumKeys); i++ {
			totalSize += int(elements[i].KeySize)
		}

		// Resize arena if needed
		if cap(n.arena) < totalSize {
			n.arena = make([]byte, totalSize)
		} else {
			n.arena = n.arena[:totalSize]
		}

		// Resize slice headers
		if cap(n.Keys) < int(n.NumKeys) {
			n.Keys = make([][]byte, n.NumKeys)
		} else {
			n.Keys = n.Keys[:n.NumKeys]
		}
		if cap(n.Children) < int(n.NumKeys)+1 {
			n.Children = make([]PageID, n.NumKeys+1)
		} else {
			n.Children = n.Children[:n.NumKeys+1]
		}

		// Read Children[0]
		n.Children[0] = p.ReadBranchFirstChild()

		// Copy all keys into arena
		offset := 0
		for i := 0; i < int(n.NumKeys); i++ {
			elem := elements[i]

			// Copy key into arena
			keyData, err := p.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			keySize := int(elem.KeySize)
			copy(n.arena[offset:], keyData)
			n.Keys[i] = n.arena[offset : offset+keySize : offset+keySize]
			offset += keySize

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
// Write path: direct allocations for speed (temporary, will be GC'd quickly)
func (n *Node) Clone() *Node {
	cloned := Pool.Get().(*Node)
	cloned.PageID = 0
	cloned.Dirty = true
	cloned.Leaf = n.Leaf
	cloned.NumKeys = n.NumKeys

	// Deep copy Keys (direct alloc - fast write path)
	cloned.Keys = cloned.Keys[:0]
	for _, key := range n.Keys {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		cloned.Keys = append(cloned.Keys, keyCopy)
	}

	// Deep copy Values (leaf nodes only)
	if n.Leaf && len(n.Values) > 0 {
		cloned.Values = cloned.Values[:0]
		for _, val := range n.Values {
			valCopy := make([]byte, len(val))
			copy(valCopy, val)
			cloned.Values = append(cloned.Values, valCopy)
		}
	} else {
		cloned.Values = cloned.Values[:0]
	}

	// Copy Children (branch nodes only)
	if !n.Leaf {
		cloned.Children = append(cloned.Children[:0], n.Children...)
	} else {
		cloned.Children = cloned.Children[:0]
	}

	return cloned
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
	if n.Leaf {
		return n.Size()+LeafElementSize+len(key)+len(value) > PageSize
	}
	// Branch nodes only have keys, no values
	return n.Size()+BranchElementSize+len(key) > PageSize
}

// Size calculates the Size of the serialized Node
func (n *Node) Size() int {
	size := PageHeaderSize

	if n.Leaf {
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

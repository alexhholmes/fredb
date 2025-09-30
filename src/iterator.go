package src

import "bytes"

// Cursor provides ordered iteration over B-tree keys
type Cursor struct {
	btree *BTree
	node  *Node  // Current leaf node
	index int    // Position within current node
	key   []byte // Cached current key
	value []byte // Cached current value
	valid bool   // Is cursor positioned on valid key?
}

// Seek positions cursor at first key >= target
// Returns error if tree traversal fails
func (it *Cursor) Seek(key []byte) error {
	it.valid = false

	// Navigate to leaf containing key (standard B-tree descent)
	node := it.btree.root
	for !node.isLeaf {
		i := 0
		for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
			i++
		}

		child, err := it.btree.loadNode(node.children[i])
		if err != nil {
			return err
		}
		node = child
	}

	// Find position within leaf using binary search
	i := 0
	for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
		i++
	}

	it.node = node
	it.index = i

	// If positioned within node, we're valid
	if i < int(node.numKeys) {
		it.key = node.keys[i]
		it.value = node.values[i]
		it.valid = true
		return nil
	}

	// Key not in this node - advance to next leaf
	return it.moveToNextLeaf()
}

// Next advances cursor to next key
// Returns true if advanced successfully, false if exhausted
func (it *Cursor) Next() bool {
	if !it.valid {
		return false
	}

	it.index++

	// Still within current node?
	if it.index < int(it.node.numKeys) {
		it.key = it.node.keys[it.index]
		it.value = it.node.values[it.index]
		return true
	}

	// Move to next leaf
	err := it.moveToNextLeaf()
	return err == nil && it.valid
}

// Prev moves cursor to previous key
// Returns true if moved successfully, false if at beginning
func (it *Cursor) Prev() bool {
	if !it.valid {
		return false
	}

	it.index--

	// Still within current node?
	if it.index >= 0 {
		it.key = it.node.keys[it.index]
		it.value = it.node.values[it.index]
		return true
	}

	// Move to previous leaf
	err := it.moveToPrevLeaf()
	return err == nil && it.valid
}

// Key returns current key (only valid when Valid() == true)
func (it *Cursor) Key() []byte {
	return it.key
}

// Value returns current value (only valid when Valid() == true)
func (it *Cursor) Value() []byte {
	return it.value
}

// Valid returns true if cursor is positioned on a valid key
func (it *Cursor) Valid() bool {
	return it.valid
}

// moveToNextLeaf advances to next leaf via sibling pointer
func (it *Cursor) moveToNextLeaf() error {
	nextID := it.node.getNextLeaf()
	if nextID == 0 {
		it.valid = false
		return nil
	}

	nextNode, err := it.btree.loadNode(nextID)
	if err != nil {
		it.valid = false
		return err
	}

	it.node = nextNode
	it.index = 0

	if it.node.numKeys > 0 {
		it.key = it.node.keys[0]
		it.value = it.node.values[0]
		it.valid = true
	} else {
		it.valid = false
	}

	return nil
}

// moveToPrevLeaf moves to previous leaf via sibling pointer
func (it *Cursor) moveToPrevLeaf() error {
	prevID := it.node.getPrevLeaf()
	if prevID == 0 {
		it.valid = false
		return nil
	}

	prevNode, err := it.btree.loadNode(prevID)
	if err != nil {
		it.valid = false
		return err
	}

	it.node = prevNode
	it.index = int(it.node.numKeys) - 1

	if it.index >= 0 {
		it.key = it.node.keys[it.index]
		it.value = it.node.values[it.index]
		it.valid = true
	} else {
		it.valid = false
	}

	return nil
}
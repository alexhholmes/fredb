package src

import "bytes"

// pathElem represents one level in the cursor's navigation path from root to leaf
// For branch nodes: childIndex is which child we descended to
// For leaf nodes: childIndex is which key we're currently at
type pathElem struct {
	node       *Node
	childIndex int
}

// Cursor provides ordered iteration over B-tree keys
type Cursor struct {
	btree *BTree
	stack []pathElem // Navigation path from root to current leaf
	key   []byte     // Cached current key
	value []byte     // Cached current value
	valid bool       // Is cursor positioned on valid key?
}

// Seek positions cursor at first key >= target
// Returns error if tree traversal fails
func (it *Cursor) Seek(key []byte) error {
	it.valid = false
	it.stack = it.stack[:0] // Clear stack

	// Navigate to leaf containing key, building stack
	node := it.btree.root
	for !node.isLeaf {
		// Find which child to descend to
		i := 0
		for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
			i++
		}

		// Push current node and child index to stack
		it.stack = append(it.stack, pathElem{node: node, childIndex: i})

		// Descend to child
		child, err := it.btree.loadNode(node.children[i])
		if err != nil {
			return err
		}
		node = child
	}

	// Find position within leaf
	i := 0
	for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
		i++
	}

	// Push leaf to stack (childIndex is key position in leaf)
	it.stack = append(it.stack, pathElem{node: node, childIndex: i})

	// If positioned within node, we're valid
	if i < int(node.numKeys) {
		it.key = node.keys[i]
		it.value = node.values[i]
		it.valid = true
		return nil
	}

	// Key not in this node - advance to next leaf
	return it.nextLeaf()
}

// Next advances cursor to next key
// Returns true if advanced successfully, false if exhausted
func (it *Cursor) Next() bool {
	if !it.valid {
		return false
	}

	// Try to move within current leaf
	leaf := &it.stack[len(it.stack)-1]
	leaf.childIndex++

	if leaf.childIndex < int(leaf.node.numKeys) {
		it.key = leaf.node.keys[leaf.childIndex]
		it.value = leaf.node.values[leaf.childIndex]
		return true
	}

	// Exhausted current leaf, move to next
	return it.nextLeaf() == nil && it.valid
}

// Prev moves cursor to previous key
// Returns true if moved successfully, false if at beginning
func (it *Cursor) Prev() bool {
	if !it.valid {
		return false
	}

	// Try to move within current leaf
	leaf := &it.stack[len(it.stack)-1]
	leaf.childIndex--

	if leaf.childIndex >= 0 {
		it.key = leaf.node.keys[leaf.childIndex]
		it.value = leaf.node.values[leaf.childIndex]
		return true
	}

	// Exhausted current leaf, move to previous
	return it.prevLeaf() == nil && it.valid
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

// nextLeaf advances to next leaf via tree navigation
func (it *Cursor) nextLeaf() error {
	// Pop up the stack to find a parent with more children
	for len(it.stack) > 1 {
		// Pop current leaf
		it.stack = it.stack[:len(it.stack)-1]

		// Check parent
		parent := &it.stack[len(it.stack)-1]
		parent.childIndex++

		// Does parent have more children?
		if parent.childIndex < len(parent.node.children) {
			// Descend to leftmost leaf of next subtree
			return it.descendToFirstLeaf()
		}
	}

	// Reached root with no more children
	it.valid = false
	return nil
}

// prevLeaf moves to previous leaf via tree navigation
func (it *Cursor) prevLeaf() error {
	// Pop up the stack to find a parent with more children to the left
	for len(it.stack) > 1 {
		// Pop current leaf
		it.stack = it.stack[:len(it.stack)-1]

		// Check parent
		parent := &it.stack[len(it.stack)-1]
		parent.childIndex--

		// Does parent have more children to the left?
		if parent.childIndex >= 0 {
			// Descend to rightmost leaf of previous subtree
			return it.descendToLastLeaf()
		}
	}

	// Reached root with no more children to the left
	it.valid = false
	return nil
}

// descendToFirstLeaf descends to the leftmost leaf from current stack top
func (it *Cursor) descendToFirstLeaf() error {
	parent := it.stack[len(it.stack)-1]
	node, err := it.btree.loadNode(parent.node.children[parent.childIndex])
	if err != nil {
		it.valid = false
		return err
	}

	// Keep descending to leftmost child
	for !node.isLeaf {
		it.stack = append(it.stack, pathElem{node: node, childIndex: 0})
		child, err := it.btree.loadNode(node.children[0])
		if err != nil {
			it.valid = false
			return err
		}
		node = child
	}

	// Reached leaf
	it.stack = append(it.stack, pathElem{node: node, childIndex: 0})

	if node.numKeys > 0 {
		it.key = node.keys[0]
		it.value = node.values[0]
		it.valid = true
	} else {
		it.valid = false
	}

	return nil
}

// descendToLastLeaf descends to the rightmost leaf from current stack top
func (it *Cursor) descendToLastLeaf() error {
	parent := it.stack[len(it.stack)-1]
	node, err := it.btree.loadNode(parent.node.children[parent.childIndex])
	if err != nil {
		it.valid = false
		return err
	}

	// Keep descending to rightmost child
	for !node.isLeaf {
		lastChild := len(node.children) - 1
		it.stack = append(it.stack, pathElem{node: node, childIndex: lastChild})
		child, err := it.btree.loadNode(node.children[lastChild])
		if err != nil {
			it.valid = false
			return err
		}
		node = child
	}

	// Reached leaf
	lastIndex := int(node.numKeys) - 1
	it.stack = append(it.stack, pathElem{node: node, childIndex: lastIndex})

	if lastIndex >= 0 {
		it.key = node.keys[lastIndex]
		it.value = node.values[lastIndex]
		it.valid = true
	} else {
		it.valid = false
	}

	return nil
}
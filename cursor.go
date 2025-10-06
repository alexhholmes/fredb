package fredb

import "bytes"

// Special marker values for Seek operations
var (
	// START is a special marker for seeking to the first key in the database
	// Usage: cursor.Seek(pkg.START) or cursor.SeekFirst()
	START = []byte{}

	// END is a special marker for seeking to the last key in the database
	// Since max key size is MaxKeySize (1024 bytes), END is 1024 bytes of 0xFF
	// Usage: cursor.Seek(pkg.END) positions at last key (or invalid if empty)
	// Useful for range bounds: bytes.Compare(key, END) < 0
	END = make([]byte, MaxKeySize) // MaxKeySize bytes of 0xFF
)

func init() {
	// Initialize END to all 0xFF bytes
	for i := range END {
		END[i] = 0xFF
	}
}

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
	tx    *Tx        // Transaction this cursor belongs to
	stack []pathElem // Navigation path from root to current leaf
	key   []byte     // Cached current key
	value []byte     // Cached current value
	valid bool       // Is cursor positioned on valid key?
}

// active validates that the cursor's transaction is still active
func (it *Cursor) active() error {
	if it.tx != nil {
		return it.tx.check()
	}
	return nil
}

// SeekFirst positions cursor at the first key in the database
// Equivalent to Seek(START)
func (it *Cursor) SeekFirst() error {
	return it.Seek(START)
}

// SeekLast positions cursor at the last key in the database
// Equivalent to Seek(END)
// Returns invalid cursor if database is empty
func (it *Cursor) SeekLast() error {
	return it.Seek(END)
}

// Seek positions cursor at first key >= target
// Returns error if tree traversal fails
// Special cases:
//   - Seek(START) positions at first key in database
//   - Seek(END) positions at last key in database (or invalid if empty)
func (it *Cursor) Seek(key []byte) error {
	// validate transaction state
	if err := it.active(); err != nil {
		return err
	}

	it.valid = false
	it.stack = it.stack[:0] // Clear stack

	// get root from transaction for snapshot isolation
	// Use tx.root if set (modified in this tx), otherwise use btree.root
	var node *Node
	if it.tx != nil && it.tx.root != nil {
		node = it.tx.root
	} else {
		node = it.btree.root
	}
	for !node.isLeaf {
		// Find which child to descend to
		i := 0
		for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
			i++
		}

		// Push current Node and child index to stack
		it.stack = append(it.stack, pathElem{node: node, childIndex: i})

		// Descend to child
		child, err := it.btree.loadNode(it.tx, node.children[i])
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

	// If positioned within Node, we're valid
	if i < int(node.numKeys) {
		it.key = node.keys[i]
		it.value = node.values[i]
		it.valid = true
		return nil
	}

	// Special case: if seeking END, position on last key instead of advancing
	if bytes.Equal(key, END) {
		// Move back one position to get the actual last key
		it.stack[len(it.stack)-1].childIndex--
		if it.stack[len(it.stack)-1].childIndex >= 0 {
			leaf := it.stack[len(it.stack)-1]
			it.key = leaf.node.keys[leaf.childIndex]
			it.value = leaf.node.values[leaf.childIndex]
			it.valid = true
			return nil
		}
		// Current leaf is empty, try previous leaf
		return it.prevLeaf()
	}

	// Key not in this Node - advance to next leaf
	return it.nextLeaf()
}

// Next advances cursor to next key
// Returns true if advanced successfully, false if exhausted
// B+ tree: only visits leaf nodes (all data is in leaves)
func (it *Cursor) Next() bool {
	// validate transaction state
	if err := it.active(); err != nil {
		it.valid = false
		return false
	}

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
// B+ tree: only visits leaf nodes (all data is in leaves)
func (it *Cursor) Prev() bool {
	// validate transaction state
	if err := it.active(); err != nil {
		it.valid = false
		return false
	}

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
// B+ tree: skip branch nodes, only visit leaves
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
// B+ tree: skip branch nodes, only visit leaves
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
	node, err := it.btree.loadNode(it.tx, parent.node.children[parent.childIndex])
	if err != nil {
		it.valid = false
		return err
	}

	// Keep descending to leftmost child
	for !node.isLeaf {
		it.stack = append(it.stack, pathElem{node: node, childIndex: 0})
		child, err := it.btree.loadNode(it.tx, node.children[0])
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
	node, err := it.btree.loadNode(it.tx, parent.node.children[parent.childIndex])
	if err != nil {
		it.valid = false
		return err
	}

	// Keep descending to rightmost child
	for !node.isLeaf {
		lastChild := len(node.children) - 1
		it.stack = append(it.stack, pathElem{node: node, childIndex: lastChild})
		child, err := it.btree.loadNode(it.tx, node.children[lastChild])
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

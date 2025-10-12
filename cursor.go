package fredb

import (
	"bytes"

	"fredb/internal/base"
)

var (
	// START is a special marker for seeking to the first key in the database
	// Usage: cursor.Seek(pkg.START) or cursor.SeekFirst()
	START []byte

	// END is a special marker for seeking to the last key in the database
	// Since max key size is MaxKeySize (1024 bytes), END is 1024 bytes of 0xFF
	// Usage: cursor.Seek(pkg.END) positions at last key (or invalid if empty)
	END = make([]byte, MaxKeySize) // MaxKeySize bytes of 0xFF
)

func init() {
	// Initialize END to all 0xFF bytes
	for i := range END {
		END[i] = 0xFF
	}
}

// path represents one level in the cursor's navigation path from root to leaf
// For branch nodes: childIndex is which child we descended to
// For leaf nodes: childIndex is which key we're currently at
type path struct {
	node       *base.Node
	childIndex int
}

// Cursor provides ordered iteration over B-tree Keys
type Cursor struct {
	tx         *Tx        // Transaction this cursor belongs to
	bucketRoot *base.Node // Bucket's root (if nil, use tx.root)
	stack      []path     // Navigation path from root to current leaf
	key        []byte     // Cached current key
	value      []byte     // Cached current value
	valid      bool       // Is cursor positioned on valid key?
}

// getRoot returns the root to use for this cursor
func (c *Cursor) getRoot() *base.Node {
	if c.bucketRoot != nil {
		return c.bucketRoot
	}
	return c.tx.root
}

// shouldSkip returns true if the key should be skipped (internal keys like __root__)
func (c *Cursor) shouldSkip(key []byte) bool {
	// If iterating root tree (bucket directory), skip __root__ internal bucket
	root := c.getRoot()
	if root == c.tx.root {
		return string(key) == "__root__"
	}
	return false
}

// First positions cursor at the first key in the database
// Equivalent to Seek(START)
func (it *Cursor) First() ([]byte, []byte) {
	if err := it.active(); err != nil {
		return nil, nil
	}

	it.stack = nil
	it.valid = false

	root := it.getRoot()
	if root == nil {
		return nil, nil
	}

	key, val := it.descendToFirst(root)

	// Skip __root__ if present
	if key != nil && it.shouldSkip(key) {
		return it.Next()
	}

	return key, val
}

// descendToFirst descends to the leftmost leaf and returns first key-value
func (it *Cursor) descendToFirst(node *base.Node) ([]byte, []byte) {
	// Descend to leftmost leaf
	for !node.IsLeaf() {
		it.stack = append(it.stack, path{node: node, childIndex: 0})
		child, err := it.tx.loadNode(node.Children[0])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// At leftmost leaf
	it.stack = append(it.stack, path{node: node, childIndex: 0})

	if node.NumKeys > 0 {
		it.key = node.Keys[0]
		it.value = node.Values[0]
		it.valid = true
		return it.key, it.value
	}

	return nil, nil
}

// Last positions cursor at the last key in the database
// Equivalent to Seek(END)
// Returns invalid cursor if database is empty
func (it *Cursor) Last() ([]byte, []byte) {
	if err := it.active(); err != nil {
		return nil, nil
	}

	it.stack = nil
	it.valid = false

	root := it.getRoot()
	if root == nil {
		return nil, nil
	}

	key, val := it.descendToLast(root)

	// Skip __root__ if present
	if key != nil && it.shouldSkip(key) {
		return it.Prev()
	}

	return key, val
}

// descendToLast descends to the rightmost leaf and returns last key-value
func (it *Cursor) descendToLast(root *base.Node) ([]byte, []byte) {
	node := root

	// Descend to rightmost leaf
	for !node.IsLeaf() {
		lastChild := len(node.Children) - 1
		it.stack = append(it.stack, path{node: node, childIndex: lastChild})
		child, err := it.tx.loadNode(node.Children[lastChild])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// At rightmost leaf
	lastIndex := int(node.NumKeys) - 1
	it.stack = append(it.stack, path{node: node, childIndex: lastIndex})

	if lastIndex >= 0 {
		it.key = node.Keys[lastIndex]
		it.value = node.Values[lastIndex]
		it.valid = true
		return it.key, it.value
	}

	return nil, nil
}

// Seek positions cursor at first key >= target
// Returns error if tree traversal fails
// Special cases:
//   - Seek(START) positions at first key in database
//   - Seek(END) positions at last key in database (or invalid if empty)
func (it *Cursor) Seek(seek []byte) ([]byte, []byte) {
	if err := it.active(); err != nil {
		return nil, nil
	}

	// Special case: START (nil) positions at first key
	if seek == nil || len(seek) == 0 {
		return it.First()
	}

	// Special case: END positions at last key
	if bytes.Equal(seek, END) {
		return it.Last()
	}

	it.stack = nil
	it.valid = false

	root := it.getRoot()
	if root == nil {
		return nil, nil
	}

	key, val := it.seekTo(root, seek)

	// Skip __root__ if present
	if key != nil && it.shouldSkip(key) {
		return it.Next()
	}

	return key, val
}

// seekTo performs the actual seek operation
func (it *Cursor) seekTo(root *base.Node, seek []byte) ([]byte, []byte) {
	node := root

	// Descend to appropriate leaf
	for !node.IsLeaf() {
		i := 0
		for i < int(node.NumKeys) && bytes.Compare(seek, node.Keys[i]) > 0 {
			i++
		}
		it.stack = append(it.stack, path{node: node, childIndex: i})
		child, err := it.tx.loadNode(node.Children[i])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// Find position within leaf
	i := 0
	for i < int(node.NumKeys) && bytes.Compare(seek, node.Keys[i]) > 0 {
		i++
	}

	it.stack = append(it.stack, path{node: node, childIndex: i})

	if i < int(node.NumKeys) {
		it.key = node.Keys[i]
		it.value = node.Values[i]
		it.valid = true
		return it.key, it.value
	}

	return nil, nil
}

// Next advances cursor to next key
// Returns key, value (nil, nil if exhausted)
func (it *Cursor) Next() ([]byte, []byte) {
	if err := it.active(); err != nil {
		it.valid = false
		return nil, nil
	}

	if !it.valid || len(it.stack) == 0 {
		return nil, nil
	}

	// Try to move within current leaf
	leaf := &it.stack[len(it.stack)-1]
	leaf.childIndex++

	if leaf.childIndex < int(leaf.node.NumKeys) {
		it.key = leaf.node.Keys[leaf.childIndex]
		it.value = leaf.node.Values[leaf.childIndex]

		// Skip __root__ if present
		if it.shouldSkip(it.key) {
			return it.Next()
		}
		return it.key, it.value
	}

	// Exhausted current leaf, move to next
	err := it.nextLeaf()
	if err != nil || !it.valid {
		return nil, nil
	}

	// Skip __root__ if present
	if it.shouldSkip(it.key) {
		return it.Next()
	}
	return it.key, it.value
}

// Prev moves cursor to previous key
// Returns key, value (nil, nil if at beginning)
func (it *Cursor) Prev() ([]byte, []byte) {
	if err := it.active(); err != nil {
		it.valid = false
		return nil, nil
	}

	if !it.valid || len(it.stack) == 0 {
		return nil, nil
	}

	// Try to move within current leaf
	leaf := &it.stack[len(it.stack)-1]
	leaf.childIndex--

	if leaf.childIndex >= 0 {
		it.key = leaf.node.Keys[leaf.childIndex]
		it.value = leaf.node.Values[leaf.childIndex]

		// Skip __root__ if present
		if it.shouldSkip(it.key) {
			return it.Prev()
		}
		return it.key, it.value
	}

	// Exhausted current leaf, move to previous
	err := it.prevLeaf()
	if err != nil || !it.valid {
		return nil, nil
	}

	// Skip __root__ if present
	if it.shouldSkip(it.key) {
		return it.Prev()
	}
	return it.key, it.value
}

// Key returns current key (only valid when Valid() == true)
func (it *Cursor) Key() []byte {
	if err := it.active(); err != nil {
		return nil
	}
	return it.key
}

// Value returns current value (only valid when Valid() == true)
func (it *Cursor) Value() []byte {
	if err := it.active(); err != nil {
		return nil
	}
	return it.value
}

// Valid returns true if cursor is positioned on a valid key
func (it *Cursor) Valid() bool {
	if err := it.active(); err != nil {
		return false
	}
	return it.valid
}

// active validates that the cursor's transaction is still active
func (it *Cursor) active() error {
	if it.tx != nil {
		return it.tx.check()
	}
	return nil
}

// nextLeaf advances to next leaf via tree navigation
// B+ tree: skip branch nodes, only visit leaves
func (it *Cursor) nextLeaf() error {
	// Pop up the stack to find a parent with more Children
	for len(it.stack) > 1 {
		// Pop current leaf
		it.stack = it.stack[:len(it.stack)-1]

		// Check parent
		parent := &it.stack[len(it.stack)-1]
		parent.childIndex++

		// Does parent have more Children?
		if parent.childIndex < len(parent.node.Children) {
			// Descend to leftmost leaf of next subtree
			node, err := it.tx.loadNode(parent.node.Children[parent.childIndex])
			if err != nil {
				it.valid = false
				return err
			}

			// Keep descending to leftmost child
			for !node.IsLeaf() {
				it.stack = append(it.stack, path{node: node, childIndex: 0})
				child, err := it.tx.loadNode(node.Children[0])
				if err != nil {
					it.valid = false
					return err
				}
				node = child
			}

			// Reached leaf
			it.stack = append(it.stack, path{node: node, childIndex: 0})

			if node.NumKeys > 0 {
				it.key = node.Keys[0]
				it.value = node.Values[0]
				it.valid = true
			} else {
				it.valid = false
			}

			return nil
		}
	}

	// Reached root with no more Children
	it.valid = false
	return nil
}

// prevLeaf moves to previous leaf via tree navigation
// B+ tree: skip branch nodes, only visit leaves
func (it *Cursor) prevLeaf() error {
	// Pop up the stack to find a parent with more Children to the left
	for len(it.stack) > 1 {
		// Pop current leaf
		it.stack = it.stack[:len(it.stack)-1]

		// Check parent
		parent := &it.stack[len(it.stack)-1]
		parent.childIndex--

		// Does parent have more Children to the left?
		if parent.childIndex >= 0 {
			// Descend to rightmost leaf of previous subtree
			node, err := it.tx.loadNode(parent.node.Children[parent.childIndex])
			if err != nil {
				it.valid = false
				return err
			}

			// Keep descending to rightmost child
			for !node.IsLeaf() {
				lastChild := len(node.Children) - 1
				it.stack = append(it.stack, path{node: node, childIndex: lastChild})
				child, err := it.tx.loadNode(node.Children[lastChild])
				if err != nil {
					it.valid = false
					return err
				}
				node = child
			}

			// Reached leaf
			lastIndex := int(node.NumKeys) - 1
			it.stack = append(it.stack, path{node: node, childIndex: lastIndex})

			if lastIndex >= 0 {
				it.key = node.Keys[lastIndex]
				it.value = node.Values[lastIndex]
				it.valid = true
			} else {
				it.valid = false
			}

			return nil
		}
	}

	// Reached root with no more Children to the left
	it.valid = false
	return nil
}

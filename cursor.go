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

// First positions cursor at the first key in the database
// Equivalent to Seek(START)
func (c *Cursor) First() ([]byte, []byte) {
	if err := c.active(); err != nil {
		return nil, nil
	}

	c.stack = nil
	c.valid = false

	root := c.getRoot()
	if root == nil {
		return nil, nil
	}

	// Descend to leftmost leaf
	node := root
	for !node.IsLeaf() {
		c.stack = append(c.stack, path{node: node, childIndex: 0})
		child, err := c.tx.loadNode(node.Children[0])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// At leftmost leaf
	c.stack = append(c.stack, path{node: node, childIndex: 0})

	if node.NumKeys > 0 {
		c.key = node.Keys[0]
		c.value = node.Values[0]
		c.valid = true

		// Skip __root__ if present
		if c.shouldSkip(c.key) {
			return c.Next()
		}

		return c.key, c.value
	}

	return nil, nil
}

// Last positions cursor at the last key in the database
// Equivalent to Seek(END)
// Returns invalid cursor if database is empty
func (c *Cursor) Last() ([]byte, []byte) {
	if err := c.active(); err != nil {
		return nil, nil
	}

	c.stack = nil
	c.valid = false

	root := c.getRoot()
	if root == nil {
		return nil, nil
	}

	// Descend to rightmost leaf
	node := root
	for !node.IsLeaf() {
		lastChild := len(node.Children) - 1
		c.stack = append(c.stack, path{node: node, childIndex: lastChild})
		child, err := c.tx.loadNode(node.Children[lastChild])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// At rightmost leaf
	lastIndex := int(node.NumKeys) - 1
	c.stack = append(c.stack, path{node: node, childIndex: lastIndex})

	if lastIndex >= 0 {
		c.key = node.Keys[lastIndex]
		c.value = node.Values[lastIndex]
		c.valid = true

		// Skip __root__ if present
		if c.shouldSkip(c.key) {
			return c.Prev()
		}

		return c.key, c.value
	}

	return nil, nil
}

// Seek positions cursor at first key >= target
// Returns error if tree traversal fails
// Special cases:
//   - Seek(START) positions at first key in database
//   - Seek(END) positions at last key in database (or invalid if empty)
func (c *Cursor) Seek(seek []byte) ([]byte, []byte) {
	if err := c.active(); err != nil {
		return nil, nil
	}

	// Special case: START (nil) positions at first key
	if seek == nil || len(seek) == 0 {
		return c.First()
	}

	// Special case: END positions at last key
	if bytes.Equal(seek, END) {
		return c.Last()
	}

	c.stack = nil
	c.valid = false

	root := c.getRoot()
	if root == nil {
		return nil, nil
	}

	// Descend to appropriate leaf
	node := root
	for !node.IsLeaf() {
		i := 0
		for i < int(node.NumKeys) && bytes.Compare(seek, node.Keys[i]) > 0 {
			i++
		}
		c.stack = append(c.stack, path{node: node, childIndex: i})
		child, err := c.tx.loadNode(node.Children[i])
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

	c.stack = append(c.stack, path{node: node, childIndex: i})

	if i < int(node.NumKeys) {
		c.key = node.Keys[i]
		c.value = node.Values[i]
		c.valid = true

		// Skip __root__ if present
		if c.shouldSkip(c.key) {
			return c.Next()
		}

		return c.key, c.value
	}

	return nil, nil
}

// Next advances cursor to next key
// Returns key, value (nil, nil if exhausted)
func (c *Cursor) Next() ([]byte, []byte) {
	if err := c.active(); err != nil {
		c.valid = false
		return nil, nil
	}

	if !c.valid || len(c.stack) == 0 {
		return nil, nil
	}

	// Try to move within current leaf
	leaf := &c.stack[len(c.stack)-1]
	leaf.childIndex++

	if leaf.childIndex < int(leaf.node.NumKeys) {
		c.key = leaf.node.Keys[leaf.childIndex]
		c.value = leaf.node.Values[leaf.childIndex]

		// Skip __root__ if present
		if c.shouldSkip(c.key) {
			return c.Next()
		}
		return c.key, c.value
	}

	// Exhausted current leaf, move to next
	err := c.nextLeaf()
	if err != nil || !c.valid {
		return nil, nil
	}

	// Skip __root__ if present
	if c.shouldSkip(c.key) {
		return c.Next()
	}
	return c.key, c.value
}

// Prev moves cursor to previous key
// Returns key, value (nil, nil if at beginning)
func (c *Cursor) Prev() ([]byte, []byte) {
	if err := c.active(); err != nil {
		c.valid = false
		return nil, nil
	}

	if !c.valid || len(c.stack) == 0 {
		return nil, nil
	}

	// Try to move within current leaf
	leaf := &c.stack[len(c.stack)-1]
	leaf.childIndex--

	if leaf.childIndex >= 0 {
		c.key = leaf.node.Keys[leaf.childIndex]
		c.value = leaf.node.Values[leaf.childIndex]

		// Skip __root__ if present
		if c.shouldSkip(c.key) {
			return c.Prev()
		}
		return c.key, c.value
	}

	// Exhausted current leaf, move to previous
	err := c.prevLeaf()
	if err != nil || !c.valid {
		return nil, nil
	}

	// Skip __root__ if present
	if c.shouldSkip(c.key) {
		return c.Prev()
	}
	return c.key, c.value
}

// Key returns current key (only valid when Valid() == true)
func (c *Cursor) Key() []byte {
	if err := c.active(); err != nil {
		return nil
	}
	return c.key
}

// Value returns current value (only valid when Valid() == true)
func (c *Cursor) Value() []byte {
	if err := c.active(); err != nil {
		return nil
	}
	return c.value
}

// Valid returns true if cursor is positioned on a valid key
func (c *Cursor) Valid() bool {
	if err := c.active(); err != nil {
		return false
	}
	return c.valid
}

// active validates that the cursor's transaction is still active
func (c *Cursor) active() error {
	if c.tx != nil {
		return c.tx.check()
	}
	return nil
}

// nextLeaf advances to next leaf via tree navigation
// B+ tree: skip branch nodes, only visit leaves
func (c *Cursor) nextLeaf() error {
	// Pop up the stack to find a parent with more Children
	for len(c.stack) > 1 {
		// Pop current leaf
		c.stack = c.stack[:len(c.stack)-1]

		// Check parent
		parent := &c.stack[len(c.stack)-1]
		parent.childIndex++

		// Does parent have more Children?
		if parent.childIndex < len(parent.node.Children) {
			// Descend to leftmost leaf of next subtree
			node, err := c.tx.loadNode(parent.node.Children[parent.childIndex])
			if err != nil {
				c.valid = false
				return err
			}

			// Keep descending to leftmost child
			for !node.IsLeaf() {
				c.stack = append(c.stack, path{node: node, childIndex: 0})
				child, err := c.tx.loadNode(node.Children[0])
				if err != nil {
					c.valid = false
					return err
				}
				node = child
			}

			// Reached leaf
			c.stack = append(c.stack, path{node: node, childIndex: 0})

			if node.NumKeys > 0 {
				c.key = node.Keys[0]
				c.value = node.Values[0]
				c.valid = true
			} else {
				c.valid = false
			}

			return nil
		}
	}

	// Reached root with no more Children
	c.valid = false
	return nil
}

// prevLeaf moves to previous leaf via tree navigation
// B+ tree: skip branch nodes, only visit leaves
func (c *Cursor) prevLeaf() error {
	// Pop up the stack to find a parent with more Children to the left
	for len(c.stack) > 1 {
		// Pop current leaf
		c.stack = c.stack[:len(c.stack)-1]

		// Check parent
		parent := &c.stack[len(c.stack)-1]
		parent.childIndex--

		// Does parent have more Children to the left?
		if parent.childIndex >= 0 {
			// Descend to rightmost leaf of previous subtree
			node, err := c.tx.loadNode(parent.node.Children[parent.childIndex])
			if err != nil {
				c.valid = false
				return err
			}

			// Keep descending to rightmost child
			for !node.IsLeaf() {
				lastChild := len(node.Children) - 1
				c.stack = append(c.stack, path{node: node, childIndex: lastChild})
				child, err := c.tx.loadNode(node.Children[lastChild])
				if err != nil {
					c.valid = false
					return err
				}
				node = child
			}

			// Reached leaf
			lastIndex := int(node.NumKeys) - 1
			c.stack = append(c.stack, path{node: node, childIndex: lastIndex})

			if lastIndex >= 0 {
				c.key = node.Keys[lastIndex]
				c.value = node.Values[lastIndex]
				c.valid = true
			} else {
				c.valid = false
			}

			return nil
		}
	}

	// Reached root with no more Children to the left
	c.valid = false
	return nil
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

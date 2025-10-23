package fredb

import (
	"bytes"

	"github.com/alexhholmes/fredb/internal/base"
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
	node       base.PageData
	childIndex int
}

// Cursor provides ordered iteration over B-tree Keys
type Cursor struct {
	tx         *Tx           // Transaction this cursor belongs to
	bucketRoot base.PageData // Bucket's root (if nil, use tx.root)
	stack      []path        // Navigation path from root to current leaf
	key        []byte        // Cached current key
	value      []byte        // Cached current value
	valid      bool          // Is cursor positioned on valid key?
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
	for node.PageType() != base.LeafPageFlag {
		branch := node.(*base.BranchPage)
		c.stack = append(c.stack, path{node: node, childIndex: 0})
		children := branch.Children()
		child, err := c.tx.loadNode(children[0])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// At leftmost leaf
	leaf := node.(*base.LeafPage)
	c.stack = append(c.stack, path{node: node, childIndex: 0})

	if leaf.Header.NumKeys > 0 {
		c.key = leaf.Keys[0]
		c.value = leaf.Values[0]
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
	for node.PageType() != base.LeafPageFlag {
		branch := node.(*base.BranchPage)
		children := branch.Children()
		lastChild := len(children) - 1
		c.stack = append(c.stack, path{node: node, childIndex: lastChild})
		child, err := c.tx.loadNode(children[lastChild])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// At rightmost leaf
	leaf := node.(*base.LeafPage)
	lastIndex := int(leaf.Header.NumKeys) - 1
	c.stack = append(c.stack, path{node: node, childIndex: lastIndex})

	if lastIndex >= 0 {
		c.key = leaf.Keys[lastIndex]
		c.value = leaf.Values[lastIndex]
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
	for node.PageType() != base.LeafPageFlag {
		branch := node.(*base.BranchPage)
		i := 0
		for i < int(branch.Header.NumKeys) && bytes.Compare(seek, branch.Keys[i]) > 0 {
			i++
		}
		c.stack = append(c.stack, path{node: node, childIndex: i})
		children := branch.Children()
		child, err := c.tx.loadNode(children[i])
		if err != nil {
			return nil, nil
		}
		node = child
	}

	// Find position within leaf
	leaf := node.(*base.LeafPage)
	i := 0
	for i < int(leaf.Header.NumKeys) && bytes.Compare(seek, leaf.Keys[i]) > 0 {
		i++
	}

	c.stack = append(c.stack, path{node: node, childIndex: i})

	if i < int(leaf.Header.NumKeys) {
		c.key = leaf.Keys[i]
		c.value = leaf.Values[i]
		c.valid = true

		// Skip __root__ if present
		if c.shouldSkip(c.key) {
			return c.Next()
		}

		return c.key, c.value
	}

	// Landed after last key in this leaf - advance to next leaf (bbolt semantics)
	c.valid = true // Mark valid so Next() can advance
	return c.Next()
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
	pathEntry := &c.stack[len(c.stack)-1]
	pathEntry.childIndex++

	leafNode := pathEntry.node.(*base.LeafPage)
	if pathEntry.childIndex < int(leafNode.Header.NumKeys) {
		c.key = leafNode.Keys[pathEntry.childIndex]
		c.value = leafNode.Values[pathEntry.childIndex]

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
	pathEntry := &c.stack[len(c.stack)-1]
	pathEntry.childIndex--

	leafNode := pathEntry.node.(*base.LeafPage)
	if pathEntry.childIndex >= 0 {
		c.key = leafNode.Keys[pathEntry.childIndex]
		c.value = leafNode.Values[pathEntry.childIndex]

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

	result := make([]byte, len(c.key))
	copy(result, c.key)
	return result
}

// Value returns current value (only valid when Valid() == true)
func (c *Cursor) Value() []byte {
	if err := c.active(); err != nil {
		return nil
	}
	result := make([]byte, len(c.value))
	copy(result, c.value)
	return result
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
	// Tree navigation
	// Pop up the stack to find a parent with more children
	for len(c.stack) > 1 {
		// Pop current leaf
		c.stack = c.stack[:len(c.stack)-1]

		// Check parent
		parent := &c.stack[len(c.stack)-1]
		parent.childIndex++

		// Does parent have more children?
		parentBranch := parent.node.(*base.BranchPage)
		parentChildren := parentBranch.Children()
		if parent.childIndex < len(parentChildren) {
			// Descend to leftmost leaf of next subtree
			node, err := c.tx.loadNode(parentChildren[parent.childIndex])
			if err != nil {
				c.valid = false
				return err
			}

			// Keep descending to leftmost child
			for node.PageType() != base.LeafPageFlag {
				branch := node.(*base.BranchPage)
				c.stack = append(c.stack, path{node: node, childIndex: 0})
				children := branch.Children()
				child, err := c.tx.loadNode(children[0])
				if err != nil {
					c.valid = false
					return err
				}
				node = child
			}

			// Reached leaf
			leaf := node.(*base.LeafPage)
			c.stack = append(c.stack, path{node: node, childIndex: 0})

			if leaf.Header.NumKeys > 0 {
				c.key = leaf.Keys[0]
				c.value = leaf.Values[0]
				c.valid = true
			} else {
				c.valid = false
			}

			return nil
		}
	}

	// Reached root with no more children
	c.valid = false
	return nil
}

// prevLeaf moves to previous leaf via tree navigation
// B+ tree: skip branch nodes, only visit leaves
func (c *Cursor) prevLeaf() error {
	// Tree navigation
	// Pop up the stack to find a parent with more children to the left
	for len(c.stack) > 1 {
		// Pop current leaf
		c.stack = c.stack[:len(c.stack)-1]

		// Check parent
		parent := &c.stack[len(c.stack)-1]
		parent.childIndex--

		// Does parent have more children to the left?
		if parent.childIndex >= 0 {
			// Descend to rightmost leaf of previous subtree
			parentBranch := parent.node.(*base.BranchPage)
			parentChildren := parentBranch.Children()
			node, err := c.tx.loadNode(parentChildren[parent.childIndex])
			if err != nil {
				c.valid = false
				return err
			}

			// Keep descending to rightmost child
			for node.PageType() != base.LeafPageFlag {
				branch := node.(*base.BranchPage)
				children := branch.Children()
				lastChild := len(children) - 1
				c.stack = append(c.stack, path{node: node, childIndex: lastChild})
				child, err := c.tx.loadNode(children[lastChild])
				if err != nil {
					c.valid = false
					return err
				}
				node = child
			}

			// Reached leaf
			leaf := node.(*base.LeafPage)
			lastIndex := int(leaf.Header.NumKeys) - 1
			c.stack = append(c.stack, path{node: node, childIndex: lastIndex})

			if lastIndex >= 0 {
				c.key = leaf.Keys[lastIndex]
				c.value = leaf.Values[lastIndex]
				c.valid = true
			} else {
				c.valid = false
			}

			return nil
		}
	}

	// Reached root with no more children to the left
	c.valid = false
	return nil
}

// getRoot returns the root to use for this cursor
func (c *Cursor) getRoot() base.PageData {
	if c.bucketRoot != nil {
		return c.bucketRoot
	}
	return c.tx.root
}

// shouldSkip returns true if the key should be skipped (internal keys like __root__)
func (c *Cursor) shouldSkip(key []byte) bool {
	// If iterating root tree (bucket directory), skip __root__ internal bucket
	root := c.getRoot()
	if root.GetPageID() == c.tx.root.GetPageID() {
		return string(key) == "__root__"
	}
	return false
}

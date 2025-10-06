package fredb

import (
	"bytes"
	"fmt"
)

// BTree is the main structure
type BTree struct {
	pager PageManager
	root  *node
	cache *PageCache // LRU cache for non-root nodes
}

// NewBTree creates a new BTree with the given PageManager
func NewBTree(pager PageManager) (*BTree, error) {
	meta := pager.GetMeta()

	bt := &BTree{
		pager: pager,
		cache: NewPageCache(DefaultCacheSize, pager),
	}

	// Check if existing root exists
	if meta.RootPageID != 0 {
		// Load existing root directly (no transaction during initialization)
		rootPage, err := pager.ReadPage(meta.RootPageID)
		if err != nil {
			return nil, err
		}

		root := &node{
			pageID: meta.RootPageID,
			dirty:  false,
		}

		// deserialize root
		if err := root.deserialize(rootPage); err != nil {
			return nil, err
		}

		bt.root = root
		// Root is never cached (always stays in bt.root)
		return bt, nil
	}

	// No existing root - allocate new one
	rootPageID, err := pager.AllocatePage()
	if err != nil {
		return nil, err
	}

	// Create root node
	root := &node{
		pageID:   rootPageID,
		dirty:    true,
		isLeaf:   true,
		numKeys:  0,
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]pageID, 0),
	}

	bt.root = root

	// Update meta with root page ID
	meta.RootPageID = rootPageID
	if err := pager.PutMeta(meta); err != nil {
		return nil, err
	}

	return bt, nil
}

// newCursor creates a new cursor for this B-tree
// Cursor starts in invalid state - call Seek() to position it
func (bt *BTree) newCursor(tx *Tx) *Cursor {
	return &Cursor{
		btree: bt,
		tx:    tx,
		valid: false,
	}
}

// close flushes any dirty pages and closes the B-tree
func (bt *BTree) close() error {
	// Flush root if dirty
	if bt.root != nil && bt.root.dirty {
		meta := bt.pager.GetMeta()
		page, err := bt.root.serialize(meta.TxnID)
		if err != nil {
			return err
		}
		if err := bt.pager.WritePage(bt.root.pageID, page); err != nil {
			return err
		}
		bt.root.dirty = false
	}

	// Flush all cached nodes
	if bt.cache != nil {
		if err := bt.cache.FlushDirty(&bt.pager); err != nil {
			return err
		}
	}

	// Clear references
	bt.cache = nil
	bt.root = nil

	// close pager (writes meta page and frees resources)
	return bt.pager.Close()
}

// searchNode recursively searches for a key in the tree
// B+ tree: only leaf nodes contain actual data
func (bt *BTree) searchNode(tx *Tx, node *node, key []byte) ([]byte, error) {
	// Find position in current node
	i := 0
	for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) >= 0 {
		i++
	}
	// After loop: i points to first key > search_key (or numKeys if all keys <= search_key)

	// If leaf node, active if key found
	if node.isLeaf {
		// In leaf, we need to active the previous position (since loop went past equal keys)
		if i > 0 && bytes.Equal(key, node.keys[i-1]) {
			return node.values[i-1], nil
		}
		// Not found in leaf
		return nil, ErrKeyNotFound
	}

	// Branch node: continue descending
	// i is the correct child index (first child with keys >= search_key)
	child, err := bt.loadNode(tx, node.children[i])
	if err != nil {
		return nil, err
	}

	return bt.searchNode(tx, child, key)
}

// loadNode loads a node using hybrid cache: tx.pages → versioned global cache → disk
func (bt *BTree) loadNode(tx *Tx, pageID pageID) (*node, error) {
	// MVCC requires a transaction for snapshot isolation
	if tx == nil {
		return nil, fmt.Errorf("loadNode requires a transaction (cannot be nil)")
	}

	// 1. Check TX-local cache first (if writable tx with uncommitted changes)
	if tx.writable && tx.pages != nil {
		if node, exists := tx.pages[pageID]; exists {
			return node, nil
		}
	}

	// 2. GetOrLoad atomically checks cache or coordinates disk load
	node, found := bt.cache.GetOrLoad(pageID, tx.txnID, func() (*node, uint64, error) {
		// Load from disk (called by at most one thread per pageID)
		page, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return nil, 0, err
		}

		// Create node and deserialize
		node := &node{
			pageID: pageID,
			dirty:  false,
		}

		// Try to deserialize - if page is empty (new page), header.NumKeys will be 0
		header := page.header()
		if header.NumKeys > 0 {
			if err := node.deserialize(page); err != nil {
				return nil, 0, err
			}
		} else {
			// New/empty page - initialize as empty leaf
			node.pageID = pageID
			node.isLeaf = true
			node.numKeys = 0
			node.keys = make([][]byte, 0)
			node.values = make([][]byte, 0)
			node.children = make([]pageID, 0)
		}

		// Cycle detection: active if deserialized node references itself
		if !node.isLeaf {
			for _, childID := range node.children {
				if childID == pageID {
					return nil, 0, ErrCorruption // Self-reference detected
				}
			}
		}

		// Return node and its TxnID from disk header
		return node, header.TxnID, nil
	})

	if !found {
		// Version not visible or load failed
		return nil, ErrKeyNotFound
	}

	// Cycle detection on cached node (fast path)
	if !node.isLeaf {
		for _, childID := range node.children {
			if childID == pageID {
				return nil, ErrCorruption // Self-reference detected
			}
		}
	}

	return node, nil
}

func insertAt(slice [][]byte, index int, value []byte) [][]byte {
	slice = append(slice[:index], append([][]byte{value}, slice[index:]...)...)
	return slice
}

// removeAt removes element at index from slice
func removeAt(slice [][]byte, index int) [][]byte {
	return append(slice[:index], slice[index+1:]...)
}

// removeChildAt removes child at index from slice
func removeChildAt(slice []pageID, index int) []pageID {
	return append(slice[:index], slice[index+1:]...)
}

// findPredecessor finds the predecessor key/value in the subtree rooted at node
func (bt *BTree) findPredecessor(tx *Tx, current *node) ([]byte, []byte, error) {
	// Keep going right until we reach a leaf
	for !current.isLeaf {
		lastChildIdx := len(current.children) - 1
		child, err := bt.loadNode(tx, current.children[lastChildIdx])
		if err != nil {
			return nil, nil, err
		}
		current = child
	}

	// Return the last key/value in the leaf
	if current.numKeys == 0 {
		return nil, nil, ErrKeyNotFound
	}
	lastIdx := current.numKeys - 1
	return current.keys[lastIdx], current.values[lastIdx], nil
}

// findSuccessor finds the successor key/value in the subtree rooted at node
func (bt *BTree) findSuccessor(tx *Tx, current *node) ([]byte, []byte, error) {
	// Keep going left until we reach a leaf
	for !current.isLeaf {
		child, err := bt.loadNode(tx, current.children[0])
		if err != nil {
			return nil, nil, err
		}
		current = child
	}

	// Return the first key/value in the leaf
	if current.numKeys == 0 {
		return nil, nil, ErrKeyNotFound
	}
	return current.keys[0], current.values[0], nil
}

// insertNonFull inserts into a non-full node with COW.
// It performs COW on nodes before modifying them.
func (bt *BTree) insertNonFull(tx *Tx, n *node, key, value []byte) (*node, error) {
	if n.isLeaf {
		// COW before modifying leaf
		node, err := tx.ensureWritable(n)
		if err != nil {
			return nil, err
		}

		// Binary search for position
		pos := 0
		for pos < int(node.numKeys) && bytes.Compare(key, node.keys[pos]) > 0 {
			pos++
		}

		// Check for update
		if pos < int(node.numKeys) && bytes.Equal(key, node.keys[pos]) {
			// Save old value for rollback
			oldValue := make([]byte, len(node.values[pos]))
			copy(oldValue, node.values[pos])

			node.values[pos] = value
			node.dirty = true

			// Try to serialize after update
			_, err = node.serialize(tx.txnID)
			if err != nil {
				// Rollback: restore old value
				node.values[pos] = oldValue
				return nil, err
			}
			return node, nil
		}

		// Insert new key-value
		node.keys = append(node.keys[:pos], append([][]byte{key}, node.keys[pos:]...)...)
		node.values = append(node.values[:pos], append([][]byte{value}, node.values[pos:]...)...)
		node.numKeys++
		node.dirty = true

		// Try to serialize after insertion
		_, err = node.serialize(tx.txnID)
		if err != nil {
			// Rollback: remove the inserted key/value
			node.keys = append(node.keys[:pos], node.keys[pos+1:]...)
			node.values = append(node.values[:pos], node.values[pos+1:]...)
			node.numKeys--
			return nil, err
		}

		return node, nil
	}

	// Branch n - recursive COW
	// B+ tree: branch nodes are routing only, never update here
	// Always descend to leaf even if key matches a branch key
	i := int(n.numKeys) - 1

	// Find child to insert into
	for i >= 0 && bytes.Compare(key, n.keys[i]) < 0 {
		i--
	}

	i++

	// Load child
	child, err := bt.loadNode(tx, n.children[i])
	if err != nil {
		return nil, err
	}

	// Handle full child with COW-aware split
	if child.isFull() {
		// Split child using COW
		leftChild, rightChild, midKey, midVal, err := bt.splitChild(tx, child)
		if err != nil {
			return nil, err
		}

		// COW parent to insert middle key and update pointers
		n, err = tx.ensureWritable(n)
		if err != nil {
			return nil, err
		}

		// Save old state for rollback
		oldKeys := n.keys
		oldValues := n.values
		oldChildren := n.children
		oldNumKeys := n.numKeys

		// Insert middle key into parent
		n.keys = insertAt(n.keys, i, midKey)
		// Branch nodes don't have values, skip value insertion
		if n.isLeaf {
			n.values = insertAt(n.values, i, midVal)
		}

		// Build new children array atomically to avoid slice sharing issues
		newChildren := make([]pageID, len(n.children)+1)
		copy(newChildren[:i], n.children[:i])     // Before split point
		newChildren[i] = leftChild.pageID         // Left child
		newChildren[i+1] = rightChild.pageID      // Right child
		copy(newChildren[i+2:], n.children[i+1:]) // After split point

		n.children = newChildren

		n.numKeys++
		n.dirty = true

		// serialize parent after modification
		_, err = n.serialize(tx.txnID)
		if err != nil {
			// Rollback: restore old state
			n.keys = oldKeys
			n.values = oldValues
			n.children = oldChildren
			n.numKeys = oldNumKeys
			return nil, err
		}

		// Determine which child to use after split
		// B+ tree: midKey is the minimum of right child, so key >= midKey goes right
		if bytes.Compare(key, midKey) >= 0 {
			i++
			child = rightChild
		} else {
			child = leftChild
		}
	}

	// Store original child pageID to detect COW
	oldChildID := child.pageID

	// Recursive insert (may COW child)
	newChild, err := bt.insertNonFull(tx, child, key, value)
	if err == ErrPageOverflow {
		// Child couldn't fit the key/value - split it and retry
		leftChild, rightChild, midKey, midVal, err := bt.splitChild(tx, child)
		if err != nil {
			return nil, err
		}

		// COW parent to insert middle key and update pointers
		n, err = tx.ensureWritable(n)
		if err != nil {
			return nil, err
		}

		// Save old state for rollback
		oldKeys := n.keys
		oldValues := n.values
		oldChildren := n.children
		oldNumKeys := n.numKeys

		// Insert middle key into parent
		n.keys = insertAt(n.keys, i, midKey)
		// Branch nodes don't have values, skip value insertion
		if n.isLeaf {
			n.values = insertAt(n.values, i, midVal)
		}

		// Build new children array
		newChildren := make([]pageID, len(n.children)+1)
		copy(newChildren[:i], n.children[:i])
		newChildren[i] = leftChild.pageID
		newChildren[i+1] = rightChild.pageID
		copy(newChildren[i+2:], n.children[i+1:])

		n.children = newChildren
		n.numKeys++
		n.dirty = true

		// serialize parent after modification
		_, err = n.serialize(tx.txnID)
		if err != nil {
			// Rollback: restore old state
			n.keys = oldKeys
			n.values = oldValues
			n.children = oldChildren
			n.numKeys = oldNumKeys
			return nil, err
		}

		// Retry insert into correct child
		var insertedChild *node
		if bytes.Compare(key, midKey) >= 0 {
			i++
			insertedChild, err = bt.insertNonFull(tx, rightChild, key, value)
		} else {
			insertedChild, err = bt.insertNonFull(tx, leftChild, key, value)
		}
		if err != nil {
			return nil, err
		}

		// Update parent pointer to the child we inserted into
		n.children[i] = insertedChild.pageID
		n.dirty = true
		_, err = n.serialize(tx.txnID)
		if err != nil {
			return nil, err
		}

		return n, nil
	} else if err != nil {
		return nil, err
	}

	// If child was COWed, update parent pointer
	if newChild.pageID != oldChildID {
		// COW parent to update child pointer
		n, err = tx.ensureWritable(n)
		if err != nil {
			return nil, err
		}

		n.children[i] = newChild.pageID
		n.dirty = true
		// serialize after updating child pointer
		_, err = n.serialize(tx.txnID)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// deleteFromLeaf is the transaction-aware version of deleteFromLeaf.
// It performs COW on the leaf before deleting.
func (bt *BTree) deleteFromLeaf(tx *Tx, node *node, idx int) (*node, error) {
	// COW before modifying leaf
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, err
	}

	// Simply remove the key and value
	node.keys = removeAt(node.keys, idx)
	node.values = removeAt(node.values, idx)
	node.numKeys--
	node.dirty = true

	return node, nil
}

// mergeNodes merges two nodes with COW semantics.
// Transaction-aware merge operation.
func (bt *BTree) mergeNodes(tx *Tx, leftNode, rightNode, parent *node, parentKeyIdx int) (*node, *node, error) {
	// COW left node (will receive merged content)
	leftNode, err := tx.ensureWritable(leftNode)
	if err != nil {
		return nil, nil, err
	}

	// COW parent (will have separator removed)
	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, nil, err
	}

	// B+ tree: Only pull down separator for branch nodes, not leaves
	if !leftNode.isLeaf {
		// Branch merge: pull down separator from parent
		leftNode.keys = append(leftNode.keys, parent.keys[parentKeyIdx])
		// Branch nodes don't have values (parent.values is nil for branch)
	}
	// For leaf merge: separator is routing only, don't pull it down

	// Add all keys from right node to left node
	leftNode.keys = append(leftNode.keys, rightNode.keys...)

	// Add values for leaf nodes only
	if leftNode.isLeaf {
		leftNode.values = append(leftNode.values, rightNode.values...)
	}

	// Copy children pointers for branch nodes
	if !leftNode.isLeaf {
		leftNode.children = append(leftNode.children, rightNode.children...)
	}

	// Update left node's key count
	leftNode.numKeys = uint16(len(leftNode.keys))
	leftNode.dirty = true

	// Remove the separator key from parent
	parent.keys = removeAt(parent.keys, parentKeyIdx)
	// Remove value for leaf parents only (parent is branch, values is nil)
	if parent.isLeaf {
		parent.values = removeAt(parent.values, parentKeyIdx)
	}
	parent.children = removeChildAt(parent.children, parentKeyIdx+1)
	parent.numKeys--
	parent.dirty = true

	// Update parent's child pointer to merged node
	parent.children[parentKeyIdx] = leftNode.pageID

	// Track right node as freed (it's been merged into left)
	tx.addFreed(rightNode.pageID)

	return leftNode, parent, nil
}

// borrowFromLeft borrows a key from left sibling through parent (COW).
func (bt *BTree) borrowFromLeft(tx *Tx, node, leftSibling, parent *node, parentKeyIdx int) (*node, *node, *node, error) {
	// COW all three nodes being modified
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, nil, nil, err
	}

	leftSibling, err = tx.ensureWritable(leftSibling)
	if err != nil {
		return nil, nil, nil, err
	}

	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, nil, nil, err
	}

	if node.isLeaf {
		// B+ tree leaf borrow: move actual data from sibling, update parent separator
		// Move the last key/value from left sibling to beginning of node
		node.keys = append([][]byte{leftSibling.keys[leftSibling.numKeys-1]}, node.keys...)
		node.values = append([][]byte{leftSibling.values[leftSibling.numKeys-1]}, node.values...)

		// Remove from left sibling
		leftSibling.keys = leftSibling.keys[:leftSibling.numKeys-1]
		leftSibling.values = leftSibling.values[:leftSibling.numKeys-1]
		leftSibling.numKeys--

		// Update parent separator to be the first key of right node
		parent.keys[parentKeyIdx] = node.keys[0]

		node.numKeys++
	} else {
		// Branch borrow: traditional B-tree style (pull down separator, push up replacement)
		// Move a key from parent to node (at beginning)
		node.keys = append([][]byte{parent.keys[parentKeyIdx]}, node.keys...)
		// Branch nodes don't have values

		// Move the last key from left sibling to parent
		parent.keys[parentKeyIdx] = leftSibling.keys[leftSibling.numKeys-1]
		// Branch nodes don't have values

		// Move the last child pointer too
		node.children = append([]pageID{leftSibling.children[len(leftSibling.children)-1]}, node.children...)
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]

		// Remove the last key from left sibling
		leftSibling.keys = leftSibling.keys[:leftSibling.numKeys-1]
		// Branch nodes don't have values
		leftSibling.numKeys--

		node.numKeys++
	}

	// Mark all as dirty
	node.dirty = true
	leftSibling.dirty = true
	parent.dirty = true

	// Update parent's children pointers to COWed nodes
	parent.children[parentKeyIdx] = leftSibling.pageID
	parent.children[parentKeyIdx+1] = node.pageID

	return node, leftSibling, parent, nil
}

// borrowFromRight borrows a key from right sibling through parent (COW).
func (bt *BTree) borrowFromRight(tx *Tx, node, rightSibling, parent *node, parentKeyIdx int) (*node, *node, *node, error) {
	// COW all three nodes being modified
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, nil, nil, err
	}

	rightSibling, err = tx.ensureWritable(rightSibling)
	if err != nil {
		return nil, nil, nil, err
	}

	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, nil, nil, err
	}

	if node.isLeaf {
		// B+ tree leaf borrow: move actual data from sibling, update parent separator
		// Move the first key/value from right sibling to end of node
		node.keys = append(node.keys, rightSibling.keys[0])
		node.values = append(node.values, rightSibling.values[0])

		// Remove from right sibling
		rightSibling.keys = rightSibling.keys[1:]
		rightSibling.values = rightSibling.values[1:]
		rightSibling.numKeys--

		// Update parent separator to be the first key of right sibling
		parent.keys[parentKeyIdx] = rightSibling.keys[0]

		node.numKeys++
	} else {
		// Branch borrow: traditional B-tree style (pull down separator, push up replacement)
		// Move a key from parent to node (at end)
		node.keys = append(node.keys, parent.keys[parentKeyIdx])
		// Branch nodes don't have values

		// Move the first key from right sibling to parent
		parent.keys[parentKeyIdx] = rightSibling.keys[0]
		// Branch nodes don't have values

		// Move the first child pointer too
		node.children = append(node.children, rightSibling.children[0])
		rightSibling.children = rightSibling.children[1:]

		// Remove the first key from right sibling
		rightSibling.keys = rightSibling.keys[1:]
		// Branch nodes don't have values
		rightSibling.numKeys--

		node.numKeys++
	}

	// Mark all as dirty
	node.dirty = true
	rightSibling.dirty = true
	parent.dirty = true

	// Update parent's children pointers to COWed nodes
	parent.children[parentKeyIdx] = node.pageID
	parent.children[parentKeyIdx+1] = rightSibling.pageID

	return node, rightSibling, parent, nil
}

// fixUnderflow fixes underflow in child at childIdx with COW semantics.
func (bt *BTree) fixUnderflow(tx *Tx, parent *node, childIdx int, child *node) (*node, *node, error) {
	// Try to borrow from left sibling
	if childIdx > 0 {
		leftSibling, err := bt.loadNode(tx, parent.children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}

		if leftSibling.numKeys > MinKeysPerNode {
			child, leftSibling, parent, err = bt.borrowFromLeft(tx, child, leftSibling, parent, childIdx-1)
			if err != nil {
				return nil, nil, err
			}
			return child, parent, nil
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(parent.children)-1 {
		rightSibling, err := bt.loadNode(tx, parent.children[childIdx+1])
		if err != nil {
			return nil, nil, err
		}

		if rightSibling.numKeys > MinKeysPerNode {
			child, rightSibling, parent, err = bt.borrowFromRight(tx, child, rightSibling, parent, childIdx)
			if err != nil {
				return nil, nil, err
			}
			return child, parent, nil
		}
	}

	// Merge with a sibling
	if childIdx > 0 {
		// Merge with left sibling
		leftSibling, err := bt.loadNode(tx, parent.children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}
		leftSibling, parent, err = bt.mergeNodes(tx, leftSibling, child, parent, childIdx-1)
		if err != nil {
			return nil, nil, err
		}
		return leftSibling, parent, nil
	}

	// Merge with right sibling
	rightSibling, err := bt.loadNode(tx, parent.children[childIdx+1])
	if err != nil {
		return nil, nil, err
	}
	child, parent, err = bt.mergeNodes(tx, child, rightSibling, parent, childIdx)
	if err != nil {
		return nil, nil, err
	}
	return child, parent, nil
}

// deleteFromNonLeaf deletes a key from a non-leaf node with COW semantics.
func (bt *BTree) deleteFromNonLeaf(tx *Tx, node *node, key []byte, idx int) (*node, error) {
	// Try to replace with predecessor from left subtree
	leftChild, err := bt.loadNode(tx, node.children[idx])
	if err != nil {
		return nil, err
	}

	if leftChild.numKeys > MinKeysPerNode {
		// Get predecessor
		predKey, predVal, err := bt.findPredecessor(tx, leftChild)
		if err != nil {
			return nil, err
		}

		// COW node before replacing key
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Replace key with predecessor
		node.keys[idx] = predKey
		node.values[idx] = predVal
		node.dirty = true

		// Delete predecessor from left subtree
		leftChild, err = bt.deleteFromNode(tx, leftChild, predKey)
		if err != nil {
			return nil, err
		}

		// Update child pointer if child was COWed
		node.children[idx] = leftChild.pageID

		return node, nil
	}

	// Try to replace with successor from right subtree
	rightChild, err := bt.loadNode(tx, node.children[idx+1])
	if err != nil {
		return nil, err
	}

	if rightChild.numKeys > MinKeysPerNode {
		// Get successor
		succKey, succVal, err := bt.findSuccessor(tx, rightChild)
		if err != nil {
			return nil, err
		}

		// COW node before replacing key
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Replace key with successor
		node.keys[idx] = succKey
		node.values[idx] = succVal
		node.dirty = true

		// Delete successor from right subtree
		rightChild, err = bt.deleteFromNode(tx, rightChild, succKey)
		if err != nil {
			return nil, err
		}

		// Update child pointer if child was COWed
		node.children[idx+1] = rightChild.pageID

		return node, nil
	}

	// Both children have minimum keys, merge them
	leftChild, parent, err := bt.mergeNodes(tx, leftChild, rightChild, node, idx)
	if err != nil {
		return nil, err
	}

	// Delete from the merged node
	leftChild, err = bt.deleteFromNode(tx, leftChild, key)
	if err != nil {
		return nil, err
	}

	// Update child pointer
	parent.children[idx] = leftChild.pageID

	return parent, nil
}

// deleteFromNode recursively deletes a key from the subtree rooted at node with COW.
// B+ tree: only delete from leaves, branch keys are routing only
func (bt *BTree) deleteFromNode(tx *Tx, node *node, key []byte) (*node, error) {
	// B+ tree: if this is a leaf, active if key exists and delete
	if node.isLeaf {
		idx := node.findKey(key)
		if idx >= 0 {
			return bt.deleteFromLeaf(tx, node, idx)
		}
		return nil, ErrKeyNotFound
	}

	// Branch node: descend to child (never delete from branch)

	// Find child where key might be
	// B+ tree: separator keys are minimums of right children, so key >= separator goes right
	childIdx := 0
	for childIdx < int(node.numKeys) && bytes.Compare(key, node.keys[childIdx]) >= 0 {
		childIdx++
	}

	child, err := bt.loadNode(tx, node.children[childIdx])
	if err != nil {
		return nil, err
	}

	// Check if child will underflow
	shouldCheckUnderflow := (child.numKeys == MinKeysPerNode)

	// Delete from child
	child, err = bt.deleteFromNode(tx, child, key)
	if err != nil {
		return nil, err
	}

	// COW parent to update child pointer
	node, err = tx.ensureWritable(node)
	if err != nil {
		return nil, err
	}

	// Update child pointer (child may have been COWed)
	node.children[childIdx] = child.pageID

	// Handle underflow if necessary
	if shouldCheckUnderflow && child.isUnderflow() && node != tx.root {
		_, node, err = bt.fixUnderflow(tx, node, childIdx, child)
		if err != nil {
			return nil, err
		}
		// Note: fixUnderflow already updates parent's children pointers
	}

	return node, nil
}

// splitChild performs COW on the child being split and allocates the new sibling.
// Returns the COWed child and new sibling, along with the middle key/value.
func (bt *BTree) splitChild(tx *Tx, child *node) (*node, *node, []byte, []byte, error) {
	// COW the child being split
	child, err := tx.ensureWritable(child)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Create new node for right half using tx.allocatePage()
	newNodeID, _, err := tx.allocatePage()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	mid := MaxKeysPerNode / 2

	// For size-based splits with few keys, ensure mid doesn't exceed key count
	// This handles large key/value overflow cases where numKeys < MaxKeysPerNode
	if mid >= len(child.keys)-1 {
		mid = len(child.keys)/2 - 1
		if mid < 0 {
			mid = 0
		}
	}

	// B+ tree semantics:
	// - Leaf nodes: separator is first key of right child (child.keys[mid+1])
	// - Branch nodes: separator is middle key (child.keys[mid])
	var middleKey []byte
	var leftKeyCount, rightKeyCount int
	if child.isLeaf {
		// Leaf split: left keeps [0:mid+1], right gets [mid+1:]
		// Separator is the first key of the right child (minimum of right subtree)
		middleKey = make([]byte, len(child.keys[mid+1]))
		copy(middleKey, child.keys[mid+1])
		leftKeyCount = mid + 1
		rightKeyCount = len(child.keys) - mid - 1
	} else {
		// Branch split: left keeps [0:mid], right gets [mid+1:]
		// Middle key goes to parent (removed from children)
		middleKey = make([]byte, len(child.keys[mid]))
		copy(middleKey, child.keys[mid])
		leftKeyCount = mid
		rightKeyCount = len(child.keys) - mid - 1
	}

	newNode := &node{
		pageID:   newNodeID,
		dirty:    true,
		isLeaf:   child.isLeaf,
		numKeys:  uint16(rightKeyCount),
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]pageID, 0),
	}

	// Copy right half of keys to new node
	// Deep copy to avoid sharing underlying arrays
	for i := mid + 1; i < len(child.keys); i++ {
		keyCopy := make([]byte, len(child.keys[i]))
		copy(keyCopy, child.keys[i])
		newNode.keys = append(newNode.keys, keyCopy)
	}

	// Copy values for leaf nodes only
	if child.isLeaf {
		for i := mid + 1; i < len(child.values); i++ {
			valCopy := make([]byte, len(child.values[i]))
			copy(valCopy, child.values[i])
			newNode.values = append(newNode.values, valCopy)
		}
	}

	// Copy children for branch nodes only
	if !child.isLeaf {
		for i := mid + 1; i < len(child.children); i++ {
			newNode.children = append(newNode.children, child.children[i])
		}
	}

	// Keep left portion in child - keys
	leftKeys := make([][]byte, leftKeyCount)
	copy(leftKeys, child.keys[:leftKeyCount])
	child.keys = leftKeys

	// Keep left portion in child - values (leaf only)
	if child.isLeaf {
		leftValues := make([][]byte, leftKeyCount)
		copy(leftValues, child.values[:leftKeyCount])
		child.values = leftValues
	} else {
		child.values = nil // Branch nodes don't have values
	}

	child.numKeys = uint16(leftKeyCount)
	child.dirty = true

	// Keep left portion in child - children (branch only)
	if !child.isLeaf {
		leftChildren := make([]pageID, mid+1)
		copy(leftChildren, child.children[:mid+1])
		child.children = leftChildren
	}

	// No longer need to serialize here - nodes serialize when written to disk
	// The dirty flag tracks which nodes need to be written

	// Cache newNode in TX-local cache (write transaction)
	// Newly split node goes into tx.pages, not global cache yet
	tx.pages[newNodeID] = newNode

	// B+ tree: Return empty value for parent (branch nodes don't store values)
	// The middle key is only for routing purposes
	return child, newNode, middleKey, []byte{}, nil
}

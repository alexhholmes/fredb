package fredb

import (
	"bytes"
	"errors"
	"fmt"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/storage"
)

// BTree is the main structure
type BTree struct {
	Pager *storage.PageManager
	root  *base.Node
	cache *cache.PageCache // LRU cache for non-root nodes
}

// NewBTree creates a new BTree with the given PageManager
func NewBTree(pager *storage.PageManager) (*BTree, error) {
	meta := pager.GetMeta()

	bt := &BTree{
		Pager: pager,
		cache: cache.NewPageCache(cache.DefaultCacheSize, pager),
	}

	// Check if existing root exists
	if meta.RootPageID != 0 {
		// Load existing root directly (no transaction during initialization)
		rootPage, err := pager.ReadPage(meta.RootPageID)
		if err != nil {
			return nil, err
		}

		root := &base.Node{
			PageID: meta.RootPageID,
			Dirty:  false,
		}

		// Deserialize root
		if err := root.Deserialize(rootPage); err != nil {
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

	// Create root Node
	root := &base.Node{
		PageID:   rootPageID,
		Dirty:    true,
		IsLeaf:   true,
		NumKeys:  0,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: make([]base.PageID, 0),
	}

	bt.root = root

	// Update meta with root Page ID
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

// close flushes any Dirty pages and closes the B-tree
func (bt *BTree) close() error {
	// Flush root if Dirty
	if bt.root != nil && bt.root.Dirty {
		meta := bt.Pager.GetMeta()
		page, err := bt.root.Serialize(meta.TxnID)
		if err != nil {
			return err
		}
		if err := bt.Pager.WritePage(bt.root.PageID, page); err != nil {
			return err
		}
		bt.root.Dirty = false
	}

	// Flush all cached nodes
	if bt.cache != nil {
		if err := bt.cache.FlushDirty(bt.Pager); err != nil {
			return err
		}
	}

	// Clear references
	bt.cache = nil
	bt.root = nil

	// close storage (writes meta Page and frees resources)
	return bt.Pager.Close()
}

// searchNode recursively searches for a key in the tree
// B+ tree: only leaf nodes contain actual data
func (bt *BTree) searchNode(tx *Tx, node *base.Node, key []byte) ([]byte, error) {
	// Find position in current Node
	i := 0
	for i < int(node.NumKeys) && bytes.Compare(key, node.Keys[i]) >= 0 {
		i++
	}
	// After loop: i points to first key > search_key (or NumKeys if all Keys <= search_key)

	// If leaf Node, active if key found
	if node.IsLeaf {
		// In leaf, we need to activate the previous position (since loop went past equal Keys)
		if i > 0 && bytes.Equal(key, node.Keys[i-1]) {
			return node.Values[i-1], nil
		}
		// Not found in leaf
		return nil, ErrKeyNotFound
	}

	// Branch Node: continue descending
	// i is the correct child index (first child with Keys >= search_key)
	child, err := bt.loadNode(tx, node.Children[i])
	if err != nil {
		return nil, err
	}

	return bt.searchNode(tx, child, key)
}

// loadNode loads a Node using hybrid cache: tx.pages → versioned global cache → disk
func (bt *BTree) loadNode(tx *Tx, pageID base.PageID) (*base.Node, error) {
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
	node, found := bt.cache.GetOrLoad(pageID, tx.txnID, func() (*base.Node, uint64, error) {
		// Load from disk (called by at most one thread per PageID)
		page, err := bt.Pager.ReadPage(pageID)
		if err != nil {
			return nil, 0, err
		}

		// Create Node and Deserialize
		node := &base.Node{
			PageID: pageID,
			Dirty:  false,
		}

		// Try to Deserialize - if Page is empty (new Page), header.NumKeys will be 0
		header := page.Header()
		if header.NumKeys > 0 {
			if err := node.Deserialize(page); err != nil {
				return nil, 0, err
			}
		} else {
			// New/empty Page - initialize as empty leaf
			node.PageID = pageID
			node.IsLeaf = true
			node.NumKeys = 0
			node.Keys = make([][]byte, 0)
			node.Values = make([][]byte, 0)
			node.Children = make([]base.PageID, 0)
		}

		// Cycle detection: active if deserialized Node references itself
		if !node.IsLeaf {
			for _, childID := range node.Children {
				if childID == pageID {
					return nil, 0, ErrCorruption // Self-reference detected
				}
			}
		}

		// Return Node and its TxnID from disk header
		return node, header.TxnID, nil
	})

	if !found {
		// Version not visible or load failed
		return nil, ErrKeyNotFound
	}

	// Cycle detection on cached Node (fast path)
	if !node.IsLeaf {
		for _, childID := range node.Children {
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
func removeChildAt(slice []base.PageID, index int) []base.PageID {
	return append(slice[:index], slice[index+1:]...)
}

// findPredecessor finds the predecessor key/value in the subtree rooted at Node
func (bt *BTree) findPredecessor(tx *Tx, current *base.Node) ([]byte, []byte, error) {
	// Keep going right until we reach a leaf
	for !current.IsLeaf {
		lastChildIdx := len(current.Children) - 1
		child, err := bt.loadNode(tx, current.Children[lastChildIdx])
		if err != nil {
			return nil, nil, err
		}
		current = child
	}

	// Return the last key/value in the leaf
	if current.NumKeys == 0 {
		return nil, nil, ErrKeyNotFound
	}
	lastIdx := current.NumKeys - 1
	return current.Keys[lastIdx], current.Values[lastIdx], nil
}

// findSuccessor finds the successor key/value in the subtree rooted at Node
func (bt *BTree) findSuccessor(tx *Tx, current *base.Node) ([]byte, []byte, error) {
	// Keep going left until we reach a leaf
	for !current.IsLeaf {
		child, err := bt.loadNode(tx, current.Children[0])
		if err != nil {
			return nil, nil, err
		}
		current = child
	}

	// Return the first key/value in the leaf
	if current.NumKeys == 0 {
		return nil, nil, ErrKeyNotFound
	}
	return current.Keys[0], current.Values[0], nil
}

// insertNonFull inserts into a non-full Node with COW.
// It performs COW on nodes before modifying them.
func (bt *BTree) insertNonFull(tx *Tx, n *base.Node, key, value []byte) (*base.Node, error) {
	if n.IsLeaf {
		// COW before modifying leaf
		node, err := tx.ensureWritable(n)
		if err != nil {
			return nil, err
		}

		// Binary search for position
		pos := 0
		for pos < int(node.NumKeys) && bytes.Compare(key, node.Keys[pos]) > 0 {
			pos++
		}

		// Check for update
		if pos < int(node.NumKeys) && bytes.Equal(key, node.Keys[pos]) {
			// Save old value for rollback
			oldValue := make([]byte, len(node.Values[pos]))
			copy(oldValue, node.Values[pos])

			node.Values[pos] = value
			node.Dirty = true

			// Try to Serialize after update
			_, err = node.Serialize(tx.txnID)
			if err != nil {
				// Rollback: restore old value
				node.Values[pos] = oldValue
				return nil, err
			}
			return node, nil
		}

		// Insert new key-value
		node.Keys = append(node.Keys[:pos], append([][]byte{key}, node.Keys[pos:]...)...)
		node.Values = append(node.Values[:pos], append([][]byte{value}, node.Values[pos:]...)...)
		node.NumKeys++
		node.Dirty = true

		// Try to Serialize after insertion
		_, err = node.Serialize(tx.txnID)
		if err != nil {
			// Rollback: remove the inserted key/value
			node.Keys = append(node.Keys[:pos], node.Keys[pos+1:]...)
			node.Values = append(node.Values[:pos], node.Values[pos+1:]...)
			node.NumKeys--
			return nil, err
		}

		return node, nil
	}

	// Branch n - recursive COW
	// B+ tree: branch nodes are routing only, never update here
	// Always descend to leaf even if key matches a branch key
	i := int(n.NumKeys) - 1

	// Find child to insert into
	for i >= 0 && bytes.Compare(key, n.Keys[i]) < 0 {
		i--
	}

	i++

	// Load child
	child, err := bt.loadNode(tx, n.Children[i])
	if err != nil {
		return nil, err
	}

	// Handle full child with COW-aware split
	if child.IsFull() {
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
		oldKeys := n.Keys
		oldValues := n.Values
		oldChildren := n.Children
		oldNumKeys := n.NumKeys

		// Insert middle key into parent
		n.Keys = insertAt(n.Keys, i, midKey)
		// Branch nodes don't have Values, skip value insertion
		if n.IsLeaf {
			n.Values = insertAt(n.Values, i, midVal)
		}

		// Build new Children array atomically to avoid slice sharing issues
		newChildren := make([]base.PageID, len(n.Children)+1)
		copy(newChildren[:i], n.Children[:i])     // Before split point
		newChildren[i] = leftChild.PageID         // Left child
		newChildren[i+1] = rightChild.PageID      // Right child
		copy(newChildren[i+2:], n.Children[i+1:]) // After split point

		n.Children = newChildren

		n.NumKeys++
		n.Dirty = true

		// Serialize parent after modification
		_, err = n.Serialize(tx.txnID)
		if err != nil {
			// Rollback: restore old state
			n.Keys = oldKeys
			n.Values = oldValues
			n.Children = oldChildren
			n.NumKeys = oldNumKeys
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

	// Store original child PageID to detect COW
	oldChildID := child.PageID

	// Recursive insert (may COW child)
	newChild, err := bt.insertNonFull(tx, child, key, value)
	if errors.Is(err, ErrPageOverflow) {
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
		oldKeys := n.Keys
		oldValues := n.Values
		oldChildren := n.Children
		oldNumKeys := n.NumKeys

		// Insert middle key into parent
		n.Keys = insertAt(n.Keys, i, midKey)
		// Branch nodes don't have Values, skip value insertion
		if n.IsLeaf {
			n.Values = insertAt(n.Values, i, midVal)
		}

		// Build new Children array
		newChildren := make([]base.PageID, len(n.Children)+1)
		copy(newChildren[:i], n.Children[:i])
		newChildren[i] = leftChild.PageID
		newChildren[i+1] = rightChild.PageID
		copy(newChildren[i+2:], n.Children[i+1:])

		n.Children = newChildren
		n.NumKeys++
		n.Dirty = true

		// Serialize parent after modification
		_, err = n.Serialize(tx.txnID)
		if err != nil {
			// Rollback: restore old state
			n.Keys = oldKeys
			n.Values = oldValues
			n.Children = oldChildren
			n.NumKeys = oldNumKeys
			return nil, err
		}

		// Retry insert into correct child
		var insertedChild *base.Node
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
		n.Children[i] = insertedChild.PageID
		n.Dirty = true
		_, err = n.Serialize(tx.txnID)
		if err != nil {
			return nil, err
		}

		return n, nil
	} else if err != nil {
		return nil, err
	}

	// If child was COWed, update parent pointer
	if newChild.PageID != oldChildID {
		// COW parent to update child pointer
		n, err = tx.ensureWritable(n)
		if err != nil {
			return nil, err
		}

		n.Children[i] = newChild.PageID
		n.Dirty = true
		// Serialize after updating child pointer
		_, err = n.Serialize(tx.txnID)
		if err != nil {
			return nil, err
		}
	}

	return n, nil
}

// deleteFromLeaf is the transaction-aware version of deleteFromLeaf.
// It performs COW on the leaf before deleting.
func (bt *BTree) deleteFromLeaf(tx *Tx, node *base.Node, idx int) (*base.Node, error) {
	// COW before modifying leaf
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, err
	}

	// Simply remove the key and value
	node.Keys = removeAt(node.Keys, idx)
	node.Values = removeAt(node.Values, idx)
	node.NumKeys--
	node.Dirty = true

	return node, nil
}

// mergeNodes merges two nodes with COW semantics.
// Transaction-aware merge operation.
func (bt *BTree) mergeNodes(tx *Tx, leftNode, rightNode, parent *base.Node, parentKeyIdx int) (*base.Node, *base.Node, error) {
	// COW left Node (will receive merged content)
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
	if !leftNode.IsLeaf {
		// Branch merge: pull down separator from parent
		leftNode.Keys = append(leftNode.Keys, parent.Keys[parentKeyIdx])
		// Branch nodes don't have Values (parent.Values is nil for branch)
	}
	// For leaf merge: separator is routing only, don't pull it down

	// Add all Keys from right Node to left Node
	leftNode.Keys = append(leftNode.Keys, rightNode.Keys...)

	// Add Values for leaf nodes only
	if leftNode.IsLeaf {
		leftNode.Values = append(leftNode.Values, rightNode.Values...)
	}

	// Copy Children pointers for branch nodes
	if !leftNode.IsLeaf {
		leftNode.Children = append(leftNode.Children, rightNode.Children...)
	}

	// Update left Node's key count
	leftNode.NumKeys = uint16(len(leftNode.Keys))
	leftNode.Dirty = true

	// Remove the separator key from parent
	parent.Keys = removeAt(parent.Keys, parentKeyIdx)
	// Remove value for leaf parents only (parent is branch, Values is nil)
	if parent.IsLeaf {
		parent.Values = removeAt(parent.Values, parentKeyIdx)
	}
	parent.Children = removeChildAt(parent.Children, parentKeyIdx+1)
	parent.NumKeys--
	parent.Dirty = true

	// Update parent's child pointer to merged Node
	parent.Children[parentKeyIdx] = leftNode.PageID

	// Track right Node as freed (it's been merged into left)
	tx.addFreed(rightNode.PageID)

	return leftNode, parent, nil
}

// borrowFromLeft borrows a key from left sibling through parent (COW).
func (bt *BTree) borrowFromLeft(tx *Tx, node, leftSibling, parent *base.Node, parentKeyIdx int) (*base.Node, *base.Node, *base.Node, error) {
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

	if node.IsLeaf {
		// B+ tree leaf borrow: move actual data from sibling, update parent separator
		// Move the last key/value from left sibling to beginning of Node
		node.Keys = append([][]byte{leftSibling.Keys[leftSibling.NumKeys-1]}, node.Keys...)
		node.Values = append([][]byte{leftSibling.Values[leftSibling.NumKeys-1]}, node.Values...)

		// Remove from left sibling
		leftSibling.Keys = leftSibling.Keys[:leftSibling.NumKeys-1]
		leftSibling.Values = leftSibling.Values[:leftSibling.NumKeys-1]
		leftSibling.NumKeys--

		// Update parent separator to be the first key of right Node
		parent.Keys[parentKeyIdx] = node.Keys[0]

		node.NumKeys++
	} else {
		// Branch borrow: traditional B-tree style (pull down separator, push up replacement)
		// Move a key from parent to Node (at beginning)
		node.Keys = append([][]byte{parent.Keys[parentKeyIdx]}, node.Keys...)
		// Branch nodes don't have Values

		// Move the last key from left sibling to parent
		parent.Keys[parentKeyIdx] = leftSibling.Keys[leftSibling.NumKeys-1]
		// Branch nodes don't have Values

		// Move the last child pointer too
		node.Children = append([]base.PageID{leftSibling.Children[len(leftSibling.Children)-1]}, node.Children...)
		leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]

		// Remove the last key from left sibling
		leftSibling.Keys = leftSibling.Keys[:leftSibling.NumKeys-1]
		// Branch nodes don't have Values
		leftSibling.NumKeys--

		node.NumKeys++
	}

	// Mark all as Dirty
	node.Dirty = true
	leftSibling.Dirty = true
	parent.Dirty = true

	// Update parent's Children pointers to COWed nodes
	parent.Children[parentKeyIdx] = leftSibling.PageID
	parent.Children[parentKeyIdx+1] = node.PageID

	return node, leftSibling, parent, nil
}

// borrowFromRight borrows a key from right sibling through parent (COW).
func (bt *BTree) borrowFromRight(tx *Tx, node, rightSibling, parent *base.Node, parentKeyIdx int) (*base.Node, *base.Node, *base.Node, error) {
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

	if node.IsLeaf {
		// B+ tree leaf borrow: move actual data from sibling, update parent separator
		// Move the first key/value from right sibling to end of Node
		node.Keys = append(node.Keys, rightSibling.Keys[0])
		node.Values = append(node.Values, rightSibling.Values[0])

		// Remove from right sibling
		rightSibling.Keys = rightSibling.Keys[1:]
		rightSibling.Values = rightSibling.Values[1:]
		rightSibling.NumKeys--

		// Update parent separator to be the first key of right sibling
		parent.Keys[parentKeyIdx] = rightSibling.Keys[0]

		node.NumKeys++
	} else {
		// Branch borrow: traditional B-tree style (pull down separator, push up replacement)
		// Move a key from parent to Node (at end)
		node.Keys = append(node.Keys, parent.Keys[parentKeyIdx])
		// Branch nodes don't have Values

		// Move the first key from right sibling to parent
		parent.Keys[parentKeyIdx] = rightSibling.Keys[0]
		// Branch nodes don't have Values

		// Move the first child pointer too
		node.Children = append(node.Children, rightSibling.Children[0])
		rightSibling.Children = rightSibling.Children[1:]

		// Remove the first key from right sibling
		rightSibling.Keys = rightSibling.Keys[1:]
		// Branch nodes don't have Values
		rightSibling.NumKeys--

		node.NumKeys++
	}

	// Mark all as Dirty
	node.Dirty = true
	rightSibling.Dirty = true
	parent.Dirty = true

	// Update parent's Children pointers to COWed nodes
	parent.Children[parentKeyIdx] = node.PageID
	parent.Children[parentKeyIdx+1] = rightSibling.PageID

	return node, rightSibling, parent, nil
}

// fixUnderflow fixes underflow in child at childIdx with COW semantics.
func (bt *BTree) fixUnderflow(tx *Tx, parent *base.Node, childIdx int, child *base.Node) (*base.Node, *base.Node, error) {
	// Try to borrow from left sibling
	if childIdx > 0 {
		leftSibling, err := bt.loadNode(tx, parent.Children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}

		if leftSibling.NumKeys > base.MinKeysPerNode {
			child, leftSibling, parent, err = bt.borrowFromLeft(tx, child, leftSibling, parent, childIdx-1)
			if err != nil {
				return nil, nil, err
			}
			return child, parent, nil
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(parent.Children)-1 {
		rightSibling, err := bt.loadNode(tx, parent.Children[childIdx+1])
		if err != nil {
			return nil, nil, err
		}

		if rightSibling.NumKeys > base.MinKeysPerNode {
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
		leftSibling, err := bt.loadNode(tx, parent.Children[childIdx-1])
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
	rightSibling, err := bt.loadNode(tx, parent.Children[childIdx+1])
	if err != nil {
		return nil, nil, err
	}
	child, parent, err = bt.mergeNodes(tx, child, rightSibling, parent, childIdx)
	if err != nil {
		return nil, nil, err
	}
	return child, parent, nil
}

// deleteFromNonLeaf deletes a key from a non-leaf Node with COW semantics.
func (bt *BTree) deleteFromNonLeaf(tx *Tx, node *base.Node, key []byte, idx int) (*base.Node, error) {
	// Try to replace with predecessor from left subtree
	leftChild, err := bt.loadNode(tx, node.Children[idx])
	if err != nil {
		return nil, err
	}

	if leftChild.NumKeys > base.MinKeysPerNode {
		// get predecessor
		predKey, predVal, err := bt.findPredecessor(tx, leftChild)
		if err != nil {
			return nil, err
		}

		// COW Node before replacing key
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Replace key with predecessor
		node.Keys[idx] = predKey
		node.Values[idx] = predVal
		node.Dirty = true

		// Delete predecessor from left subtree
		leftChild, err = bt.deleteFromNode(tx, leftChild, predKey)
		if err != nil {
			return nil, err
		}

		// Update child pointer if child was COWed
		node.Children[idx] = leftChild.PageID

		return node, nil
	}

	// Try to replace with successor from right subtree
	rightChild, err := bt.loadNode(tx, node.Children[idx+1])
	if err != nil {
		return nil, err
	}

	if rightChild.NumKeys > base.MinKeysPerNode {
		// get successor
		succKey, succVal, err := bt.findSuccessor(tx, rightChild)
		if err != nil {
			return nil, err
		}

		// COW Node before replacing key
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Replace key with successor
		node.Keys[idx] = succKey
		node.Values[idx] = succVal
		node.Dirty = true

		// Delete successor from right subtree
		rightChild, err = bt.deleteFromNode(tx, rightChild, succKey)
		if err != nil {
			return nil, err
		}

		// Update child pointer if child was COWed
		node.Children[idx+1] = rightChild.PageID

		return node, nil
	}

	// Both Children have minimum Keys, merge them
	leftChild, parent, err := bt.mergeNodes(tx, leftChild, rightChild, node, idx)
	if err != nil {
		return nil, err
	}

	// Delete from the merged Node
	leftChild, err = bt.deleteFromNode(tx, leftChild, key)
	if err != nil {
		return nil, err
	}

	// Update child pointer
	parent.Children[idx] = leftChild.PageID

	return parent, nil
}

// deleteFromNode recursively deletes a key from the subtree rooted at Node with COW.
// B+ tree: only delete from leaves, branch Keys are routing only
func (bt *BTree) deleteFromNode(tx *Tx, node *base.Node, key []byte) (*base.Node, error) {
	// B+ tree: if this is a leaf, active if key exists and delete
	if node.IsLeaf {
		idx := node.FindKey(key)
		if idx >= 0 {
			return bt.deleteFromLeaf(tx, node, idx)
		}
		return nil, ErrKeyNotFound
	}

	// Branch Node: descend to child (never delete from branch)

	// Find child where key might be
	// B+ tree: separator Keys are minimums of right Children, so key >= separator goes right
	childIdx := 0
	for childIdx < int(node.NumKeys) && bytes.Compare(key, node.Keys[childIdx]) >= 0 {
		childIdx++
	}

	child, err := bt.loadNode(tx, node.Children[childIdx])
	if err != nil {
		return nil, err
	}

	// Check if child will underflow
	shouldCheckUnderflow := (child.NumKeys == base.MinKeysPerNode)

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
	node.Children[childIdx] = child.PageID

	// Handle underflow if necessary
	if shouldCheckUnderflow && child.IsUnderflow() && node != tx.root {
		_, node, err = bt.fixUnderflow(tx, node, childIdx, child)
		if err != nil {
			return nil, err
		}
		// Note: fixUnderflow already updates parent's Children pointers
	}

	return node, nil
}

// splitChild performs COW on the child being split and allocates the new sibling.
// Returns the COWed child and new sibling, along with the middle key/value.
func (bt *BTree) splitChild(tx *Tx, child *base.Node) (*base.Node, *base.Node, []byte, []byte, error) {
	// COW the child being split
	child, err := tx.ensureWritable(child)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Create new Node for right half using tx.allocatePage()
	newNodeID, _, err := tx.allocatePage()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	mid := base.MaxKeysPerNode / 2

	// For size-based splits with few Keys, ensure mid doesn't exceed key count
	// This handles large key/value overflow cases where NumKeys < MaxKeysPerNode
	if mid >= len(child.Keys)-1 {
		mid = len(child.Keys)/2 - 1
		if mid < 0 {
			mid = 0
		}
	}

	// B+ tree semantics:
	// - Leaf nodes: separator is first key of right child (child.Keys[mid+1])
	// - Branch nodes: separator is middle key (child.Keys[mid])
	var middleKey []byte
	var leftKeyCount, rightKeyCount int
	if child.IsLeaf {
		// Leaf split: left keeps [0:mid+1], right gets [mid+1:]
		// Separator is the first key of the right child (minimum of right subtree)
		middleKey = make([]byte, len(child.Keys[mid+1]))
		copy(middleKey, child.Keys[mid+1])
		leftKeyCount = mid + 1
		rightKeyCount = len(child.Keys) - mid - 1
	} else {
		// Branch split: left keeps [0:mid], right gets [mid+1:]
		// Middle key goes to parent (removed from Children)
		middleKey = make([]byte, len(child.Keys[mid]))
		copy(middleKey, child.Keys[mid])
		leftKeyCount = mid
		rightKeyCount = len(child.Keys) - mid - 1
	}

	newNode := &base.Node{
		PageID:   newNodeID,
		Dirty:    true,
		IsLeaf:   child.IsLeaf,
		NumKeys:  uint16(rightKeyCount),
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: make([]base.PageID, 0),
	}

	// Copy right half of Keys to new Node
	// Deep copy to avoid sharing underlying arrays
	for i := mid + 1; i < len(child.Keys); i++ {
		keyCopy := make([]byte, len(child.Keys[i]))
		copy(keyCopy, child.Keys[i])
		newNode.Keys = append(newNode.Keys, keyCopy)
	}

	// Copy Values for leaf nodes only
	if child.IsLeaf {
		for i := mid + 1; i < len(child.Values); i++ {
			valCopy := make([]byte, len(child.Values[i]))
			copy(valCopy, child.Values[i])
			newNode.Values = append(newNode.Values, valCopy)
		}
	}

	// Copy Children for branch nodes only
	if !child.IsLeaf {
		for i := mid + 1; i < len(child.Children); i++ {
			newNode.Children = append(newNode.Children, child.Children[i])
		}
	}

	// Keep left portion in child - Keys
	leftKeys := make([][]byte, leftKeyCount)
	copy(leftKeys, child.Keys[:leftKeyCount])
	child.Keys = leftKeys

	// Keep left portion in child - Values (leaf only)
	if child.IsLeaf {
		leftValues := make([][]byte, leftKeyCount)
		copy(leftValues, child.Values[:leftKeyCount])
		child.Values = leftValues
	} else {
		child.Values = nil // Branch nodes don't have Values
	}

	child.NumKeys = uint16(leftKeyCount)
	child.Dirty = true

	// Keep left portion in child - Children (branch only)
	if !child.IsLeaf {
		leftChildren := make([]base.PageID, mid+1)
		copy(leftChildren, child.Children[:mid+1])
		child.Children = leftChildren
	}

	// No longer need to Serialize here - nodes Serialize when written to disk
	// The Dirty flag tracks which nodes need to be written

	// Cache newNode in TX-local cache (write transaction)
	// Newly split Node goes into tx.pages, not global cache yet
	tx.pages[newNodeID] = newNode

	// B+ tree: Return empty value for parent (branch nodes don't store Values)
	// The middle key is only for routing purposes
	return child, newNode, middleKey, []byte{}, nil
}

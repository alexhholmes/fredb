package pkg

import (
	"bytes"
	"fmt"
)

const (
	// MaxKeysPerNode must be small enough that a full node can serialize to PageSize
	MaxKeysPerNode = 64
	MinKeysPerNode = MaxKeysPerNode / 4 // Minimum keys for non-root nodes

	// maxSeparatorSize limits separator keys in branch nodes
	// Separator only needs to be long enough to distinguish the split
	maxSeparatorSize = 16
)

// Node wraps a Page with BTree operations
type Node struct {
	page   *Page
	pageID PageID
	dirty  bool

	// Cached decoded values
	isLeaf   bool
	numKeys  uint16
	keys     [][]byte
	values   [][]byte
	children []PageID
}

// clone creates a deep copy of this node for copy-on-write
// The clone is marked dirty and does not have a page allocated yet
func (n *Node) clone() *Node {
	cloned := &Node{
		page:    nil,
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

	// Deep copy values
	cloned.values = make([][]byte, len(n.values))
	for i, val := range n.values {
		cloned.values[i] = make([]byte, len(val))
		copy(cloned.values[i], val)
	}

	// Deep copy children (for branch nodes)
	if !n.isLeaf && len(n.children) > 0 {
		cloned.children = make([]PageID, len(n.children))
		copy(cloned.children, n.children)
	}

	return cloned
}

// BTree is the main structure
type BTree struct {
	pager PageManager
	root  *Node
	cache *PageCache // LRU cache for non-root nodes
}

// NewBTree creates a new BTree with the given PageManager
func NewBTree(pager PageManager) (*BTree, error) {
	meta := pager.GetMeta()

	bt := &BTree{
		pager: pager,
		cache: NewPageCache(DefaultCacheSize),
	}

	// Check if existing root exists
	if meta.RootPageID != 0 {
		// Load existing root directly (no transaction during initialization)
		rootPage, err := pager.ReadPage(meta.RootPageID)
		if err != nil {
			return nil, err
		}

		root := &Node{
			page:   rootPage,
			pageID: meta.RootPageID,
			dirty:  false,
		}

		// Deserialize root
		if err := root.deserialize(); err != nil {
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

	// Read the allocated page
	rootPage, err := pager.ReadPage(rootPageID)
	if err != nil {
		return nil, err
	}

	// Create root node
	root := &Node{
		page:     rootPage,
		pageID:   rootPageID,
		dirty:    true,
		isLeaf:   true,
		numKeys:  0,
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]PageID, 0),
	}

	bt.root = root

	// Update meta with root page ID
	meta.RootPageID = rootPageID
	if err := pager.PutMeta(meta); err != nil {
		return nil, err
	}

	return bt, nil
}

// NewCursor creates a new cursor for this B-tree
// Cursor starts in invalid state - call Seek() to position it
func (bt *BTree) NewCursor(tx *Tx) *Cursor {
	return &Cursor{
		btree: bt,
		tx:    tx,
		valid: false,
	}
}

// Close flushes any dirty pages and closes the B-tree
func (bt *BTree) Close() error {
	// Flush root if dirty
	if bt.root != nil && bt.root.dirty {
		meta := bt.pager.GetMeta()
		if err := bt.root.serialize(meta.TxnID); err != nil {
			return err
		}
		if err := bt.pager.WritePage(bt.root.pageID, bt.root.page); err != nil {
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

	// Close pager (writes meta page and frees resources)
	return bt.pager.Close()
}

// searchNode recursively searches for a key in the tree
// B+ tree: only leaf nodes contain actual data
func (bt *BTree) searchNode(tx *Tx, node *Node, key []byte) ([]byte, error) {
	// Find position in current node
	i := 0
	for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) >= 0 {
		i++
	}
	// After loop: i points to first key > search_key (or numKeys if all keys <= search_key)

	// If leaf node, check if key found
	if node.isLeaf {
		// In leaf, we need to check the previous position (since loop went past equal keys)
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
func (bt *BTree) loadNode(tx *Tx, pageID PageID) (*Node, error) {
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
	node, found := bt.cache.GetOrLoad(pageID, tx.txnID, func() (*Node, uint64, error) {
		// Load from disk (called by at most one thread per PageID)
		page, err := bt.pager.ReadPage(pageID)
		if err != nil {
			return nil, 0, err
		}

		// Create node and deserialize
		node := &Node{
			page:   page,
			pageID: pageID,
			dirty:  false,
		}

		// Try to deserialize - if page is empty (new page), header.NumKeys will be 0
		header := page.Header()
		if header.NumKeys > 0 {
			if err := node.deserialize(); err != nil {
				return nil, 0, err
			}
		} else {
			// New/empty page - initialize as empty leaf
			node.isLeaf = true
			node.numKeys = 0
			node.keys = make([][]byte, 0)
			node.values = make([][]byte, 0)
			node.children = make([]PageID, 0)
		}

		// Cycle detection: check if deserialized node references itself
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

// isFull checks if a node is full
// For branch nodes with truncated separators, check size-based capacity
// For leaf nodes, check key count
func (n *Node) isFull() bool {
	if n.isLeaf {
		// Leaf nodes: use key count check
		// Overflow is detected during serialize with try-rollback
		return int(n.numKeys) >= MaxKeysPerNode
	}

	// Branch nodes: with 16-byte truncated separators, capacity is limited by size
	// Conservative check: with 16-byte keys, we can fit ~126 separators
	// PageHeaderSize (40) + FirstChild (8) + N * (BranchElementSize (16) + 16) <= 4096
	// 48 + N * 32 <= 4096
	// N <= 126.5
	const maxBranchKeysWithTruncation = 126
	return int(n.numKeys) >= maxBranchKeysWithTruncation
}

// serializedSize calculates the size of the serialized node
func (n *Node) serializedSize() int {
	size := PageHeaderSize

	if n.isLeaf {
		size += int(n.numKeys) * LeafElementSize
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) + len(n.values[i])
		}
	} else {
		// B+ tree: branch nodes only store keys (no values)
		size += int(n.numKeys) * BranchElementSize
		size += 8 // children[0]
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) // Only keys, no values
		}
	}

	return size
}

// serialize encodes the node data into page.data
func (n *Node) serialize(txnID uint64) error {
	// Check size
	if n.serializedSize() > PageSize {
		return ErrPageOverflow
	}

	// Clear page data
	for i := range n.page.data {
		n.page.data[i] = 0
	}

	// Write header
	header := &PageHeader{
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
	n.page.WriteHeader(header)

	if n.isLeaf {
		// Serialize leaf node - pack from end backward
		dataOffset := uint16(PageSize)
		// Process in reverse order to pack from end
		for i := int(n.numKeys) - 1; i >= 0; i-- {
			key := n.keys[i]
			value := n.values[i]

			// Write value first (at end)
			dataOffset -= uint16(len(value))
			copy(n.page.data[dataOffset:], value)
			valueOffset := dataOffset

			// Write key before value
			dataOffset -= uint16(len(key))
			copy(n.page.data[dataOffset:], key)
			keyOffset := dataOffset

			elem := &LeafElement{
				KeyOffset:   keyOffset,
				KeySize:     uint16(len(key)),
				ValueOffset: valueOffset,
				ValueSize:   uint16(len(value)),
			}
			n.page.WriteLeafElement(i, elem)
		}
	} else {
		// Serialize branch node (B+ tree: only keys, no values)
		// Write children[0] at fixed location (last 8 bytes)
		if len(n.children) > 0 {
			n.page.WriteBranchFirstChild(n.children[0])
		}

		// Pack keys from end backward (reserve last 8 bytes for children[0])
		dataOffset := uint16(PageSize - 8)
		// Process in reverse order to pack from end
		for i := int(n.numKeys) - 1; i >= 0; i-- {
			key := n.keys[i]

			// Write key
			dataOffset -= uint16(len(key))
			copy(n.page.data[dataOffset:], key)

			elem := &BranchElement{
				KeyOffset: dataOffset,
				KeySize:   uint16(len(key)),
				Reserved:  0, // No values in B+ tree branches
				ChildID:   n.children[i+1],
			}
			n.page.WriteBranchElement(i, elem)
		}
	}

	return nil
}

// deserialize decodes the page data into node fields
func (n *Node) deserialize() error {
	header := n.page.Header()
	n.pageID = header.PageID
	n.numKeys = header.NumKeys
	n.isLeaf = (header.Flags & LeafPageFlag) != 0

	if n.isLeaf {
		// Deserialize leaf node
		n.keys = make([][]byte, n.numKeys)
		n.values = make([][]byte, n.numKeys)
		n.children = nil

		elements := n.page.LeafElements()
		for i := 0; i < int(n.numKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := n.page.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy value
			valueData, err := n.page.GetValue(elem.ValueOffset, elem.ValueSize)
			if err != nil {
				return err
			}
			n.values[i] = make([]byte, len(valueData))
			copy(n.values[i], valueData)
		}
	} else {
		// Deserialize branch node (B+ tree: only keys, no values)
		n.keys = make([][]byte, n.numKeys)
		n.values = make([][]byte, n.numKeys) // Empty values (B+ tree branches don't store values)
		n.children = make([]PageID, n.numKeys+1)

		// Read children[0]
		n.children[0] = n.page.ReadBranchFirstChild()

		elements := n.page.BranchElements()
		for i := 0; i < int(n.numKeys); i++ {
			elem := elements[i]

			// Copy key
			keyData, err := n.page.GetKey(elem.KeyOffset, elem.KeySize)
			if err != nil {
				return err
			}
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy child pointer
			n.children[i+1] = elem.ChildID

			// B+ tree: Branch nodes don't store values, set to empty
			n.values[i] = []byte{}
		}
	}

	return nil
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
func removeChildAt(slice []PageID, index int) []PageID {
	return append(slice[:index], slice[index+1:]...)
}

// truncateSeparatorKey truncates a separator key to reduce branch node size.
// Separator keys in branch nodes only need to be long enough to route correctly.
// It ensures the truncated key can still distinguish between separator and leftMax.
// leftMax is the maximum key in the left child (can be nil if no left context).
func truncateSeparatorKey(separator []byte, leftMax []byte) []byte {
	if len(separator) == 0 {
		return separator
	}

	// If no left context, use simple truncation
	if leftMax == nil || len(leftMax) == 0 {
		if len(separator) > maxSeparatorSize {
			truncated := make([]byte, maxSeparatorSize)
			copy(truncated, separator)
			return truncated
		}
		return separator
	}

	// Find first differing byte position
	i := 0
	for i < len(separator) && i < len(leftMax) && separator[i] == leftMax[i] {
		i++
	}

	// Need i+1 bytes to include the distinguishing byte
	// This ensures separator > leftMax in lexicographic order
	minNeeded := i + 1
	if minNeeded > len(separator) {
		// leftMax is a prefix of separator, need full separator
		return separator
	}

	// Take minimum of (bytes needed, maxSeparatorSize, full length)
	// Correctness over space: if we need 18 bytes to distinguish, return 18 bytes
	return separator[:minNeeded]
}

// findKey returns the index of key in node, or -1 if not found
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

// findPredecessor finds the predecessor key/value in the subtree rooted at node
func (bt *BTree) findPredecessor(tx *Tx, node *Node) ([]byte, []byte, error) {
	// Keep going right until we reach a leaf
	for !node.isLeaf {
		lastChildIdx := len(node.children) - 1
		child, err := bt.loadNode(tx, node.children[lastChildIdx])
		if err != nil {
			return nil, nil, err
		}
		node = child
	}

	// Return the last key/value in the leaf
	if node.numKeys == 0 {
		return nil, nil, ErrKeyNotFound
	}
	lastIdx := node.numKeys - 1
	return node.keys[lastIdx], node.values[lastIdx], nil
}

// findSuccessor finds the successor key/value in the subtree rooted at node
func (bt *BTree) findSuccessor(tx *Tx, node *Node) ([]byte, []byte, error) {
	// Keep going left until we reach a leaf
	for !node.isLeaf {
		child, err := bt.loadNode(tx, node.children[0])
		if err != nil {
			return nil, nil, err
		}
		node = child
	}

	// Return the first key/value in the leaf
	if node.numKeys == 0 {
		return nil, nil, ErrKeyNotFound
	}
	return node.keys[0], node.values[0], nil
}

// isUnderflow checks if node has too few keys (doesn't apply to root)
func (n *Node) isUnderflow() bool {
	return int(n.numKeys) < MinKeysPerNode
}

// insertNonFull inserts into a non-full node with COW.
// It performs COW on nodes before modifying them.
func (bt *BTree) insertNonFull(tx *Tx, node *Node, key, value []byte) (*Node, error) {
	if node.isLeaf {
		// COW before modifying leaf
		node, err := tx.ensureWritable(node)
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

			// Try serialize after update
			if err := node.serialize(tx.txnID); err != nil {
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

		// Try serialize after insertion
		if err := node.serialize(tx.txnID); err != nil {
			// Rollback: remove the inserted key/value
			node.keys = append(node.keys[:pos], node.keys[pos+1:]...)
			node.values = append(node.values[:pos], node.values[pos+1:]...)
			node.numKeys--
			return nil, err
		}

		return node, nil
	}

	// Branch node - recursive COW
	// B+ tree: branch nodes are routing only, never update here
	// Always descend to leaf even if key matches a branch key
	i := int(node.numKeys) - 1

	// Find child to insert into
	for i >= 0 && bytes.Compare(key, node.keys[i]) < 0 {
		i--
	}

	i++

	// Load child
	child, err := bt.loadNode(tx, node.children[i])
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
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Insert middle key into parent
		node.keys = insertAt(node.keys, i, midKey)
		node.values = insertAt(node.values, i, midVal)

		// Build new children array atomically to avoid slice sharing issues
		newChildren := make([]PageID, len(node.children)+1)
		copy(newChildren[:i], node.children[:i])     // Before split point
		newChildren[i] = leftChild.pageID            // Left child
		newChildren[i+1] = rightChild.pageID         // Right child
		copy(newChildren[i+2:], node.children[i+1:]) // After split point

		node.children = newChildren

		node.numKeys++
		node.dirty = true

		// Serialize parent after modification
		if err := node.serialize(tx.txnID); err != nil {
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
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Insert middle key into parent
		node.keys = insertAt(node.keys, i, midKey)
		node.values = insertAt(node.values, i, midVal)

		// Build new children array
		newChildren := make([]PageID, len(node.children)+1)
		copy(newChildren[:i], node.children[:i])
		newChildren[i] = leftChild.pageID
		newChildren[i+1] = rightChild.pageID
		copy(newChildren[i+2:], node.children[i+1:])

		node.children = newChildren
		node.numKeys++
		node.dirty = true

		// Serialize parent after modification
		if err := node.serialize(tx.txnID); err != nil {
			return nil, err
		}

		// Retry insert into correct child
		var insertedChild *Node
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
		node.children[i] = insertedChild.pageID
		node.dirty = true
		if err := node.serialize(tx.txnID); err != nil {
			return nil, err
		}

		return node, nil
	} else if err != nil {
		return nil, err
	}

	// If child was COWed, update parent pointer
	if newChild.pageID != oldChildID {
		// COW parent to update child pointer
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		node.children[i] = newChild.pageID
		node.dirty = true
		// Serialize after updating child pointer
		if err := node.serialize(tx.txnID); err != nil {
			return nil, err
		}
	}

	return node, nil
}

// deleteFromLeaf is the transaction-aware version of deleteFromLeaf.
// It performs COW on the leaf before deleting.
func (bt *BTree) deleteFromLeaf(tx *Tx, node *Node, idx int) (*Node, error) {
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
func (bt *BTree) mergeNodes(tx *Tx, leftNode, rightNode, parent *Node, parentKeyIdx int) (*Node, *Node, error) {
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
		leftNode.values = append(leftNode.values, parent.values[parentKeyIdx])
	}
	// For leaf merge: separator is routing only, don't pull it down

	// Add all keys/values from right node to left node
	leftNode.keys = append(leftNode.keys, rightNode.keys...)
	leftNode.values = append(leftNode.values, rightNode.values...)

	// If not leaf, copy children pointers too
	if !leftNode.isLeaf {
		leftNode.children = append(leftNode.children, rightNode.children...)
	}

	// Update left node's key count
	leftNode.numKeys = uint16(len(leftNode.keys))
	leftNode.dirty = true

	// Remove the separator key from parent
	parent.keys = removeAt(parent.keys, parentKeyIdx)
	parent.values = removeAt(parent.values, parentKeyIdx)
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
func (bt *BTree) borrowFromLeft(tx *Tx, node, leftSibling, parent *Node, parentKeyIdx int) (*Node, *Node, *Node, error) {
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

		// Update parent separator to be the first key of right node (truncated)
		// leftMax is the new last key of left sibling (after removal)
		var leftMax []byte
		if leftSibling.numKeys > 0 {
			leftMax = leftSibling.keys[leftSibling.numKeys-1]
		}
		parent.keys[parentKeyIdx] = truncateSeparatorKey(node.keys[0], leftMax)

		node.numKeys++
	} else {
		// Branch borrow: traditional B-tree style (pull down separator, push up replacement)
		// Move a key from parent to node (at beginning)
		node.keys = append([][]byte{parent.keys[parentKeyIdx]}, node.keys...)
		node.values = append([][]byte{parent.values[parentKeyIdx]}, node.values...)

		// Move the last key from left sibling to parent (truncated)
		// leftMax is the second-to-last key of left sibling (before removal)
		var leftMax []byte
		if leftSibling.numKeys > 1 {
			leftMax = leftSibling.keys[leftSibling.numKeys-2]
		}
		parent.keys[parentKeyIdx] = truncateSeparatorKey(leftSibling.keys[leftSibling.numKeys-1], leftMax)
		parent.values[parentKeyIdx] = leftSibling.values[leftSibling.numKeys-1]

		// Move the last child pointer too
		node.children = append([]PageID{leftSibling.children[len(leftSibling.children)-1]}, node.children...)
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]

		// Remove the last key from left sibling
		leftSibling.keys = leftSibling.keys[:leftSibling.numKeys-1]
		leftSibling.values = leftSibling.values[:leftSibling.numKeys-1]
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
func (bt *BTree) borrowFromRight(tx *Tx, node, rightSibling, parent *Node, parentKeyIdx int) (*Node, *Node, *Node, error) {
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

		// Update parent separator to be the first key of right sibling (truncated)
		// leftMax is the old last key of node (before we added the new one)
		var leftMax []byte
		if node.numKeys > 0 {
			leftMax = node.keys[node.numKeys-1]
		}
		parent.keys[parentKeyIdx] = truncateSeparatorKey(rightSibling.keys[0], leftMax)

		node.numKeys++
	} else {
		// Branch borrow: traditional B-tree style (pull down separator, push up replacement)
		// Move a key from parent to node (at end)
		node.keys = append(node.keys, parent.keys[parentKeyIdx])
		node.values = append(node.values, parent.values[parentKeyIdx])

		// Move the first key from right sibling to parent (truncated)
		// leftMax is the old last key of node (before we added parent's key)
		var leftMax []byte
		if node.numKeys > 0 {
			leftMax = node.keys[node.numKeys-1]
		}
		parent.keys[parentKeyIdx] = truncateSeparatorKey(rightSibling.keys[0], leftMax)
		parent.values[parentKeyIdx] = rightSibling.values[0]

		// Move the first child pointer too
		node.children = append(node.children, rightSibling.children[0])
		rightSibling.children = rightSibling.children[1:]

		// Remove the first key from right sibling
		rightSibling.keys = rightSibling.keys[1:]
		rightSibling.values = rightSibling.values[1:]
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
func (bt *BTree) fixUnderflow(tx *Tx, parent *Node, childIdx int, child *Node) (*Node, *Node, error) {
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
func (bt *BTree) deleteFromNonLeaf(tx *Tx, node *Node, key []byte, idx int) (*Node, error) {
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
func (bt *BTree) deleteFromNode(tx *Tx, node *Node, key []byte) (*Node, error) {
	// B+ tree: if this is a leaf, check if key exists and delete
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
func (bt *BTree) splitChild(tx *Tx, child *Node) (*Node, *Node, []byte, []byte, error) {
	// COW the child being split
	child, err := tx.ensureWritable(child)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Create new node for right half using tx.allocatePage()
	newNodeID, newNodePage, err := tx.allocatePage()
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

	newNode := &Node{
		page:     newNodePage,
		pageID:   newNodeID,
		dirty:    true,
		isLeaf:   child.isLeaf,
		numKeys:  uint16(rightKeyCount),
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]PageID, 0),
	}

	// Copy right half of keys/values to new node
	// Deep copy to avoid sharing underlying arrays
	for i := mid + 1; i < len(child.keys); i++ {
		keyCopy := make([]byte, len(child.keys[i]))
		copy(keyCopy, child.keys[i])
		newNode.keys = append(newNode.keys, keyCopy)

		valCopy := make([]byte, len(child.values[i]))
		copy(valCopy, child.values[i])
		newNode.values = append(newNode.values, valCopy)
	}

	// If not leaf, copy right half of children
	if !child.isLeaf {
		// Deep copy to avoid sharing underlying array
		for i := mid + 1; i < len(child.children); i++ {
			newNode.children = append(newNode.children, child.children[i])
		}
	}

	// Keep left portion in child
	// Deep copy to avoid sharing arrays with newNode
	leftKeys := make([][]byte, leftKeyCount)
	copy(leftKeys, child.keys[:leftKeyCount])
	child.keys = leftKeys

	leftValues := make([][]byte, leftKeyCount)
	copy(leftValues, child.values[:leftKeyCount])
	child.values = leftValues

	child.numKeys = uint16(leftKeyCount)
	child.dirty = true

	if !child.isLeaf {
		leftChildren := make([]PageID, mid+1)
		copy(leftChildren, child.children[:mid+1])
		child.children = leftChildren
	}

	// Serialize nodes to their pages
	// This ensures if they're evicted and reloaded, the page has the correct data
	if err := child.serialize(tx.txnID); err != nil {
		return nil, nil, nil, nil, err
	}
	if err := newNode.serialize(tx.txnID); err != nil {
		return nil, nil, nil, nil, err
	}

	// Cache newNode in TX-local cache (write transaction)
	// Newly split node goes into tx.pages, not global cache yet
	tx.pages[newNodeID] = newNode

	// B+ tree: Return empty value for parent (branch nodes don't store values)
	// The middle key is only for routing purposes

	// Truncate separator key to reduce branch node size
	// For leaf: leftMax is the last key of left child (child.keys[mid])
	// For branch: leftMax is the last key of left child (child.keys[mid-1] since mid was removed)
	var leftMax []byte
	if child.isLeaf {
		// Leaf: left keeps [0:mid+1], so last key is child.keys[mid]
		if mid < len(child.keys) {
			leftMax = child.keys[mid]
		}
	} else {
		// Branch: left keeps [0:mid], so last key is child.keys[mid-1]
		if mid > 0 && mid-1 < len(child.keys) {
			leftMax = child.keys[mid-1]
		}
	}
	middleKey = truncateSeparatorKey(middleKey, leftMax)

	return child, newNode, middleKey, []byte{}, nil
}

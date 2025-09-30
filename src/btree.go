package src

import (
	"bytes"
)

const (
	// MaxKeysPerNode must be small enough that a full node can serialize to PageSize
	MaxKeysPerNode = 64
	MinKeysPerNode = MaxKeysPerNode / 4 // Minimum keys for non-root nodes
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

// getNextLeaf returns the next leaf pointer for this node (0 if none)
func (n *Node) getNextLeaf() PageID {
	if !n.isLeaf {
		return 0
	}
	header := n.page.Header()
	return header.NextLeaf
}

// setNextLeaf sets the next leaf pointer
func (n *Node) setNextLeaf(id PageID) {
	if n.isLeaf {
		header := n.page.Header()
		header.NextLeaf = id
		n.dirty = true
	}
}

// getPrevLeaf returns the previous leaf pointer for this node (0 if none)
func (n *Node) getPrevLeaf() PageID {
	if !n.isLeaf {
		return 0
	}
	header := n.page.Header()
	return header.PrevLeaf
}

// setPrevLeaf sets the previous leaf pointer
func (n *Node) setPrevLeaf(id PageID) {
	if n.isLeaf {
		header := n.page.Header()
		header.PrevLeaf = id
		n.dirty = true
	}
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
		// Load existing root
		root, err := bt.loadNode(meta.RootPageID)
		if err != nil {
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
		if err := bt.root.serialize(); err != nil {
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
func (bt *BTree) searchNode(node *Node, key []byte) ([]byte, error) {
	// Binary search for key position
	i := 0
	for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
		i++
	}

	// Check if key found
	if i < int(node.numKeys) && bytes.Equal(key, node.keys[i]) {
		// Found the key
		return node.values[i], nil
	}

	// If leaf node, key not found
	if node.isLeaf {
		return nil, ErrKeyNotFound
	}

	// Load child node and continue search
	child, err := bt.loadNode(node.children[i])
	if err != nil {
		return nil, err
	}

	return bt.searchNode(child, key)
}

// loadNode loads a node from disk or cache
func (bt *BTree) loadNode(pageID PageID) (*Node, error) {
	// Check cache first
	if cached, hit := bt.cache.Get(pageID); hit {
		bt.cache.Unpin(pageID)

		// Cycle detection: check if this node references itself
		if !cached.isLeaf {
			for _, childID := range cached.children {
				if childID == pageID {
					return nil, ErrCorruption // Self-reference detected
				}
			}
		}

		return cached, nil
	}

	// Load from disk
	page, err := bt.pager.ReadPage(pageID)
	if err != nil {
		return nil, err
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
			return nil, err
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
				return nil, ErrCorruption // Self-reference detected
			}
		}
	}

	// Cache it
	bt.cache.Put(pageID, node)
	bt.cache.Unpin(pageID)

	return node, nil
}

// isFull checks if a node is full (simplified for in-memory)
func (n *Node) isFull() bool {
	return int(n.numKeys) >= MaxKeysPerNode
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
		size += int(n.numKeys) * BranchElementSize
		size += 8 // children[0]
		for i := 0; i < int(n.numKeys); i++ {
			size += len(n.keys[i]) + len(n.values[i])
		}
	}

	return size
}

// serialize encodes the node data into page.data
func (n *Node) serialize() error {
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
		PageID:   n.pageID,
		NumKeys:  n.numKeys,
		NextLeaf: 0,
		PrevLeaf: 0,
	}
	if n.isLeaf {
		header.Flags = LeafPageFlag
	} else {
		header.Flags = BranchPageFlag
	}
	n.page.WriteHeader(header)

	if n.isLeaf {
		// Serialize leaf node
		dataOffset := uint16(0)
		for i := 0; i < int(n.numKeys); i++ {
			key := n.keys[i]
			value := n.values[i]

			elem := &LeafElement{
				KeyOffset:   dataOffset,
				KeySize:     uint16(len(key)),
				ValueOffset: dataOffset + uint16(len(key)),
				ValueSize:   uint16(len(value)),
			}
			n.page.WriteLeafElement(i, elem)

			// Write key and value to data area
			dataStart := n.page.DataAreaStart()
			copy(n.page.data[dataStart+int(dataOffset):], key)
			dataOffset += uint16(len(key))
			copy(n.page.data[dataStart+int(dataOffset):], value)
			dataOffset += uint16(len(value))
		}
	} else {
		// Serialize branch node
		// Write children[0] first
		if len(n.children) > 0 {
			n.page.WriteBranchFirstChild(n.children[0])
		}

		// Write keys, values, and remaining children
		dataOffset := uint16(8) // Start after children[0]
		for i := 0; i < int(n.numKeys); i++ {
			key := n.keys[i]
			value := n.values[i]

			elem := &BranchElement{
				KeyOffset:   dataOffset,
				KeySize:     uint16(len(key)),
				ValueOffset: dataOffset + uint16(len(key)),
				ValueSize:   uint16(len(value)),
				ChildID:     n.children[i+1],
			}
			n.page.WriteBranchElement(i, elem)

			// Write key and value to data area
			dataStart := n.page.DataAreaStart()
			copy(n.page.data[dataStart+int(dataOffset):], key)
			dataOffset += uint16(len(key))
			copy(n.page.data[dataStart+int(dataOffset):], value)
			dataOffset += uint16(len(value))
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
		// Deserialize branch node
		n.keys = make([][]byte, n.numKeys)
		n.values = make([][]byte, n.numKeys) // Branch nodes also store values for separator keys
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

			// Copy value (separator values in branch nodes)
			valueData, err := n.page.GetValue(elem.ValueOffset, elem.ValueSize)
			if err != nil {
				return err
			}
			n.values[i] = make([]byte, len(valueData))
			copy(n.values[i], valueData)
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
func (bt *BTree) findPredecessor(node *Node) ([]byte, []byte, error) {
	// Keep going right until we reach a leaf
	for !node.isLeaf {
		lastChildIdx := len(node.children) - 1
		child, err := bt.loadNode(node.children[lastChildIdx])
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
func (bt *BTree) findSuccessor(node *Node) ([]byte, []byte, error) {
	// Keep going left until we reach a leaf
	for !node.isLeaf {
		child, err := bt.loadNode(node.children[0])
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
			node.values[pos] = value
			node.dirty = true
			// Serialize after update
			if err := node.serialize(); err != nil {
				return nil, err
			}
			return node, nil
		}

		// Insert new key-value
		node.keys = append(node.keys[:pos], append([][]byte{key}, node.keys[pos:]...)...)
		node.values = append(node.values[:pos], append([][]byte{value}, node.values[pos:]...)...)
		node.numKeys++
		node.dirty = true

		// Serialize after modification
		if err := node.serialize(); err != nil {
			return nil, err
		}

		return node, nil
	}

	// Branch node - recursive COW
	i := int(node.numKeys) - 1

	// Find child to insert into
	for i >= 0 && bytes.Compare(key, node.keys[i]) < 0 {
		i--
	}

	// Check for update in branch
	if i >= 0 && bytes.Equal(key, node.keys[i]) {
		// Key exists in branch node - COW and update
		node, err := tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}
		node.values[i] = value
		node.dirty = true
		return node, nil
	}

	i++

	// Load child
	child, err := bt.loadNode(node.children[i])
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
		if err := node.serialize(); err != nil {
			return nil, err
		}

		// Determine which child to use after split
		if bytes.Compare(key, midKey) > 0 {
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
	if err != nil {
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
		if err := node.serialize(); err != nil {
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

	// Add parent's separator key/value to left node
	leftNode.keys = append(leftNode.keys, parent.keys[parentKeyIdx])
	leftNode.values = append(leftNode.values, parent.values[parentKeyIdx])

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

	// Move a key from parent to node (at beginning)
	node.keys = append([][]byte{parent.keys[parentKeyIdx]}, node.keys...)
	node.values = append([][]byte{parent.values[parentKeyIdx]}, node.values...)

	// Move the last key from left sibling to parent
	parent.keys[parentKeyIdx] = leftSibling.keys[leftSibling.numKeys-1]
	parent.values[parentKeyIdx] = leftSibling.values[leftSibling.numKeys-1]

	// If not leaf, move the last child pointer too
	if !node.isLeaf {
		node.children = append([]PageID{leftSibling.children[len(leftSibling.children)-1]}, node.children...)
		leftSibling.children = leftSibling.children[:len(leftSibling.children)-1]
	}

	// Remove the last key from left sibling
	leftSibling.keys = leftSibling.keys[:leftSibling.numKeys-1]
	leftSibling.values = leftSibling.values[:leftSibling.numKeys-1]
	leftSibling.numKeys--

	node.numKeys++

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

	// Move a key from parent to node (at end)
	node.keys = append(node.keys, parent.keys[parentKeyIdx])
	node.values = append(node.values, parent.values[parentKeyIdx])

	// Move the first key from right sibling to parent
	parent.keys[parentKeyIdx] = rightSibling.keys[0]
	parent.values[parentKeyIdx] = rightSibling.values[0]

	// If not leaf, move the first child pointer too
	if !node.isLeaf {
		node.children = append(node.children, rightSibling.children[0])
		rightSibling.children = rightSibling.children[1:]
	}

	// Remove the first key from right sibling
	rightSibling.keys = rightSibling.keys[1:]
	rightSibling.values = rightSibling.values[1:]
	rightSibling.numKeys--

	node.numKeys++

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
		leftSibling, err := bt.loadNode(parent.children[childIdx-1])
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
		rightSibling, err := bt.loadNode(parent.children[childIdx+1])
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
		leftSibling, err := bt.loadNode(parent.children[childIdx-1])
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
	rightSibling, err := bt.loadNode(parent.children[childIdx+1])
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
	leftChild, err := bt.loadNode(node.children[idx])
	if err != nil {
		return nil, err
	}

	if leftChild.numKeys > MinKeysPerNode {
		// Get predecessor
		predKey, predVal, err := bt.findPredecessor(leftChild)
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
	rightChild, err := bt.loadNode(node.children[idx+1])
	if err != nil {
		return nil, err
	}

	if rightChild.numKeys > MinKeysPerNode {
		// Get successor
		succKey, succVal, err := bt.findSuccessor(rightChild)
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
func (bt *BTree) deleteFromNode(tx *Tx, node *Node, key []byte) (*Node, error) {
	idx := node.findKey(key)

	if idx >= 0 {
		// Key found in this node
		if node.isLeaf {
			return bt.deleteFromLeaf(tx, node, idx)
		}
		return bt.deleteFromNonLeaf(tx, node, key, idx)
	}

	// Key not in this node
	if node.isLeaf {
		return nil, ErrKeyNotFound
	}

	// Find child where key might be
	childIdx := 0
	for childIdx < int(node.numKeys) && bytes.Compare(key, node.keys[childIdx]) > 0 {
		childIdx++
	}

	child, err := bt.loadNode(node.children[childIdx])
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

	newNode := &Node{
		page:     newNodePage,
		pageID:   newNodeID,
		dirty:    true,
		isLeaf:   child.isLeaf,
		numKeys:  uint16(len(child.keys) - mid - 1),
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

	// Extract middle key/value before truncating child
	// Deep copy to avoid reference issues
	middleKey := make([]byte, len(child.keys[mid]))
	copy(middleKey, child.keys[mid])
	middleValue := make([]byte, len(child.values[mid]))
	copy(middleValue, child.values[mid])

	// Keep left half in child
	// Deep copy to avoid sharing arrays with newNode
	leftKeys := make([][]byte, mid)
	copy(leftKeys, child.keys[:mid])
	child.keys = leftKeys

	leftValues := make([][]byte, mid)
	copy(leftValues, child.values[:mid])
	child.values = leftValues

	child.numKeys = uint16(mid)
	child.dirty = true

	if !child.isLeaf {
		leftChildren := make([]PageID, mid+1)
		copy(leftChildren, child.children[:mid+1])
		child.children = leftChildren
	}

	// Serialize nodes to their pages
	// This ensures if they're evicted and reloaded, the page has the correct data
	if err := child.serialize(); err != nil {
		return nil, nil, nil, nil, err
	}
	if err := newNode.serialize(); err != nil {
		return nil, nil, nil, nil, err
	}

	// Cache newNode (right child from split)
	// child is already cached from ensureWritable above
	bt.cache.Put(newNodeID, newNode)

	return child, newNode, middleKey, middleValue, nil
}

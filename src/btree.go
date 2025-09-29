package src

import (
	"bytes"
)

const (
	MaxKeysPerNode = 256                // Simplified for in-memory version
	MinKeysPerNode = MaxKeysPerNode / 2 // Minimum keys for non-root nodes
)

// PageManager handles disk I/O
type PageManager interface {
	ReadPage(id PageID) (*Page, error)
	WritePage(id PageID, page *Page) error
	AllocatePage() (PageID, error)
	FreePage(id PageID) error
}

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

// BTree is the main structure
type BTree struct {
	pager     PageManager
	root      *Node
	pageCache map[PageID]*Node
}

// NewBTree creates a new BTree with the given PageManager
func NewBTree(pager PageManager) (*BTree, error) {
	// Allocate root page
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

	bt := &BTree{
		pager:     pager,
		root:      root,
		pageCache: make(map[PageID]*Node),
	}

	return bt, nil
}

// Get searches for a key in the B-tree
func (bt *BTree) Get(key []byte) ([]byte, error) {
	return bt.searchNode(bt.root, key)
}

// Set inserts or updates a key-value pair in the B-tree
func (bt *BTree) Set(key, value []byte) error {
	// Check if root is full
	if bt.root.isFull() {
		// Create new root
		newRootID, err := bt.pager.AllocatePage()
		if err != nil {
			return err
		}

		newRootPage, err := bt.pager.ReadPage(newRootID)
		if err != nil {
			return err
		}

		oldRoot := bt.root
		newRoot := &Node{
			page:     newRootPage,
			pageID:   newRootID,
			dirty:    true,
			isLeaf:   false,
			numKeys:  0,
			keys:     make([][]byte, 0),
			values:   make([][]byte, 0),
			children: []PageID{oldRoot.pageID},
		}

		// Cache the old root before it becomes a child
		bt.pageCache[oldRoot.pageID] = oldRoot

		bt.root = newRoot

		// Split the old root
		if err := bt.splitChild(newRoot, 0, oldRoot); err != nil {
			return err
		}
	}

	return bt.insertNonFull(bt.root, key, value)
}

// Delete removes a key from the B-tree
func (bt *BTree) Delete(key []byte) error {
	if bt.root == nil || bt.root.numKeys == 0 {
		return ErrKeyNotFound
	}

	// Delete from root
	err := bt.deleteFromNode(bt.root, key)
	if err != nil {
		return err
	}

	// If root is empty after deletion, make its only child the new root
	if bt.root.numKeys == 0 {
		if !bt.root.isLeaf && len(bt.root.children) > 0 {
			// Free old root
			oldRootID := bt.root.pageID

			// Load new root
			newRoot, err := bt.loadNode(bt.root.children[0])
			if err != nil {
				return err
			}

			bt.root = newRoot
			delete(bt.pageCache, newRoot.pageID) // Remove from cache since it's now root

			// Free old root page
			if err := bt.pager.FreePage(oldRootID); err != nil {
				return err
			}
		}
	}

	return nil
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
	for _, node := range bt.pageCache {
		if node.dirty {
			if err := node.serialize(); err != nil {
				return err
			}
			if err := bt.pager.WritePage(node.pageID, node.page); err != nil {
				return err
			}
			node.dirty = false
		}
	}

	// Clear cache
	bt.pageCache = nil
	bt.root = nil

	return nil
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
	if cached, exists := bt.pageCache[pageID]; exists {
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

	// Cache it
	bt.pageCache[pageID] = node

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
			size += len(n.keys[i])
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
		PageID:  n.pageID,
		NumKeys: n.numKeys,
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

		// Write keys and remaining children
		dataOffset := uint16(8) // Start after children[0]
		for i := 0; i < int(n.numKeys); i++ {
			key := n.keys[i]

			elem := &BranchElement{
				KeyOffset: dataOffset,
				KeySize:   uint16(len(key)),
				ChildID:   n.children[i+1],
			}
			n.page.WriteBranchElement(i, elem)

			// Write key to data area
			dataStart := n.page.DataAreaStart()
			copy(n.page.data[dataStart+int(dataOffset):], key)
			dataOffset += uint16(len(key))
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
			keyData := n.page.GetKey(elem.KeyOffset, elem.KeySize)
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy value
			valueData := n.page.GetValue(elem.ValueOffset, elem.ValueSize)
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
			keyData := n.page.GetKey(elem.KeyOffset, elem.KeySize)
			n.keys[i] = make([]byte, len(keyData))
			copy(n.keys[i], keyData)

			// Copy child pointer
			n.children[i+1] = elem.ChildID

			// Branch nodes need values too (for separator keys)
			// For now, initialize empty values
			n.values[i] = make([]byte, 0)
		}
	}

	return nil
}

// splitChild splits a full child node
func (bt *BTree) splitChild(parent *Node, index int, child *Node) error {
	// Create new node for right half
	newNodeID, err := bt.pager.AllocatePage()
	if err != nil {
		return err
	}

	newNodePage, err := bt.pager.ReadPage(newNodeID)
	if err != nil {
		return err
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
	newNode.keys = append(newNode.keys, child.keys[mid+1:]...)
	newNode.values = append(newNode.values, child.values[mid+1:]...)

	// If not leaf, copy right half of children
	if !child.isLeaf {
		newNode.children = append(newNode.children, child.children[mid+1:]...)
	}

	// Keep left half in original child
	middleKey := child.keys[mid]
	middleValue := child.values[mid]

	child.keys = child.keys[:mid]
	child.values = child.values[:mid]
	child.numKeys = uint16(mid)
	child.dirty = true

	if !child.isLeaf {
		child.children = child.children[:mid+1]
	}

	// Insert middle key into parent
	parent.keys = insertAt(parent.keys, index, middleKey)
	parent.values = insertAt(parent.values, index, middleValue)
	parent.children = insertChildAt(parent.children, index+1, newNodeID)
	parent.numKeys++
	parent.dirty = true

	// Cache new node
	bt.pageCache[newNodeID] = newNode

	return nil
}

// insertNonFull inserts into a non-full node
func (bt *BTree) insertNonFull(node *Node, key, value []byte) error {
	i := int(node.numKeys) - 1

	if node.isLeaf {
		// Binary search for position
		pos := 0
		for pos < int(node.numKeys) && bytes.Compare(key, node.keys[pos]) > 0 {
			pos++
		}

		// Check for update
		if pos < int(node.numKeys) && bytes.Equal(key, node.keys[pos]) {
			node.values[pos] = value
			node.dirty = true
			return nil
		}

		// Insert new key-value
		node.keys = append(node.keys[:pos], append([][]byte{key}, node.keys[pos:]...)...)
		node.values = append(node.values[:pos], append([][]byte{value}, node.values[pos:]...)...)
		node.numKeys++
		node.dirty = true
	} else {
		// Find child to insert into
		for i >= 0 && bytes.Compare(key, node.keys[i]) < 0 {
			i--
		}

		// Check for update
		if i >= 0 && bytes.Equal(key, node.keys[i]) {
			node.values[i] = value
			node.dirty = true
			return nil
		}

		i++

		// Load child
		child, err := bt.loadNode(node.children[i])
		if err != nil {
			return err
		}

		// Split child if full
		if child.isFull() {
			if err := bt.splitChild(node, i, child); err != nil {
				return err
			}

			// Determine which child to use after split
			if bytes.Compare(key, node.keys[i]) > 0 {
				i++
				child, err = bt.loadNode(node.children[i])
				if err != nil {
					return err
				}
			}
		}

		return bt.insertNonFull(child, key, value)
	}

	return nil
}

// Helper functions for slice operations
func insertAt(slice [][]byte, index int, value []byte) [][]byte {
	slice = append(slice[:index], append([][]byte{value}, slice[index:]...)...)
	return slice
}

func insertChildAt(slice []PageID, index int, value PageID) []PageID {
	slice = append(slice[:index], append([]PageID{value}, slice[index:]...)...)
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

// mergeNodes merges node with its sibling through the parent's separator key
func (bt *BTree) mergeNodes(leftNode, rightNode *Node, parent *Node, parentKeyIdx int) error {
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

	// Free the right node's page
	if err := bt.pager.FreePage(rightNode.pageID); err != nil {
		return err
	}

	// Remove from cache
	delete(bt.pageCache, rightNode.pageID)

	return nil
}

// borrowFromLeft borrows a key from left sibling through parent
func (bt *BTree) borrowFromLeft(node, leftSibling, parent *Node, parentKeyIdx int) {
	// Move a key from parent to node
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

	// Mark nodes as dirty
	node.dirty = true
	leftSibling.dirty = true
	parent.dirty = true
}

// borrowFromRight borrows a key from right sibling through parent
func (bt *BTree) borrowFromRight(node, rightSibling, parent *Node, parentKeyIdx int) {
	// Move a key from parent to node
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

	// Mark nodes as dirty
	node.dirty = true
	rightSibling.dirty = true
	parent.dirty = true
}

// deleteFromNode recursively deletes a key from the subtree rooted at node
func (bt *BTree) deleteFromNode(node *Node, key []byte) error {
	idx := node.findKey(key)

	if idx >= 0 {
		// Key found in this node
		if node.isLeaf {
			return bt.deleteFromLeaf(node, idx)
		}
		return bt.deleteFromNonLeaf(node, key, idx)
	}

	// Key not in this node
	if node.isLeaf {
		return ErrKeyNotFound
	}

	// Find child where key might be
	childIdx := 0
	for childIdx < int(node.numKeys) && bytes.Compare(key, node.keys[childIdx]) > 0 {
		childIdx++
	}

	child, err := bt.loadNode(node.children[childIdx])
	if err != nil {
		return err
	}

	// Check if child will underflow
	shouldCheckUnderflow := (child.numKeys == MinKeysPerNode)

	// Delete from child
	err = bt.deleteFromNode(child, key)
	if err != nil {
		return err
	}

	// Handle underflow if necessary
	if shouldCheckUnderflow && child.isUnderflow() && node != bt.root {
		return bt.fixUnderflow(node, childIdx, child)
	}

	return nil
}

// deleteFromLeaf deletes a key at index from a leaf node
func (bt *BTree) deleteFromLeaf(node *Node, idx int) error {
	// Simply remove the key and value
	node.keys = removeAt(node.keys, idx)
	node.values = removeAt(node.values, idx)
	node.numKeys--
	node.dirty = true
	return nil
}

// deleteFromNonLeaf deletes a key from a non-leaf node
func (bt *BTree) deleteFromNonLeaf(node *Node, key []byte, idx int) error {
	// Try to replace with predecessor from left subtree
	leftChild, err := bt.loadNode(node.children[idx])
	if err != nil {
		return err
	}

	if leftChild.numKeys > MinKeysPerNode {
		// Get predecessor
		predKey, predVal, err := bt.findPredecessor(leftChild)
		if err != nil {
			return err
		}

		// Replace key with predecessor
		node.keys[idx] = predKey
		node.values[idx] = predVal
		node.dirty = true

		// Delete predecessor from left subtree
		return bt.deleteFromNode(leftChild, predKey)
	}

	// Try to replace with successor from right subtree
	rightChild, err := bt.loadNode(node.children[idx+1])
	if err != nil {
		return err
	}

	if rightChild.numKeys > MinKeysPerNode {
		// Get successor
		succKey, succVal, err := bt.findSuccessor(rightChild)
		if err != nil {
			return err
		}

		// Replace key with successor
		node.keys[idx] = succKey
		node.values[idx] = succVal
		node.dirty = true

		// Delete successor from right subtree
		return bt.deleteFromNode(rightChild, succKey)
	}

	// Both children have minimum keys, merge them
	if err := bt.mergeNodes(leftChild, rightChild, node, idx); err != nil {
		return err
	}

	// Delete from the merged node
	return bt.deleteFromNode(leftChild, key)
}

// fixUnderflow fixes underflow in child at childIdx
func (bt *BTree) fixUnderflow(parent *Node, childIdx int, child *Node) error {
	// Try to borrow from left sibling
	if childIdx > 0 {
		leftSibling, err := bt.loadNode(parent.children[childIdx-1])
		if err != nil {
			return err
		}

		if leftSibling.numKeys > MinKeysPerNode {
			bt.borrowFromLeft(child, leftSibling, parent, childIdx-1)
			return nil
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(parent.children)-1 {
		rightSibling, err := bt.loadNode(parent.children[childIdx+1])
		if err != nil {
			return err
		}

		if rightSibling.numKeys > MinKeysPerNode {
			bt.borrowFromRight(child, rightSibling, parent, childIdx)
			return nil
		}
	}

	// Merge with a sibling
	if childIdx > 0 {
		// Merge with left sibling
		leftSibling, err := bt.loadNode(parent.children[childIdx-1])
		if err != nil {
			return err
		}
		return bt.mergeNodes(leftSibling, child, parent, childIdx-1)
	}

	// Merge with right sibling
	rightSibling, err := bt.loadNode(parent.children[childIdx+1])
	if err != nil {
		return err
	}
	return bt.mergeNodes(child, rightSibling, parent, childIdx)
}

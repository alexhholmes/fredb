package src

import (
	"bytes"
)

const (
	PageSize       = 4096
	PageHeaderSize = 16
	MaxKeysPerNode = 256 // Simplified for in-memory version
)

type PageID uint32

// Page is raw disk page
type Page struct {
	data [PageSize]byte
}

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
	panic("not implemented")
}

// Close flushes any dirty pages and closes the B-tree
func (bt *BTree) Close() error {
	// Flush root if dirty
	if bt.root != nil && bt.root.dirty {
		// TODO: Serialize node data into page.data before writing
		if err := bt.pager.WritePage(bt.root.pageID, bt.root.page); err != nil {
			return err
		}
		bt.root.dirty = false
	}

	// Flush all cached nodes
	for _, node := range bt.pageCache {
		if node.dirty {
			// TODO: Serialize node data into page.data before writing
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

	// Create node (would normally decode from page.data)
	node := &Node{
		page:   page,
		pageID: pageID,
		dirty:  false,
		// TODO: Decode these from page.data
		isLeaf:   true,
		numKeys:  0,
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]PageID, 0),
	}

	// Cache it
	bt.pageCache[pageID] = node

	return node, nil
}

// isFull checks if a node is full (simplified for in-memory)
func (n *Node) isFull() bool {
	return int(n.numKeys) >= MaxKeysPerNode
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

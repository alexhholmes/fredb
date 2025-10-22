package algo

import (
	"github.com/alexhholmes/fredb/internal/base"
)

// ApplyLeafUpdate updates a key's value in leaf node
// Assumes node is already writable (COW'd by caller)
func ApplyLeafUpdate(node *base.Node, pos int, newValue []byte) {
	// Deep copy the new value to prevent aliasing
	valCopy := make([]byte, len(newValue))
	copy(valCopy, newValue)
	node.Values[pos] = valCopy
	node.Dirty = true
}

// ApplyLeafInsert inserts new key-value at position
// Assumes node is already writable and has space
func ApplyLeafInsert(node *base.Node, pos int, key, value []byte) {
	node.Keys = InsertAt(node.Keys, pos, key)
	node.Values = InsertAt(node.Values, pos, value)
	node.NumKeys++
	node.Dirty = true
}

// ApplyLeafDelete removes key at position
// Assumes node is already writable
func ApplyLeafDelete(node *base.Node, idx int) {
	node.Keys = RemoveAt(node.Keys, idx)
	node.Values = RemoveAt(node.Values, idx)
	node.NumKeys--
	node.Dirty = true
}

// ApplyBranchRemoveSeparator removes separator key and child after merge
// Removes the separator at sepIdx and the child at sepIdx+1
// Assumes node is already writable
func ApplyBranchRemoveSeparator(node *base.Node, sepIdx int) {
	node.Keys = RemoveAt(node.Keys, sepIdx)
	if node.IsLeaf() {
		node.Values = RemoveAt(node.Values, sepIdx)
	}
	node.Children = RemoveChildAt(node.Children, sepIdx+1)
	node.NumKeys--
	node.Dirty = true
}

// BorrowFromLeft moves last element from left sibling to beginning of right node
// Updates parent separator key
// Assumes all nodes are already writable
// Returns updated parent
func BorrowFromLeft(node, leftSibling, parent *base.Node, parentKeyIdx int) {
	if node.IsLeaf() {
		// Extract last from left sibling
		borrowed := ExtractLastFromSibling(leftSibling)

		// Insert at beginning of node
		node.Keys = InsertAt(node.Keys, 0, borrowed.Key)
		node.Values = InsertAt(node.Values, 0, borrowed.Value)
		node.NumKeys++

		// Remove borrowed key from left sibling
		lastIdx := int(leftSibling.NumKeys) - 1
		leftSibling.Keys = RemoveAt(leftSibling.Keys, lastIdx)
		leftSibling.Values = RemoveAt(leftSibling.Values, lastIdx)
		leftSibling.NumKeys--

		// Update parent separator to be the first key of right node
		parent.Keys[parentKeyIdx] = node.Keys[0]
	} else {
		// Branch borrow: traditional B-tree style
		borrowed := ExtractLastFromSibling(leftSibling)

		// Move parent key to node (at beginning)
		node.Keys = InsertAt(node.Keys, 0, parent.Keys[parentKeyIdx])
		node.Children = append([]base.PageID{borrowed.Child}, node.Children...)
		node.NumKeys++

		// Remove borrowed key and child from left sibling
		lastIdx := int(leftSibling.NumKeys) - 1
		leftSibling.Keys = RemoveAt(leftSibling.Keys, lastIdx)
		leftSibling.Children = RemoveChildAt(leftSibling.Children, len(leftSibling.Children)-1)
		leftSibling.NumKeys--

		// Move last key from left sibling to parent
		parent.Keys[parentKeyIdx] = borrowed.Key
	}

	// Mark all as dirty
	node.Dirty = true
	leftSibling.Dirty = true
	parent.Dirty = true
}

// BorrowFromRight moves first element from right sibling to end of left node
// Updates parent separator key
// Assumes all nodes are already writable
func BorrowFromRight(node, rightSibling, parent *base.Node, parentKeyIdx int) {
	if node.IsLeaf() {
		// Extract first from right sibling
		borrowed := ExtractFirstFromSibling(rightSibling)

		// Deep copy and append to end of node
		keyCopy := make([]byte, len(borrowed.Key))
		copy(keyCopy, borrowed.Key)
		valCopy := make([]byte, len(borrowed.Value))
		copy(valCopy, borrowed.Value)
		node.Keys = append(node.Keys, keyCopy)
		node.Values = append(node.Values, valCopy)
		node.NumKeys++

		// Remove borrowed key from right sibling
		rightSibling.Keys = RemoveAt(rightSibling.Keys, 0)
		rightSibling.Values = RemoveAt(rightSibling.Values, 0)
		rightSibling.NumKeys--

		// Update parent separator to be the first key of right sibling
		parent.Keys[parentKeyIdx] = rightSibling.Keys[0]
	} else {
		// Branch borrow: traditional B-tree style
		borrowed := ExtractFirstFromSibling(rightSibling)

		// Deep copy parent key and append to node
		keyCopy := make([]byte, len(parent.Keys[parentKeyIdx]))
		copy(keyCopy, parent.Keys[parentKeyIdx])
		node.Keys = append(node.Keys, keyCopy)
		node.Children = append(node.Children, borrowed.Child)
		node.NumKeys++

		// Remove borrowed key and child from right sibling
		rightSibling.Keys = RemoveAt(rightSibling.Keys, 0)
		rightSibling.Children = RemoveChildAt(rightSibling.Children, 0)
		rightSibling.NumKeys--

		// Move first key from right sibling to parent
		parent.Keys[parentKeyIdx] = borrowed.Key
	}

	// Mark all as dirty
	node.Dirty = true
	rightSibling.Dirty = true
	parent.Dirty = true
}

// MergeNodes combines right node into left node
// For branch nodes, includes separator key from parent
// Assumes left node is already writable
// Does NOT update parent - caller must call ApplyBranchRemoveSeparator
func MergeNodes(leftNode, rightNode *base.Node, separatorKey []byte) {
	// Determine node type by checking for children (more reliable than IsLeaf())
	// This handles corrupted nodes where Values might be incorrectly set
	hasChildren := len(leftNode.Children) > 0

	if hasChildren {
		// Branch node: pull down separator key (deep copy)
		sepCopy := make([]byte, len(separatorKey))
		copy(sepCopy, separatorKey)
		leftNode.Keys = append(leftNode.Keys, sepCopy)

		// Deep copy keys from right node
		for _, key := range rightNode.Keys {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			leftNode.Keys = append(leftNode.Keys, keyCopy)
		}

		// Always clear Values for branch nodes (defense against corruption)
		leftNode.Values = nil

		// Merge children pointers (PageIDs are copy-by-value)
		leftNode.Children = append(leftNode.Children, rightNode.Children...)
	} else {
		// Leaf node: no separator, deep copy keys and values from right
		for _, key := range rightNode.Keys {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			leftNode.Keys = append(leftNode.Keys, keyCopy)
		}
		for _, val := range rightNode.Values {
			valCopy := make([]byte, len(val))
			copy(valCopy, val)
			leftNode.Values = append(leftNode.Values, valCopy)
		}
	}

	// Update left node's key count
	leftNode.NumKeys = uint16(len(leftNode.Keys))
	leftNode.Dirty = true
}

// NewBranchRoot creates a new branch root node from two children after split
func NewBranchRoot(leftChild, rightChild *base.Node, midKey []byte, pageID base.PageID) *base.Node {
	return &base.Node{
		PageID:   pageID,
		Dirty:    true,
		NumKeys:  1,
		Keys:     [][]byte{midKey},
		Values:   nil,
		Children: []base.PageID{leftChild.PageID, rightChild.PageID},
	}
}

// ApplyChildSplit updates parent after splitting child at childIdx
// Inserts separator key and updates children pointers
// Assumes parent is already writable (COW'd by caller)
func ApplyChildSplit(parent *base.Node, childIdx int, leftChild, rightChild *base.Node, midKey, midVal []byte) {
	// Insert middle key into parent
	parent.Keys = InsertAt(parent.Keys, childIdx, midKey)
	if parent.IsLeaf() {
		parent.Values = InsertAt(parent.Values, childIdx, midVal)
	}

	// Update children array
	newChildren := make([]base.PageID, len(parent.Children)+1)
	copy(newChildren[:childIdx], parent.Children[:childIdx])
	newChildren[childIdx] = leftChild.PageID
	newChildren[childIdx+1] = rightChild.PageID
	copy(newChildren[childIdx+2:], parent.Children[childIdx+1:])

	parent.Children = newChildren
	parent.NumKeys++
	parent.Dirty = true
}

// TruncateLeft modifies node to keep only left portion after split
// Assumes node is already writable (COW'd by caller)
func TruncateLeft(node *base.Node, sp SplitPoint) {
	// Handle special case: Mid=-1 means left gets nothing (sp.LeftCount=0)
	if sp.Mid == -1 {
		node.Keys = [][]byte{}
		if node.IsLeaf() {
			node.Values = [][]byte{}
		} else {
			node.Values = nil
			node.Children = []base.PageID{}
		}
		node.NumKeys = 0
		node.Dirty = true
		return
	}

	// Deep copy keys to prevent aliasing with split sibling
	leftKeys := make([][]byte, sp.LeftCount)
	for i := 0; i < sp.LeftCount; i++ {
		keyCopy := make([]byte, len(node.Keys[i]))
		copy(keyCopy, node.Keys[i])
		leftKeys[i] = keyCopy
	}
	node.Keys = leftKeys

	if node.IsLeaf() {
		// Deep copy values to prevent aliasing with split sibling
		leftVals := make([][]byte, sp.LeftCount)
		for i := 0; i < sp.LeftCount; i++ {
			valCopy := make([]byte, len(node.Values[i]))
			copy(valCopy, node.Values[i])
			leftVals[i] = valCopy
		}
		node.Values = leftVals
	} else {
		node.Values = nil
		leftChildren := make([]base.PageID, sp.Mid+1)
		copy(leftChildren, node.Children[:sp.Mid+1])
		node.Children = leftChildren
	}

	node.NumKeys = uint16(sp.LeftCount)
	node.Dirty = true
}

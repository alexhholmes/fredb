package algo

import (
	"github.com/alexhholmes/fredb/internal/base"
)

// ApplyLeafUpdate updates a key's value in leaf node
// Assumes node is already writable (COW'd by caller)
// Returns error if serialization would fail (for rollback)
func ApplyLeafUpdate(node *base.Node, pos int, newValue []byte) {
	// Check if this position has an overflow chain before updating
	_, hasOverflow := node.OverflowChains[pos]

	node.Values[pos] = newValue
	node.Dirty = true

	// Only mark as modified if it had an overflow chain (to track old chain for freeing)
	if hasOverflow {
		if node.ModifiedValues == nil {
			node.ModifiedValues = make(map[int]struct{})
		}
		node.ModifiedValues[pos] = struct{}{}
	}
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

		// Append to end of node
		node.Keys = append(node.Keys, borrowed.Key)
		node.Values = append(node.Values, borrowed.Value)
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

		// Move parent key to node (at end)
		node.Keys = append(node.Keys, parent.Keys[parentKeyIdx])
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
		// Branch node: pull down separator key
		leftNode.Keys = append(leftNode.Keys, separatorKey)
		leftNode.Keys = append(leftNode.Keys, rightNode.Keys...)

		// Always clear Values for branch nodes (defense against corruption)
		leftNode.Values = nil

		// Merge children pointers
		leftNode.Children = append(leftNode.Children, rightNode.Children...)
	} else {
		// Leaf node: no separator, merge keys and values
		leftNode.Keys = append(leftNode.Keys, rightNode.Keys...)
		leftNode.Values = append(leftNode.Values, rightNode.Values...)
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
	leftKeys := make([][]byte, sp.LeftCount)
	copy(leftKeys, node.Keys[:sp.LeftCount])
	node.Keys = leftKeys

	if node.IsLeaf() {
		leftVals := make([][]byte, sp.LeftCount)
		copy(leftVals, node.Values[:sp.LeftCount])
		node.Values = leftVals

		// Clean up OverflowChains map - remove entries for indices that were moved to right node
		if node.OverflowChains != nil {
			for i := range node.OverflowChains {
				if i >= sp.LeftCount {
					delete(node.OverflowChains, i)
				}
			}
		}
	} else {
		node.Values = nil
		leftChildren := make([]base.PageID, sp.Mid+1)
		copy(leftChildren, node.Children[:sp.Mid+1])
		node.Children = leftChildren
	}

	node.NumKeys = uint16(sp.LeftCount)
	node.Dirty = true
}

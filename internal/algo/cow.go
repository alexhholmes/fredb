package algo

import (
	"github.com/alexhholmes/fredb/internal/base"
)

// ApplyLeafUpdate updates a key's value in leaf node
// Assumes node is already writable (COW'd by caller)
func ApplyLeafUpdate(node *base.LeafPage, pos int, newValue []byte) {
	// Deep copy the new value to prevent aliasing
	valCopy := make([]byte, len(newValue))
	copy(valCopy, newValue)
	node.Values[pos] = valCopy
	node.SetDirty(true)
}

// ApplyLeafInsert inserts new key-value at position
// Assumes node is already writable and has space
func ApplyLeafInsert(node *base.LeafPage, pos int, key, value []byte) {
	node.Keys = InsertAt(node.Keys, pos, key)
	node.Values = InsertAt(node.Values, pos, value)
	node.Header.NumKeys++
	node.SetDirty(true)
}

// ApplyLeafDelete removes key at position
// Assumes node is already writable
func ApplyLeafDelete(node *base.LeafPage, idx int) {
	node.Keys = RemoveAt(node.Keys, idx)
	node.Values = RemoveAt(node.Values, idx)
	node.Header.NumKeys--
	node.SetDirty(true)
}

// ApplyBranchRemoveSeparator removes separator key and child after merge
// Removes the separator at sepIdx and the child at sepIdx+1
// Assumes node is already writable
func ApplyBranchRemoveSeparator(node *base.BranchPage, sepIdx int) {
	node.Keys = RemoveAt(node.Keys, sepIdx)
	children := node.Children()
	newChildren := RemoveChildAt(children, sepIdx+1)
	node.FirstChild = newChildren[0]
	node.Elements = make([]base.BranchElement, len(newChildren)-1)
	for i := 1; i < len(newChildren); i++ {
		node.Elements[i-1].ChildID = newChildren[i]
	}
	node.Header.NumKeys--
	node.SetDirty(true)
}

// BorrowFromLeftLeaf moves last element from left sibling to beginning of right node (leaf version)
// Updates parent separator key
// Assumes all nodes are already writable
func BorrowFromLeftLeaf(node, leftSibling *base.LeafPage, parent *base.BranchPage, parentKeyIdx int) {
	// Extract last key/value from left sibling
	lastIdx := int(leftSibling.Header.NumKeys) - 1
	borrowed := leftSibling.Keys[lastIdx]
	borrowedVal := leftSibling.Values[lastIdx]

	// Insert at beginning of node
	node.Keys = InsertAt(node.Keys, 0, borrowed)
	node.Values = InsertAt(node.Values, 0, borrowedVal)
	node.Header.NumKeys++

	// Remove borrowed key from left sibling
	leftSibling.Keys = RemoveAt(leftSibling.Keys, lastIdx)
	leftSibling.Values = RemoveAt(leftSibling.Values, lastIdx)
	leftSibling.Header.NumKeys--

	// Update parent separator to be the first key of right node
	parent.Keys[parentKeyIdx] = node.Keys[0]

	// Mark all as dirty
	node.SetDirty(true)
	leftSibling.SetDirty(true)
	parent.SetDirty(true)
}

// BorrowFromLeftBranch moves last element from left sibling to beginning of right node (branch version)
// Updates parent separator key
// Assumes all nodes are already writable
func BorrowFromLeftBranch(node, leftSibling *base.BranchPage, parent *base.BranchPage, parentKeyIdx int) {
	// Extract last key and child from left sibling
	lastIdx := int(leftSibling.Header.NumKeys) - 1
	borrowed := leftSibling.Keys[lastIdx]
	leftChildren := leftSibling.Children()
	borrowedChild := leftChildren[len(leftChildren)-1]

	// Move parent key to node (at beginning)
	node.Keys = InsertAt(node.Keys, 0, parent.Keys[parentKeyIdx])
	nodeChildren := node.Children()
	newChildren := append([]base.PageID{borrowedChild}, nodeChildren...)
	node.FirstChild = newChildren[0]
	node.Elements = make([]base.BranchElement, len(newChildren)-1)
	for i := 1; i < len(newChildren); i++ {
		node.Elements[i-1].ChildID = newChildren[i]
	}
	node.Header.NumKeys++

	// Remove borrowed key and child from left sibling
	leftSibling.Keys = RemoveAt(leftSibling.Keys, lastIdx)
	newLeftChildren := leftChildren[:len(leftChildren)-1]
	leftSibling.FirstChild = newLeftChildren[0]
	leftSibling.Elements = make([]base.BranchElement, len(newLeftChildren)-1)
	for i := 1; i < len(newLeftChildren); i++ {
		leftSibling.Elements[i-1].ChildID = newLeftChildren[i]
	}
	leftSibling.Header.NumKeys--

	// Move last key from left sibling to parent
	parent.Keys[parentKeyIdx] = borrowed

	// Mark all as dirty
	node.SetDirty(true)
	leftSibling.SetDirty(true)
	parent.SetDirty(true)
}

// BorrowFromRightLeaf moves first element from right sibling to end of left node (leaf version)
// Updates parent separator key
// Assumes all nodes are already writable
func BorrowFromRightLeaf(node, rightSibling *base.LeafPage, parent *base.BranchPage, parentKeyIdx int) {
	// Extract first key/value from right sibling
	borrowed := rightSibling.Keys[0]
	borrowedVal := rightSibling.Values[0]

	// Deep copy and append to end of node
	keyCopy := make([]byte, len(borrowed))
	copy(keyCopy, borrowed)
	valCopy := make([]byte, len(borrowedVal))
	copy(valCopy, borrowedVal)
	node.Keys = append(node.Keys, keyCopy)
	node.Values = append(node.Values, valCopy)
	node.Header.NumKeys++

	// Remove borrowed key from right sibling
	rightSibling.Keys = RemoveAt(rightSibling.Keys, 0)
	rightSibling.Values = RemoveAt(rightSibling.Values, 0)
	rightSibling.Header.NumKeys--

	// Update parent separator to be the first key of right sibling
	parent.Keys[parentKeyIdx] = rightSibling.Keys[0]

	// Mark all as dirty
	node.SetDirty(true)
	rightSibling.SetDirty(true)
	parent.SetDirty(true)
}

// BorrowFromRightBranch moves first element from right sibling to end of left node (branch version)
// Updates parent separator key
// Assumes all nodes are already writable
func BorrowFromRightBranch(node, rightSibling *base.BranchPage, parent *base.BranchPage, parentKeyIdx int) {
	// Extract first key and child from right sibling
	borrowed := rightSibling.Keys[0]
	rightChildren := rightSibling.Children()
	borrowedChild := rightChildren[0]

	// Deep copy parent key and append to node
	keyCopy := make([]byte, len(parent.Keys[parentKeyIdx]))
	copy(keyCopy, parent.Keys[parentKeyIdx])
	node.Keys = append(node.Keys, keyCopy)
	nodeChildren := node.Children()
	newChildren := append(nodeChildren, borrowedChild)
	node.FirstChild = newChildren[0]
	node.Elements = make([]base.BranchElement, len(newChildren)-1)
	for i := 1; i < len(newChildren); i++ {
		node.Elements[i-1].ChildID = newChildren[i]
	}
	node.Header.NumKeys++

	// Remove borrowed key and child from right sibling
	rightSibling.Keys = RemoveAt(rightSibling.Keys, 0)
	newRightChildren := rightChildren[1:]
	rightSibling.FirstChild = newRightChildren[0]
	rightSibling.Elements = make([]base.BranchElement, len(newRightChildren)-1)
	for i := 1; i < len(newRightChildren); i++ {
		rightSibling.Elements[i-1].ChildID = newRightChildren[i]
	}
	rightSibling.Header.NumKeys--

	// Move first key from right sibling to parent
	parent.Keys[parentKeyIdx] = borrowed

	// Mark all as dirty
	node.SetDirty(true)
	rightSibling.SetDirty(true)
	parent.SetDirty(true)
}

// MergeNodesLeaf combines right leaf into left leaf
// Assumes left node is already writable
// Does NOT update parent - caller must call ApplyBranchRemoveSeparator
func MergeNodesLeaf(leftNode, rightNode *base.LeafPage, separatorKey []byte) {
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

	// Update left node's key count
	leftNode.Header.NumKeys = uint16(len(leftNode.Keys))
	leftNode.SetDirty(true)
}

// MergeNodesBranch combines right branch into left branch
// Assumes left node is already writable
// Does NOT update parent - caller must call ApplyBranchRemoveSeparator
func MergeNodesBranch(leftNode, rightNode *base.BranchPage, separatorKey []byte) {
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

	// Merge children pointers (PageIDs are copy-by-value)
	leftChildren := leftNode.Children()
	rightChildren := rightNode.Children()
	newChildren := append(leftChildren, rightChildren...)
	leftNode.FirstChild = newChildren[0]
	leftNode.Elements = make([]base.BranchElement, len(newChildren)-1)
	for i := 1; i < len(newChildren); i++ {
		leftNode.Elements[i-1].ChildID = newChildren[i]
	}

	// Update left node's key count
	leftNode.Header.NumKeys = uint16(len(leftNode.Keys))
	leftNode.SetDirty(true)
}

// NewBranchRoot creates a new branch root node from two children after split
func NewBranchRoot(leftChild, rightChild base.PageData, midKey []byte, pageID base.PageID) base.PageData {
	branch := base.NewBranchPage()
	branch.SetPageID(pageID)
	branch.SetDirty(true)
	branch.Header.NumKeys = 1
	branch.Keys = [][]byte{midKey}
	branch.FirstChild = leftChild.GetPageID()

	// Set the right child in Elements[0]
	branch.Elements = make([]base.BranchElement, 1)
	branch.Elements[0].ChildID = rightChild.GetPageID()

	return branch
}

// ApplyChildSplit updates parent after splitting child at childIdx
// Inserts separator key and updates children pointers
// Assumes parent is already writable (COW'd by caller)
func ApplyChildSplit(parent *base.BranchPage, childIdx int, leftChild, rightChild base.PageData, midKey, midVal []byte) {
	// Insert middle key into parent
	parent.Keys = InsertAt(parent.Keys, childIdx, midKey)

	// Update children array
	oldChildren := parent.Children()
	newChildren := make([]base.PageID, len(oldChildren)+1)
	copy(newChildren[:childIdx], oldChildren[:childIdx])
	newChildren[childIdx] = leftChild.GetPageID()
	newChildren[childIdx+1] = rightChild.GetPageID()
	copy(newChildren[childIdx+2:], oldChildren[childIdx+1:])

	// Set FirstChild and Elements
	if len(newChildren) > 0 {
		parent.FirstChild = newChildren[0]
		parent.Elements = make([]base.BranchElement, len(newChildren)-1)
		for i := 1; i < len(newChildren); i++ {
			parent.Elements[i-1].ChildID = newChildren[i]
		}
	}

	parent.Header.NumKeys++
	parent.SetDirty(true)
}

// TruncateLeft modifies node to keep only left portion after split
// Assumes node is already writable (COW'd by caller)
func TruncateLeft(node base.PageData, sp SplitPoint) {
	isLeaf := node.PageType() == base.LeafPageFlag

	// Handle special case: Mid=-1 means left gets nothing (sp.LeftCount=0)
	if sp.Mid == -1 {
		if isLeaf {
			leaf := node.(*base.LeafPage)
			leaf.Keys = [][]byte{}
			leaf.Values = [][]byte{}
			leaf.Header.NumKeys = 0
			leaf.SetDirty(true)
		} else {
			branch := node.(*base.BranchPage)
			branch.Keys = [][]byte{}
			branch.FirstChild = 0
			branch.Elements = []base.BranchElement{}
			branch.Header.NumKeys = 0
			branch.SetDirty(true)
		}
		return
	}

	if isLeaf {
		leaf := node.(*base.LeafPage)
		// Deep copy keys to prevent aliasing with split sibling
		leftKeys := make([][]byte, sp.LeftCount)
		for i := 0; i < sp.LeftCount; i++ {
			keyCopy := make([]byte, len(leaf.Keys[i]))
			copy(keyCopy, leaf.Keys[i])
			leftKeys[i] = keyCopy
		}
		leaf.Keys = leftKeys

		// Deep copy values to prevent aliasing with split sibling
		leftVals := make([][]byte, sp.LeftCount)
		for i := 0; i < sp.LeftCount; i++ {
			valCopy := make([]byte, len(leaf.Values[i]))
			copy(valCopy, leaf.Values[i])
			leftVals[i] = valCopy
		}
		leaf.Values = leftVals
		leaf.Header.NumKeys = uint16(sp.LeftCount)
		leaf.SetDirty(true)
	} else {
		branch := node.(*base.BranchPage)
		// Deep copy keys to prevent aliasing with split sibling
		leftKeys := make([][]byte, sp.LeftCount)
		for i := 0; i < sp.LeftCount; i++ {
			keyCopy := make([]byte, len(branch.Keys[i]))
			copy(keyCopy, branch.Keys[i])
			leftKeys[i] = keyCopy
		}
		branch.Keys = leftKeys

		// Truncate children
		nodeChildren := branch.Children()
		leftChildren := make([]base.PageID, sp.Mid+1)
		copy(leftChildren, nodeChildren[:sp.Mid+1])

		// Set FirstChild and Elements
		if len(leftChildren) > 0 {
			branch.FirstChild = leftChildren[0]
			branch.Elements = make([]base.BranchElement, len(leftChildren)-1)
			for i := 1; i < len(leftChildren); i++ {
				branch.Elements[i-1].ChildID = leftChildren[i]
			}
		}

		branch.Header.NumKeys = uint16(sp.LeftCount)
		branch.SetDirty(true)
	}
}

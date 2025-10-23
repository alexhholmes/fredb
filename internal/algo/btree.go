// Package algo contains algorithms used for traversing and editing a b+ tree.
package algo

import (
	"bytes"
	"sort"

	"github.com/alexhholmes/fredb/internal/base"
)

const searchThreshold = 32

// FindChildIndex returns the index of child pointer to follow for key
func FindChildIndex(node base.PageData, key []byte) int {
	var keys [][]byte
	var numKeys uint16

	if node.PageType() == base.LeafPageFlag {
		leaf := node.(*base.LeafPage)
		keys = leaf.Keys
		numKeys = leaf.Header.NumKeys
	} else {
		branch := node.(*base.BranchPage)
		keys = branch.Keys
		numKeys = branch.Header.NumKeys
	}

	if numKeys < searchThreshold {
		i := 0
		for i < int(numKeys) && bytes.Compare(key, keys[i]) >= 0 {
			i++
		}
		return i
	}

	return sort.Search(int(numKeys), func(i int) bool {
		return bytes.Compare(key, keys[i]) < 0
	})
}

// FindKeyInLeaf returns index of key in leaf, or -1 if not found
func FindKeyInLeaf(node base.PageData, key []byte) int {
	if node.PageType() != base.LeafPageFlag {
		return -1
	}

	leaf := node.(*base.LeafPage)
	if leaf.Header.NumKeys < searchThreshold {
		for i := 0; i < int(leaf.Header.NumKeys); i++ {
			if bytes.Equal(key, leaf.Keys[i]) {
				return i
			}
		}
		return -1
	}

	idx := sort.Search(int(leaf.Header.NumKeys), func(i int) bool {
		return bytes.Compare(leaf.Keys[i], key) >= 0
	})
	if idx < int(leaf.Header.NumKeys) && bytes.Equal(leaf.Keys[idx], key) {
		return idx
	}
	return -1
}

// FindInsertPosition returns position to insert key in leaf
func FindInsertPosition(node base.PageData, key []byte) int {
	var keys [][]byte
	var numKeys uint16

	if node.PageType() == base.LeafPageFlag {
		leaf := node.(*base.LeafPage)
		keys = leaf.Keys
		numKeys = leaf.Header.NumKeys
	} else {
		branch := node.(*base.BranchPage)
		keys = branch.Keys
		numKeys = branch.Header.NumKeys
	}

	if numKeys < searchThreshold {
		pos := 0
		for pos < int(numKeys) && bytes.Compare(key, keys[pos]) > 0 {
			pos++
		}
		return pos
	}

	return sort.Search(int(numKeys), func(i int) bool {
		return bytes.Compare(key, keys[i]) <= 0
	})
}

// FindDeleteChildIndex returns child index for deletion in branch node
func FindDeleteChildIndex(node base.PageData, key []byte) int {
	var keys [][]byte
	var numKeys uint16

	if node.PageType() == base.LeafPageFlag {
		leaf := node.(*base.LeafPage)
		keys = leaf.Keys
		numKeys = leaf.Header.NumKeys
	} else {
		branch := node.(*base.BranchPage)
		keys = branch.Keys
		numKeys = branch.Header.NumKeys
	}

	if numKeys < searchThreshold {
		idx := 0
		for idx < int(numKeys) && bytes.Compare(key, keys[idx]) >= 0 {
			idx++
		}
		return idx
	}

	return sort.Search(int(numKeys), func(i int) bool {
		return bytes.Compare(key, keys[i]) < 0
	})
}

// SplitHint guides how to bias the split point
type SplitHint int

const (
	SplitBalanced  SplitHint = iota // Default: 50/50
	SplitLeftBias                   // Left heavy: 90/10 (descending inserts)
	SplitRightBias                  // Right heavy: 10/90 (ascending inserts)
)

// SplitPoint contains split calculation results
type SplitPoint struct {
	Mid          int
	LeftCount    int
	RightCount   int
	SeparatorKey []byte
}

// CalculateSplitPointWithHint determines split position with adaptive strategy
func CalculateSplitPointWithHint(node base.PageData, insertKey []byte, hint SplitHint) SplitPoint {
	// Get keys based on type
	var keys [][]byte
	isLeaf := node.PageType() == base.LeafPageFlag

	if isLeaf {
		keys = node.(*base.LeafPage).Keys
	} else {
		keys = node.(*base.BranchPage).Keys
	}

	// Handle edge case: node with only 1 key (can happen with large values)
	if len(keys) <= 1 {
		if len(keys) == 0 {
			panic("cannot split empty node")
		}

		// Use insertKey to decide which child gets the existing entry
		// This prevents infinite loop when inserting smaller keys
		existingKey := keys[0]

		if insertKey != nil && bytes.Compare(insertKey, existingKey) < 0 {
			// New key < existing: put existing in right, leave left empty
			// Separator = existing key (right's first key after insert)
			sep := make([]byte, len(existingKey))
			copy(sep, existingKey)
			return SplitPoint{
				Mid:          -1, // Special: means left gets nothing
				LeftCount:    0,
				RightCount:   1,
				SeparatorKey: sep,
			}
		}

		// New key >= existing: put existing in left, leave right empty
		// Separator = new key (right's first key after insert)
		sep := make([]byte, len(insertKey))
		copy(sep, insertKey)
		return SplitPoint{
			Mid:          0,
			LeftCount:    1,
			RightCount:   0,
			SeparatorKey: sep,
		}
	}

	// Detect pattern if hint not provided
	if hint == SplitBalanced && insertKey != nil {
		if bytes.Compare(insertKey, keys[len(keys)-1]) > 0 {
			// Inserting beyond rightmost key → ascending pattern
			hint = SplitRightBias
		} else if bytes.Compare(insertKey, keys[0]) < 0 {
			// Inserting before leftmost key → descending pattern
			hint = SplitLeftBias
		}
	}

	var mid int
	switch hint {
	case SplitRightBias:
		// Keep left node nearly full (90%), right node minimal (10%)
		mid = int(float64(len(keys)) * 0.9)
		if mid >= len(keys)-1 {
			mid = len(keys) - 2
		}
	case SplitLeftBias:
		// Keep right node nearly full (90%), left node minimal (10%)
		mid = int(float64(len(keys)) * 0.1)
		// For leaves: need mid+1 < len(keys) to access separator
		// For branches: need mid < len(keys)
		minMid := 0
		if !isLeaf && mid < 1 {
			minMid = 1
		}
		if mid < minMid {
			mid = minMid
		}
	default:
		// Balanced split
		mid = len(keys)/2 - 1
		if mid < 0 {
			mid = 0
		}
	}

	// Final bounds check for leaf separator access
	if isLeaf && mid+1 >= len(keys) {
		mid = len(keys) - 2
		if mid < 0 {
			mid = 0
		}
	}

	var sep []byte
	var leftCnt, rightCnt int

	if isLeaf {
		sep = make([]byte, len(keys[mid+1]))
		copy(sep, keys[mid+1])
		leftCnt = mid + 1
		rightCnt = len(keys) - mid - 1
	} else {
		sep = make([]byte, len(keys[mid]))
		copy(sep, keys[mid])
		leftCnt = mid
		rightCnt = len(keys) - mid - 1
	}

	return SplitPoint{
		Mid:          mid,
		LeftCount:    leftCnt,
		RightCount:   rightCnt,
		SeparatorKey: sep,
	}
}

// ExtractRightPortion copies right portion data (read-only on input)
func ExtractRightPortion(node base.PageData, sp SplitPoint) (keys [][]byte, vals [][]byte, children []base.PageID) {
	// Handle special case: Mid=-1 means right gets everything (sp.RightCount=1 for single key)
	startIdx := sp.Mid + 1
	if sp.Mid == -1 {
		startIdx = 0
	}

	isLeaf := node.PageType() == base.LeafPageFlag

	if isLeaf {
		leaf := node.(*base.LeafPage)
		for i := startIdx; i < len(leaf.Keys); i++ {
			keyCopy := make([]byte, len(leaf.Keys[i]))
			copy(keyCopy, leaf.Keys[i])
			keys = append(keys, keyCopy)
		}

		for i := startIdx; i < len(leaf.Values); i++ {
			valCopy := make([]byte, len(leaf.Values[i]))
			copy(valCopy, leaf.Values[i])
			vals = append(vals, valCopy)
		}
	} else {
		branch := node.(*base.BranchPage)
		for i := startIdx; i < len(branch.Keys); i++ {
			keyCopy := make([]byte, len(branch.Keys[i]))
			copy(keyCopy, branch.Keys[i])
			keys = append(keys, keyCopy)
		}

		// Extract right portion of children
		nodeChildren := branch.Children()
		for i := startIdx; i < len(nodeChildren); i++ {
			children = append(children, nodeChildren[i])
		}
	}

	return keys, vals, children
}

// InsertAt inserts value at index in slice with deep copy
func InsertAt(slice [][]byte, index int, value []byte) [][]byte {
	// Deep copy the value to prevent aliasing
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	return append(slice[:index], append([][]byte{valueCopy}, slice[index:]...)...)
}

// RemoveAt removes element at index from slice
func RemoveAt(slice [][]byte, index int) [][]byte {
	return append(slice[:index], slice[index+1:]...)
}

// RemoveChildAt removes child at index from slice
func RemoveChildAt(slice []base.PageID, index int) []base.PageID {
	return append(slice[:index], slice[index+1:]...)
}

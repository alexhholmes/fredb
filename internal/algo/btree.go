// Package algo contains algorithms used for traversing and editing a b+ tree.
package algo

import (
	"bytes"
	"sort"

	"fredb/internal/base"
)

const searchThreshold = 32

// FindChildIndex returns the index of child pointer to follow for key
func FindChildIndex(node *base.Node, key []byte) int {
	if node.NumKeys < searchThreshold {
		i := 0
		for i < int(node.NumKeys) && bytes.Compare(key, node.Keys[i]) >= 0 {
			i++
		}
		return i
	}

	return sort.Search(int(node.NumKeys), func(i int) bool {
		return bytes.Compare(key, node.Keys[i]) < 0
	})
}

// FindKeyInLeaf returns index of key in leaf, or -1 if not found
func FindKeyInLeaf(node *base.Node, key []byte) int {
	if !node.IsLeaf() {
		return -1
	}

	if node.NumKeys < searchThreshold {
		for i := 0; i < int(node.NumKeys); i++ {
			if bytes.Equal(key, node.Keys[i]) {
				return i
			}
		}
		return -1
	}

	idx := sort.Search(int(node.NumKeys), func(i int) bool {
		return bytes.Compare(node.Keys[i], key) >= 0
	})
	if idx < int(node.NumKeys) && bytes.Equal(node.Keys[idx], key) {
		return idx
	}
	return -1
}

// FindInsertPosition returns position to insert key in leaf
func FindInsertPosition(node *base.Node, key []byte) int {
	if node.NumKeys < searchThreshold {
		pos := 0
		for pos < int(node.NumKeys) && bytes.Compare(key, node.Keys[pos]) > 0 {
			pos++
		}
		return pos
	}

	return sort.Search(int(node.NumKeys), func(i int) bool {
		return bytes.Compare(key, node.Keys[i]) <= 0
	})
}

// FindDeleteChildIndex returns child index for deletion in branch node
func FindDeleteChildIndex(node *base.Node, key []byte) int {
	if node.NumKeys < searchThreshold {
		idx := 0
		for idx < int(node.NumKeys) && bytes.Compare(key, node.Keys[idx]) >= 0 {
			idx++
		}
		return idx
	}

	return sort.Search(int(node.NumKeys), func(i int) bool {
		return bytes.Compare(key, node.Keys[i]) < 0
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
func CalculateSplitPointWithHint(node *base.Node, insertKey []byte, hint SplitHint) SplitPoint {
	// Handle edge case: node with only 1 key (can happen with large values)
	if len(node.Keys) <= 1 {
		if len(node.Keys) == 0 {
			panic("cannot split empty node")
		}
		// Split at first key - left gets the entry, right gets nothing
		// This way, ascending inserts go to the empty right child
		sep := make([]byte, len(node.Keys[0]))
		copy(sep, node.Keys[0])
		return SplitPoint{
			Mid:          0,
			LeftCount:    1,
			RightCount:   0,
			SeparatorKey: sep,
		}
	}

	// Detect pattern if hint not provided
	if hint == SplitBalanced && insertKey != nil {
		if bytes.Compare(insertKey, node.Keys[len(node.Keys)-1]) > 0 {
			// Inserting beyond rightmost key → ascending pattern
			hint = SplitRightBias
		} else if bytes.Compare(insertKey, node.Keys[0]) < 0 {
			// Inserting before leftmost key → descending pattern
			hint = SplitLeftBias
		}
	}

	var mid int
	switch hint {
	case SplitRightBias:
		// Keep left node nearly full (90%), right node minimal (10%)
		mid = int(float64(len(node.Keys)) * 0.9)
		if mid >= len(node.Keys)-1 {
			mid = len(node.Keys) - 2
		}
	case SplitLeftBias:
		// Keep right node nearly full (90%), left node minimal (10%)
		mid = int(float64(len(node.Keys)) * 0.1)
		if mid < 1 {
			mid = 1
		}
	default:
		// Balanced split
		mid = len(node.Keys)/2 - 1
		if mid < 0 {
			mid = 0
		}
	}

	var sep []byte
	var leftCnt, rightCnt int

	if node.IsLeaf() {
		sep = make([]byte, len(node.Keys[mid+1]))
		copy(sep, node.Keys[mid+1])
		leftCnt = mid + 1
		rightCnt = len(node.Keys) - mid - 1
	} else {
		sep = make([]byte, len(node.Keys[mid]))
		copy(sep, node.Keys[mid])
		leftCnt = mid
		rightCnt = len(node.Keys) - mid - 1
	}

	return SplitPoint{
		Mid:          mid,
		LeftCount:    leftCnt,
		RightCount:   rightCnt,
		SeparatorKey: sep,
	}
}

// ExtractRightPortion copies right portion data (read-only on input)
func ExtractRightPortion(node *base.Node, sp SplitPoint) (keys [][]byte, vals [][]byte, children []base.PageID) {
	keys = make([][]byte, 0, sp.RightCount)
	for i := sp.Mid + 1; i < len(node.Keys); i++ {
		keyCopy := make([]byte, len(node.Keys[i]))
		copy(keyCopy, node.Keys[i])
		keys = append(keys, keyCopy)
	}

	if node.IsLeaf() {
		vals = make([][]byte, 0, sp.RightCount)
		for i := sp.Mid + 1; i < len(node.Values); i++ {
			valCopy := make([]byte, len(node.Values[i]))
			copy(valCopy, node.Values[i])
			vals = append(vals, valCopy)
		}
	}

	if !node.IsLeaf() {
		children = make([]base.PageID, 0)
		for i := sp.Mid + 1; i < len(node.Children); i++ {
			children = append(children, node.Children[i])
		}
	}

	return keys, vals, children
}

// CanBorrowFrom returns true if node has extra keys to lend
func CanBorrowFrom(node *base.Node) bool {
	return node.NumKeys > base.MinKeysPerNode
}

// BorrowData contains data borrowed from sibling
type BorrowData struct {
	Key   []byte
	Value []byte
	Child base.PageID
}

// ExtractLastFromSibling gets last key/val/child from sibling (read-only)
func ExtractLastFromSibling(sibling *base.Node) BorrowData {
	lastIdx := sibling.NumKeys - 1
	data := BorrowData{
		Key: sibling.Keys[lastIdx],
	}
	if sibling.IsLeaf() {
		data.Value = sibling.Values[lastIdx]
	} else {
		data.Child = sibling.Children[len(sibling.Children)-1]
	}
	return data
}

// ExtractFirstFromSibling gets first key/val/child from sibling (read-only)
func ExtractFirstFromSibling(sibling *base.Node) BorrowData {
	data := BorrowData{
		Key: sibling.Keys[0],
	}
	if sibling.IsLeaf() {
		data.Value = sibling.Values[0]
	} else {
		data.Child = sibling.Children[0]
	}
	return data
}

// InsertAt inserts value at index in slice
func InsertAt(slice [][]byte, index int, value []byte) [][]byte {
	return append(slice[:index], append([][]byte{value}, slice[index:]...)...)
}

// RemoveAt removes element at index from slice
func RemoveAt(slice [][]byte, index int) [][]byte {
	return append(slice[:index], slice[index+1:]...)
}

// RemoveChildAt removes child at index from slice
func RemoveChildAt(slice []base.PageID, index int) []base.PageID {
	return append(slice[:index], slice[index+1:]...)
}

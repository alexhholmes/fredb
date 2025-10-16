package algo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fredb/internal/base"
)

// Helper: compare byte slice arrays
func equalByteSlices(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if a[i][j] != b[i][j] {
				return false
			}
		}
	}
	return true
}

// Helper: create leaf node for testing
func newLeafNode(keys, values [][]byte) *base.Node {
	return &base.Node{
		PageID:  1,
		Dirty:   false,
		Leaf:    true,
		NumKeys: uint16(len(keys)),
		Keys:    keys,
		Values:  values,
	}
}

// Helper: create branch node for testing
func newBranchNode(keys [][]byte, children []base.PageID) *base.Node {
	return &base.Node{
		NumKeys:  uint16(len(keys)),
		Keys:     keys,
		Children: children,
		Values:   nil,
		Dirty:    false,
		Leaf:     false,
	}
}

// ApplyLeafUpdate Tests

func TestApplyLeafUpdate_BasicUpdate(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafUpdate(node, 1, []byte("v2-updated"))

	assert.Equal(t, []byte("v2-updated"), node.Values[1], "Values[1] should be updated")
	assert.Equal(t, []byte("v1"), node.Values[0], "Values[0] should remain unchanged")
	assert.Equal(t, []byte("v3"), node.Values[2], "Values[2] should remain unchanged")
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.Equal(t, uint16(3), node.NumKeys, "NumKeys should remain 3")
	expectedKeys := [][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should remain unchanged")
}

func TestApplyLeafUpdate_FirstPosition(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("key1"), []byte("key2")},
		[][]byte{[]byte("val1"), []byte("val2")},
	)

	ApplyLeafUpdate(node, 0, []byte("new-val1"))

	assert.Equal(t, []byte("new-val1"), node.Values[0], "Values[0] should be updated")
	assert.Equal(t, []byte("val2"), node.Values[1], "Values[1] should remain unchanged")
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should remain 2")
}

func TestApplyLeafUpdate_LastPosition(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafUpdate(node, 2, []byte("v3-new"))

	assert.Equal(t, []byte("v3-new"), node.Values[2], "Values[2] should be updated")
	assert.Equal(t, []byte("v1"), node.Values[0], "Values[0] should remain unchanged")
	assert.Equal(t, []byte("v2"), node.Values[1], "Values[1] should remain unchanged")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafUpdate_EmptyValue(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("key")},
		[][]byte{[]byte("original")},
	)

	ApplyLeafUpdate(node, 0, []byte(""))

	assert.Equal(t, []byte(""), node.Values[0], "Values[0] should be empty")
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.Equal(t, uint16(1), node.NumKeys, "NumKeys should remain 1")
}

// ApplyLeafInsert Tests

func TestApplyLeafInsert_Middle(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v3")},
	)

	ApplyLeafInsert(node, 1, []byte("b"), []byte("v2"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a', 'b', 'c']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v2', 'v3']")
	assert.Equal(t, uint16(3), node.NumKeys, "NumKeys should be 3")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafInsert_Beginning(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("b"), []byte("c")},
		[][]byte{[]byte("v2"), []byte("v3")},
	)

	ApplyLeafInsert(node, 0, []byte("a"), []byte("v1"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a', 'b', 'c']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v2', 'v3']")
	assert.Equal(t, uint16(3), node.NumKeys, "NumKeys should be 3")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafInsert_End(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)

	ApplyLeafInsert(node, 2, []byte("c"), []byte("v3"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a', 'b', 'c']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v2', 'v3']")
	assert.Equal(t, uint16(3), node.NumKeys, "NumKeys should be 3")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafInsert_EmptyNode(t *testing.T) {
	node := newLeafNode(
		[][]byte{},
		[][]byte{},
	)

	ApplyLeafInsert(node, 0, []byte("first"), []byte("value1"))

	expectedKeys := [][]byte{[]byte("first")}
	expectedValues := [][]byte{[]byte("value1")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['first']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['value1']")
	assert.Equal(t, uint16(1), node.NumKeys, "NumKeys should be 1")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafInsert_SingleElement(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)

	ApplyLeafInsert(node, 1, []byte("b"), []byte("v2"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a', 'b']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v2']")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

// ApplyLeafDelete Tests

func TestApplyLeafDelete_Middle(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafDelete(node, 1)

	expectedKeys := [][]byte{[]byte("a"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v3")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a', 'c']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v3']")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafDelete_First(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafDelete(node, 0)

	expectedKeys := [][]byte{[]byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v2"), []byte("v3")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['b', 'c']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v2', 'v3']")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafDelete_Last(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafDelete(node, 2)

	expectedKeys := [][]byte{[]byte("a"), []byte("b")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a', 'b']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v2']")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafDelete_OnlyElement(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)

	ApplyLeafDelete(node, 0)

	expectedKeys := [][]byte{}
	expectedValues := [][]byte{}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be empty")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be empty")
	assert.Equal(t, uint16(0), node.NumKeys, "NumKeys should be 0")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyLeafDelete_DownToOne(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)

	ApplyLeafDelete(node, 1)

	expectedKeys := [][]byte{[]byte("a")}
	expectedValues := [][]byte{[]byte("v1")}

	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['a']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1']")
	assert.Equal(t, uint16(1), node.NumKeys, "NumKeys should be 1")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

// ApplyBranchRemoveSeparator Tests

func TestApplyBranchRemoveSeparator_BranchNode_Middle(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[]base.PageID{1, 2, 3, 4},
	)

	ApplyBranchRemoveSeparator(node, 1)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k3")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['k1', 'k3']")
	assert.Nil(t, node.Values, "values should be nil for branch node")

	expectedChildren := []base.PageID{1, 2, 4}
	require.Equal(t, len(expectedChildren), len(node.Children), "children length should match")
	assert.Equal(t, expectedChildren, node.Children, "children should match expected")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyBranchRemoveSeparator_BranchNode_First(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{10, 20, 30},
	)

	ApplyBranchRemoveSeparator(node, 0)

	expectedKeys := [][]byte{[]byte("k2")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['k2']")

	expectedChildren := []base.PageID{10, 30}
	require.Equal(t, len(expectedChildren), len(node.Children), "children length should match")
	assert.Equal(t, expectedChildren, node.Children, "children should match expected")
	assert.Equal(t, uint16(1), node.NumKeys, "NumKeys should be 1")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyBranchRemoveSeparator_BranchNode_Last(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[]base.PageID{1, 2, 3, 4},
	)

	ApplyBranchRemoveSeparator(node, 2)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['k1', 'k2']")

	expectedChildren := []base.PageID{1, 2, 3}
	require.Equal(t, len(expectedChildren), len(node.Children), "children length should match")
	assert.Equal(t, expectedChildren, node.Children, "children should match expected")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyBranchRemoveSeparator_LeafNode(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)
	node.Children = []base.PageID{1, 2, 3, 4}

	ApplyBranchRemoveSeparator(node, 1)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k3")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['k1', 'k3']")

	expectedValues := [][]byte{[]byte("v1"), []byte("v3")}
	assert.True(t, equalByteSlices(node.Values, expectedValues), "values should be ['v1', 'v3']")

	expectedChildren := []base.PageID{1, 2, 4}
	require.Equal(t, len(expectedChildren), len(node.Children), "children length should match")
	assert.Equal(t, expectedChildren, node.Children, "children should match expected")
	assert.Equal(t, uint16(2), node.NumKeys, "NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestApplyBranchRemoveSeparator_DownToOneKey(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{1, 2, 3},
	)

	ApplyBranchRemoveSeparator(node, 0)

	expectedKeys := [][]byte{[]byte("k2")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "keys should be ['k2']")

	expectedChildren := []base.PageID{1, 3}
	require.Equal(t, len(expectedChildren), len(node.Children), "children length should match")
	assert.Equal(t, expectedChildren, node.Children, "children should match expected")
	assert.Equal(t, uint16(1), node.NumKeys, "NumKeys should be 1")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

// BorrowFromLeft Tests

func TestBorrowFromLeft_LeafNodes(t *testing.T) {
	leftSibling := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	node := newLeafNode(
		[][]byte{[]byte("e"), []byte("f")},
		[][]byte{[]byte("v5"), []byte("v6")},
	)

	parent := newBranchNode(
		[][]byte{[]byte("e")},
		[]base.PageID{1, 2},
	)

	BorrowFromLeft(node, leftSibling, parent, 0)

	// Verify node
	expectedNodeKeys := [][]byte{[]byte("c"), []byte("e"), []byte("f")}
	assert.True(t, equalByteSlices(node.Keys, expectedNodeKeys), "node keys should be ['c', 'e', 'f']")

	expectedNodeValues := [][]byte{[]byte("v3"), []byte("v5"), []byte("v6")}
	assert.True(t, equalByteSlices(node.Values, expectedNodeValues), "node values should be ['v3', 'v5', 'v6']")
	assert.Equal(t, uint16(3), node.NumKeys, "node NumKeys should be 3")

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("a"), []byte("b")}
	assert.True(t, equalByteSlices(leftSibling.Keys, expectedLeftKeys), "leftSibling keys should be ['a', 'b']")

	expectedLeftValues := [][]byte{[]byte("v1"), []byte("v2")}
	assert.True(t, equalByteSlices(leftSibling.Values, expectedLeftValues), "leftSibling values should be ['v1', 'v2']")
	assert.Equal(t, uint16(2), leftSibling.NumKeys, "leftSibling NumKeys should be 2")

	// Verify parent
	assert.Equal(t, []byte("c"), parent.Keys[0], "parent separator key should be 'c'")

	// Verify all dirty
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, leftSibling.Dirty, "leftSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromLeft_LeafNodes_SingleElementRight(t *testing.T) {
	leftSibling := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)

	node := newLeafNode(
		[][]byte{[]byte("d")},
		[][]byte{[]byte("v4")},
	)

	parent := newBranchNode(
		[][]byte{[]byte("d")},
		[]base.PageID{1, 2},
	)

	BorrowFromLeft(node, leftSibling, parent, 0)

	// Verify node
	expectedNodeKeys := [][]byte{[]byte("b"), []byte("d")}
	assert.True(t, equalByteSlices(node.Keys, expectedNodeKeys), "node keys should be ['b', 'd']")

	expectedNodeValues := [][]byte{[]byte("v2"), []byte("v4")}
	assert.True(t, equalByteSlices(node.Values, expectedNodeValues), "node values should be ['v2', 'v4']")
	assert.Equal(t, uint16(2), node.NumKeys, "node NumKeys should be 2")

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("a")}
	assert.True(t, equalByteSlices(leftSibling.Keys, expectedLeftKeys), "leftSibling keys should be ['a']")
	assert.Equal(t, uint16(1), leftSibling.NumKeys, "leftSibling NumKeys should be 1")

	// Verify parent
	assert.Equal(t, []byte("b"), parent.Keys[0], "parent separator key should be 'b'")

	// Verify all dirty
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, leftSibling.Dirty, "leftSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromLeft_BranchNodes(t *testing.T) {
	leftSibling := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[]base.PageID{1, 2, 3, 4},
	)

	node := newBranchNode(
		[][]byte{[]byte("k6"), []byte("k7")},
		[]base.PageID{10, 11, 12},
	)

	parent := newBranchNode(
		[][]byte{[]byte("k5")},
		[]base.PageID{1, 2},
	)

	BorrowFromLeft(node, leftSibling, parent, 0)

	// Verify node
	expectedNodeKeys := [][]byte{[]byte("k5"), []byte("k6"), []byte("k7")}
	assert.True(t, equalByteSlices(node.Keys, expectedNodeKeys), "node keys should be ['k5', 'k6', 'k7']")

	expectedNodeChildren := []base.PageID{4, 10, 11, 12}
	require.Equal(t, len(expectedNodeChildren), len(node.Children), "node children length should match")
	assert.Equal(t, expectedNodeChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(3), node.NumKeys, "node NumKeys should be 3")

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("k1"), []byte("k2")}
	assert.True(t, equalByteSlices(leftSibling.Keys, expectedLeftKeys), "leftSibling keys should be ['k1', 'k2']")

	expectedLeftChildren := []base.PageID{1, 2, 3}
	require.Equal(t, len(expectedLeftChildren), len(leftSibling.Children), "leftSibling children length should match")
	assert.Equal(t, expectedLeftChildren, leftSibling.Children, "leftSibling children should match expected")
	assert.Equal(t, uint16(2), leftSibling.NumKeys, "leftSibling NumKeys should be 2")

	// Verify parent
	assert.Equal(t, []byte("k3"), parent.Keys[0], "parent separator key should be 'k3'")

	// Verify all dirty
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, leftSibling.Dirty, "leftSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromLeft_BranchNodes_SingleElementRight(t *testing.T) {
	leftSibling := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{1, 2, 3},
	)

	node := newBranchNode(
		[][]byte{[]byte("k5")},
		[]base.PageID{10, 11},
	)

	parent := newBranchNode(
		[][]byte{[]byte("k4")},
		[]base.PageID{1, 2},
	)

	BorrowFromLeft(node, leftSibling, parent, 0)

	// Verify node
	expectedNodeKeys := [][]byte{[]byte("k4"), []byte("k5")}
	assert.True(t, equalByteSlices(node.Keys, expectedNodeKeys), "node keys should be ['k4', 'k5']")

	expectedNodeChildren := []base.PageID{3, 10, 11}
	require.Equal(t, len(expectedNodeChildren), len(node.Children), "node children length should match")
	assert.Equal(t, expectedNodeChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(2), node.NumKeys, "node NumKeys should be 2")

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("k1")}
	assert.True(t, equalByteSlices(leftSibling.Keys, expectedLeftKeys), "leftSibling keys should be ['k1']")

	expectedLeftChildren := []base.PageID{1, 2}
	require.Equal(t, len(expectedLeftChildren), len(leftSibling.Children), "leftSibling children length should match")
	assert.Equal(t, expectedLeftChildren, leftSibling.Children, "leftSibling children should match expected")
	assert.Equal(t, uint16(1), leftSibling.NumKeys, "leftSibling NumKeys should be 1")

	// Verify parent
	assert.Equal(t, []byte("k2"), parent.Keys[0], "parent separator key should be 'k2'")

	// Verify all dirty
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, leftSibling.Dirty, "leftSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromLeft_ParentKeyIdx_NonZero(t *testing.T) {
	leftSibling := newLeafNode(
		[][]byte{[]byte("m"), []byte("n")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)

	node := newLeafNode(
		[][]byte{[]byte("p")},
		[][]byte{[]byte("v3")},
	)

	parent := newBranchNode(
		[][]byte{[]byte("e"), []byte("p"), []byte("z")},
		[]base.PageID{1, 2, 3, 4},
	)

	BorrowFromLeft(node, leftSibling, parent, 1)

	// Verify parent key at index 1 is updated
	assert.Equal(t, []byte("n"), parent.Keys[1], "parent.Keys[1] should be 'n'")

	// Verify other parent keys are unchanged
	assert.Equal(t, []byte("e"), parent.Keys[0], "parent.Keys[0] should remain 'e'")
	assert.Equal(t, []byte("z"), parent.Keys[2], "parent.Keys[2] should remain 'z'")
}

// BorrowFromRight Tests

func TestBorrowFromRight_LeafNodes(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)
	rightSibling := newLeafNode(
		[][]byte{[]byte("e"), []byte("f"), []byte("g")},
		[][]byte{[]byte("v5"), []byte("v6"), []byte("v7")},
	)
	parent := newBranchNode(
		[][]byte{[]byte("e")},
		[]base.PageID{1, 2},
	)

	BorrowFromRight(node, rightSibling, parent, 0)

	// Verify node
	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("e")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v5")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['a', 'b', 'e']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "node values should be ['v1', 'v2', 'v5']")
	assert.Equal(t, uint16(3), node.NumKeys, "node NumKeys should be 3")

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("f"), []byte("g")}
	expectedRightValues := [][]byte{[]byte("v6"), []byte("v7")}
	assert.True(t, equalByteSlices(rightSibling.Keys, expectedRightKeys), "rightSibling keys should be ['f', 'g']")
	assert.True(t, equalByteSlices(rightSibling.Values, expectedRightValues), "rightSibling values should be ['v6', 'v7']")
	assert.Equal(t, uint16(2), rightSibling.NumKeys, "rightSibling NumKeys should be 2")

	// Verify parent
	assert.Equal(t, []byte("f"), parent.Keys[0], "parent separator key should be 'f'")

	// Verify dirty flags
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, rightSibling.Dirty, "rightSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromRight_LeafNodes_SingleElementLeft(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)
	rightSibling := newLeafNode(
		[][]byte{[]byte("c"), []byte("d")},
		[][]byte{[]byte("v3"), []byte("v4")},
	)
	parent := newBranchNode(
		[][]byte{[]byte("c")},
		[]base.PageID{1, 2},
	)

	BorrowFromRight(node, rightSibling, parent, 0)

	// Verify node
	expectedKeys := [][]byte{[]byte("a"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v3")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['a', 'c']")
	assert.True(t, equalByteSlices(node.Values, expectedValues), "node values should be ['v1', 'v3']")
	assert.Equal(t, uint16(2), node.NumKeys, "node NumKeys should be 2")

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("d")}
	assert.True(t, equalByteSlices(rightSibling.Keys, expectedRightKeys), "rightSibling keys should be ['d']")
	assert.Equal(t, uint16(1), rightSibling.NumKeys, "rightSibling NumKeys should be 1")

	// Verify parent
	assert.Equal(t, []byte("d"), parent.Keys[0], "parent separator key should be 'd'")

	// Verify dirty flags
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, rightSibling.Dirty, "rightSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromRight_BranchNodes(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{1, 2, 3},
	)
	rightSibling := newBranchNode(
		[][]byte{[]byte("k6"), []byte("k7"), []byte("k8")},
		[]base.PageID{10, 11, 12, 13},
	)
	parent := newBranchNode(
		[][]byte{[]byte("k5")},
		[]base.PageID{100, 101},
	)

	BorrowFromRight(node, rightSibling, parent, 0)

	// Verify node
	expectedKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k5")}
	expectedChildren := []base.PageID{1, 2, 3, 10}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['k1', 'k2', 'k5']")
	require.Equal(t, len(expectedChildren), len(node.Children), "node children length should match")
	assert.Equal(t, expectedChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(3), node.NumKeys, "node NumKeys should be 3")

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("k7"), []byte("k8")}
	expectedRightChildren := []base.PageID{11, 12, 13}
	assert.True(t, equalByteSlices(rightSibling.Keys, expectedRightKeys), "rightSibling keys should be ['k7', 'k8']")
	require.Equal(t, len(expectedRightChildren), len(rightSibling.Children), "rightSibling children length should match")
	assert.Equal(t, expectedRightChildren, rightSibling.Children, "rightSibling children should match expected")
	assert.Equal(t, uint16(2), rightSibling.NumKeys, "rightSibling NumKeys should be 2")

	// Verify parent
	assert.Equal(t, []byte("k6"), parent.Keys[0], "parent separator key should be 'k6'")

	// Verify dirty flags
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, rightSibling.Dirty, "rightSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromRight_BranchNodes_SingleElementLeft(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1")},
		[]base.PageID{1, 2},
	)
	rightSibling := newBranchNode(
		[][]byte{[]byte("k4"), []byte("k5")},
		[]base.PageID{10, 11, 12},
	)
	parent := newBranchNode(
		[][]byte{[]byte("k3")},
		[]base.PageID{100, 101},
	)

	BorrowFromRight(node, rightSibling, parent, 0)

	// Verify node
	expectedKeys := [][]byte{[]byte("k1"), []byte("k3")}
	expectedChildren := []base.PageID{1, 2, 10}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['k1', 'k3']")
	require.Equal(t, len(expectedChildren), len(node.Children), "node children length should match")
	assert.Equal(t, expectedChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(2), node.NumKeys, "node NumKeys should be 2")

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("k5")}
	expectedRightChildren := []base.PageID{11, 12}
	assert.True(t, equalByteSlices(rightSibling.Keys, expectedRightKeys), "rightSibling keys should be ['k5']")
	require.Equal(t, len(expectedRightChildren), len(rightSibling.Children), "rightSibling children length should match")
	assert.Equal(t, expectedRightChildren, rightSibling.Children, "rightSibling children should match expected")
	assert.Equal(t, uint16(1), rightSibling.NumKeys, "rightSibling NumKeys should be 1")

	// Verify parent
	assert.Equal(t, []byte("k4"), parent.Keys[0], "parent separator key should be 'k4'")

	// Verify dirty flags
	assert.True(t, node.Dirty, "node should be marked dirty")
	assert.True(t, rightSibling.Dirty, "rightSibling should be marked dirty")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestBorrowFromRight_ParentKeyIdx_NonZero(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("m")},
		[][]byte{[]byte("v1")},
	)
	rightSibling := newLeafNode(
		[][]byte{[]byte("p"), []byte("q")},
		[][]byte{[]byte("v2"), []byte("v3")},
	)
	parent := newBranchNode(
		[][]byte{[]byte("e"), []byte("p"), []byte("z")},
		[]base.PageID{1, 2, 3, 4},
	)

	BorrowFromRight(node, rightSibling, parent, 1)

	// Verify parent.Keys[1] changed to "q"
	assert.Equal(t, []byte("q"), parent.Keys[1], "parent.Keys[1] should be 'q'")

	// Verify other parent keys unchanged
	assert.Equal(t, []byte("e"), parent.Keys[0], "parent.Keys[0] should remain 'e'")
	assert.Equal(t, []byte("z"), parent.Keys[2], "parent.Keys[2] should remain 'z'")
}

// MergeNodes Tests

func TestMergeNodes_LeafNodes(t *testing.T) {
	left := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)
	right := newLeafNode(
		[][]byte{[]byte("d"), []byte("e")},
		[][]byte{[]byte("v4"), []byte("v5")},
	)

	MergeNodes(left, right, []byte("c"))

	// Verify left
	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("d"), []byte("e")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v4"), []byte("v5")}
	assert.True(t, equalByteSlices(left.Keys, expectedKeys), "left keys should be ['a', 'b', 'd', 'e']")
	assert.True(t, equalByteSlices(left.Values, expectedValues), "left values should be ['v1', 'v2', 'v4', 'v5']")
	assert.Equal(t, uint16(4), left.NumKeys, "left NumKeys should be 4")
	assert.True(t, left.Dirty, "left should be marked dirty")
}

func TestMergeNodes_LeafNodes_SingleElementEach(t *testing.T) {
	left := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)
	right := newLeafNode(
		[][]byte{[]byte("c")},
		[][]byte{[]byte("v3")},
	)

	MergeNodes(left, right, []byte("b"))

	// Verify left
	expectedKeys := [][]byte{[]byte("a"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v3")}
	assert.True(t, equalByteSlices(left.Keys, expectedKeys), "left keys should be ['a', 'c']")
	assert.True(t, equalByteSlices(left.Values, expectedValues), "left values should be ['v1', 'v3']")
	assert.Equal(t, uint16(2), left.NumKeys, "left NumKeys should be 2")
	assert.True(t, left.Dirty, "left should be marked dirty")
}

func TestMergeNodes_LeafNodes_EmptyLeft(t *testing.T) {
	left := newLeafNode(
		[][]byte{},
		[][]byte{},
	)
	right := newLeafNode(
		[][]byte{[]byte("b"), []byte("c")},
		[][]byte{[]byte("v2"), []byte("v3")},
	)

	MergeNodes(left, right, []byte("a"))

	// Verify left
	expectedKeys := [][]byte{[]byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v2"), []byte("v3")}
	assert.True(t, equalByteSlices(left.Keys, expectedKeys), "left keys should be ['b', 'c']")
	assert.True(t, equalByteSlices(left.Values, expectedValues), "left values should be ['v2', 'v3']")
	assert.Equal(t, uint16(2), left.NumKeys, "left NumKeys should be 2")
	assert.True(t, left.Dirty, "left should be marked dirty")
}

func TestMergeNodes_BranchNodes(t *testing.T) {
	left := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{1, 2, 3},
	)
	right := newBranchNode(
		[][]byte{[]byte("k5"), []byte("k6")},
		[]base.PageID{10, 11, 12},
	)

	MergeNodes(left, right, []byte("k4"))

	// Verify left
	expectedKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k4"), []byte("k5"), []byte("k6")}
	assert.True(t, equalByteSlices(left.Keys, expectedKeys), "left keys should include separator")
	assert.Nil(t, left.Values, "left values should be nil for branch node")

	expectedChildren := []base.PageID{1, 2, 3, 10, 11, 12}
	require.Equal(t, len(expectedChildren), len(left.Children), "left children length should match")
	assert.Equal(t, expectedChildren, left.Children, "left children should match expected")
	assert.Equal(t, uint16(5), left.NumKeys, "left NumKeys should be 5")
	assert.True(t, left.Dirty, "left should be marked dirty")
}

func TestMergeNodes_BranchNodes_SingleElementEach(t *testing.T) {
	left := newBranchNode(
		[][]byte{[]byte("k1")},
		[]base.PageID{1, 2},
	)
	right := newBranchNode(
		[][]byte{[]byte("k3")},
		[]base.PageID{10, 11},
	)

	MergeNodes(left, right, []byte("k2"))

	// Verify left
	expectedKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	assert.True(t, equalByteSlices(left.Keys, expectedKeys), "left keys should be ['k1', 'k2', 'k3']")

	expectedChildren := []base.PageID{1, 2, 10, 11}
	require.Equal(t, len(expectedChildren), len(left.Children), "left children length should match")
	assert.Equal(t, expectedChildren, left.Children, "left children should match expected")
	assert.Equal(t, uint16(3), left.NumKeys, "left NumKeys should be 3")
	assert.True(t, left.Dirty, "left should be marked dirty")
}

func TestMergeNodes_BranchNodes_EmptyLeft(t *testing.T) {
	left := newBranchNode(
		[][]byte{},
		[]base.PageID{1},
	)
	right := newBranchNode(
		[][]byte{[]byte("k2")},
		[]base.PageID{10, 11},
	)

	MergeNodes(left, right, []byte("k1"))

	// Verify left
	expectedKeys := [][]byte{[]byte("k1"), []byte("k2")}
	assert.True(t, equalByteSlices(left.Keys, expectedKeys), "left keys should be ['k1', 'k2']")

	expectedChildren := []base.PageID{1, 10, 11}
	require.Equal(t, len(expectedChildren), len(left.Children), "left children length should match")
	assert.Equal(t, expectedChildren, left.Children, "left children should match expected")
	assert.Equal(t, uint16(2), left.NumKeys, "left NumKeys should be 2")
	assert.True(t, left.Dirty, "left should be marked dirty")
}

func TestMergeNodes_LeafNodes_ValuesNilCheck(t *testing.T) {
	left := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)
	right := newLeafNode(
		[][]byte{[]byte("b")},
		[][]byte{[]byte("v2")},
	)

	MergeNodes(left, right, []byte("sep"))

	// Verify left.Values is not nil
	assert.NotNil(t, left.Values, "left values should not be nil for leaf node")

	// Verify values are correct
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}
	assert.True(t, equalByteSlices(left.Values, expectedValues), "left values should be ['v1', 'v2']")
}

func TestMergeNodes_BranchNodes_ValuesNilCheck(t *testing.T) {
	left := newBranchNode(
		[][]byte{[]byte("k1")},
		[]base.PageID{1, 2},
	)
	// Set Values to non-nil initially (junk data)
	left.Values = [][]byte{[]byte("junk")}

	right := newBranchNode(
		[][]byte{[]byte("k2")},
		[]base.PageID{10, 11},
	)

	MergeNodes(left, right, []byte("sep"))

	// Verify left.Values is nil
	assert.Nil(t, left.Values, "left values should be nil for branch node")
}

// NewBranchRoot Tests

func TestNewBranchRoot_Basic(t *testing.T) {
	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 20}

	root := NewBranchRoot(leftChild, rightChild, []byte("m"), 100)

	assert.Equal(t, base.PageID(100), root.PageID, "root PageID should be 100")
	assert.True(t, root.Dirty, "root should be marked dirty")
	assert.False(t, root.Leaf, "root should not be a leaf")
	assert.Equal(t, uint16(1), root.NumKeys, "root NumKeys should be 1")
	assert.True(t, equalByteSlices(root.Keys, [][]byte{[]byte("m")}), "root keys should contain separator")

	expectedChildren := []base.PageID{10, 20}
	require.Equal(t, 2, len(root.Children), "root should have 2 children")
	assert.Equal(t, expectedChildren, root.Children, "root children should match expected")
}

func TestNewBranchRoot_EmptyKey(t *testing.T) {
	leftChild := &base.Node{PageID: 5}
	rightChild := &base.Node{PageID: 6}

	root := NewBranchRoot(leftChild, rightChild, []byte(""), 1)

	assert.True(t, equalByteSlices(root.Keys, [][]byte{[]byte("")}), "root keys should contain empty separator")

	expectedChildren := []base.PageID{5, 6}
	require.Equal(t, 2, len(root.Children), "root should have 2 children")
	assert.Equal(t, expectedChildren, root.Children, "root children should be [5, 6]")
	assert.Equal(t, uint16(1), root.NumKeys, "root NumKeys should be 1")
	assert.False(t, root.Leaf, "root should not be a leaf")
}

func TestNewBranchRoot_LargePageIDs(t *testing.T) {
	leftChild := &base.Node{PageID: 999999}
	rightChild := &base.Node{PageID: 1000000}

	root := NewBranchRoot(leftChild, rightChild, []byte("sep"), 1)

	expectedChildren := []base.PageID{999999, 1000000}
	require.Equal(t, 2, len(root.Children), "root should have 2 children")
	assert.Equal(t, expectedChildren, root.Children, "root children should be [999999, 1000000]")
	assert.Equal(t, base.PageID(1), root.PageID, "root PageID should be 1")
}

// ApplyChildSplit Tests

func TestApplyChildSplit_Middle(t *testing.T) {
	parent := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k3")},
		[]base.PageID{1, 2, 3},
	)
	parent.NumKeys = 2
	parent.Dirty = false

	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 11}

	ApplyChildSplit(parent, 1, leftChild, rightChild, []byte("k2"), []byte("v2"))

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	assert.True(t, equalByteSlices(parent.Keys, expectedKeys), "parent keys should be ['k1', 'k2', 'k3']")

	expectedChildren := []base.PageID{1, 10, 11, 3}
	require.Equal(t, 4, len(parent.Children), "parent should have 4 children")
	assert.Equal(t, expectedChildren, parent.Children, "parent children should match expected")
	assert.Equal(t, uint16(3), parent.NumKeys, "parent NumKeys should be 3")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestApplyChildSplit_Beginning(t *testing.T) {
	parent := newBranchNode(
		[][]byte{[]byte("k2")},
		[]base.PageID{1, 2},
	)
	parent.NumKeys = 1
	parent.Dirty = false

	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 11}

	ApplyChildSplit(parent, 0, leftChild, rightChild, []byte("k1"), nil)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2")}
	assert.True(t, equalByteSlices(parent.Keys, expectedKeys), "parent keys should be ['k1', 'k2']")

	expectedChildren := []base.PageID{10, 11, 2}
	require.Equal(t, 3, len(parent.Children), "parent should have 3 children")
	assert.Equal(t, expectedChildren, parent.Children, "parent children should match expected")
	assert.Equal(t, uint16(2), parent.NumKeys, "parent NumKeys should be 2")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestApplyChildSplit_End(t *testing.T) {
	parent := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{1, 2, 3},
	)
	parent.NumKeys = 2

	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 11}

	ApplyChildSplit(parent, 2, leftChild, rightChild, []byte("k3"), nil)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	assert.True(t, equalByteSlices(parent.Keys, expectedKeys), "parent keys should be ['k1', 'k2', 'k3']")

	expectedChildren := []base.PageID{1, 2, 10, 11}
	require.Equal(t, 4, len(parent.Children), "parent should have 4 children")
	assert.Equal(t, expectedChildren, parent.Children, "parent children should match expected")
	assert.Equal(t, uint16(3), parent.NumKeys, "parent NumKeys should be 3")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestApplyChildSplit_LeafParent(t *testing.T) {
	parent := newLeafNode(
		[][]byte{[]byte("k2")},
		[][]byte{[]byte("v2")},
	)
	parent.Children = []base.PageID{1, 2}
	parent.NumKeys = 1
	parent.Dirty = false

	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 11}

	ApplyChildSplit(parent, 0, leftChild, rightChild, []byte("k1"), []byte("v1"))

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2")}
	assert.True(t, equalByteSlices(parent.Keys, expectedKeys), "parent keys should be ['k1', 'k2']")

	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}
	assert.True(t, equalByteSlices(parent.Values, expectedValues), "parent values should be ['v1', 'v2']")

	expectedChildren := []base.PageID{10, 11, 2}
	require.Equal(t, 3, len(parent.Children), "parent should have 3 children")
	assert.Equal(t, expectedChildren, parent.Children, "parent children should match expected")
	assert.Equal(t, uint16(2), parent.NumKeys, "parent NumKeys should be 2")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

func TestApplyChildSplit_SingleKeyParent(t *testing.T) {
	parent := newBranchNode(
		[][]byte{[]byte("k2")},
		[]base.PageID{1, 2},
	)
	parent.NumKeys = 1

	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 11}

	ApplyChildSplit(parent, 1, leftChild, rightChild, []byte("k3"), nil)

	expectedKeys := [][]byte{[]byte("k2"), []byte("k3")}
	assert.True(t, equalByteSlices(parent.Keys, expectedKeys), "parent keys should be ['k2', 'k3']")

	expectedChildren := []base.PageID{1, 10, 11}
	require.Equal(t, 3, len(parent.Children), "parent should have 3 children")
	assert.Equal(t, expectedChildren, parent.Children, "parent children should match expected")
	assert.Equal(t, uint16(2), parent.NumKeys, "parent NumKeys should be 2")
	assert.True(t, parent.Dirty, "parent should be marked dirty")
}

// TruncateLeft Tests

func TestTruncateLeft_LeafNode(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4"), []byte("v5")},
	)
	node.NumKeys = 5
	node.Dirty = false

	sp := SplitPoint{Mid: 2, LeftCount: 2, RightCount: 3, SeparatorKey: []byte("c")}
	TruncateLeft(node, sp)

	expectedKeys := [][]byte{[]byte("a"), []byte("b")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['a', 'b']")

	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}
	assert.True(t, equalByteSlices(node.Values, expectedValues), "node values should be ['v1', 'v2']")
	assert.Equal(t, uint16(2), node.NumKeys, "node NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestTruncateLeft_LeafNode_SingleElement(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)
	node.NumKeys = 2

	sp := SplitPoint{Mid: 0, LeftCount: 1, RightCount: 1, SeparatorKey: []byte("b")}
	TruncateLeft(node, sp)

	expectedKeys := [][]byte{[]byte("a")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['a']")

	expectedValues := [][]byte{[]byte("v1")}
	assert.True(t, equalByteSlices(node.Values, expectedValues), "node values should be ['v1']")
	assert.Equal(t, uint16(1), node.NumKeys, "node NumKeys should be 1")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestTruncateLeft_BranchNode(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4")},
		[]base.PageID{1, 2, 3, 4, 5},
	)
	node.NumKeys = 4
	node.Dirty = false

	sp := SplitPoint{Mid: 1, LeftCount: 1, RightCount: 3, SeparatorKey: []byte("k2")}
	TruncateLeft(node, sp)

	expectedKeys := [][]byte{[]byte("k1")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['k1']")
	assert.Nil(t, node.Values, "node values should be nil for branch node")

	expectedChildren := []base.PageID{1, 2}
	require.Equal(t, 2, len(node.Children), "node should have 2 children")
	assert.Equal(t, expectedChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(1), node.NumKeys, "node NumKeys should be 1")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestTruncateLeft_BranchNode_MultipleElements(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3"), []byte("k4"), []byte("k5")},
		[]base.PageID{1, 2, 3, 4, 5, 6},
	)
	node.NumKeys = 5
	node.Dirty = false

	sp := SplitPoint{Mid: 2, LeftCount: 2, RightCount: 3, SeparatorKey: []byte("k3")}
	TruncateLeft(node, sp)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['k1', 'k2']")

	expectedChildren := []base.PageID{1, 2, 3}
	require.Equal(t, 3, len(node.Children), "node should have 3 children")
	assert.Equal(t, expectedChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(2), node.NumKeys, "node NumKeys should be 2")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestTruncateLeft_LeafNode_AllButOne(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3"), []byte("v4")},
	)
	node.NumKeys = 4

	sp := SplitPoint{Mid: 2, LeftCount: 3, RightCount: 1, SeparatorKey: []byte("d")}
	TruncateLeft(node, sp)

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['a', 'b', 'c']")

	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	assert.True(t, equalByteSlices(node.Values, expectedValues), "node values should be ['v1', 'v2', 'v3']")
	assert.Equal(t, uint16(3), node.NumKeys, "node NumKeys should be 3")
	assert.True(t, node.Dirty, "node should be marked dirty")
}

func TestTruncateLeft_BranchNode_ChildrenAlignment(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[]base.PageID{10, 20, 30, 40},
	)
	node.NumKeys = 3

	sp := SplitPoint{Mid: 1, LeftCount: 1, RightCount: 2, SeparatorKey: []byte("k2")}
	TruncateLeft(node, sp)

	expectedKeys := [][]byte{[]byte("k1")}
	assert.True(t, equalByteSlices(node.Keys, expectedKeys), "node keys should be ['k1']")

	expectedChildren := []base.PageID{10, 20}
	require.Equal(t, 2, len(node.Children), "node should have 2 children")
	assert.Equal(t, expectedChildren, node.Children, "node children should match expected")
	assert.Equal(t, uint16(1), node.NumKeys, "node NumKeys should be 1")
	assert.Equal(t, len(node.Keys)+1, len(node.Children), "children count should be keys + 1")
}

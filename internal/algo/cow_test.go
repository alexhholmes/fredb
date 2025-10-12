package algo

import (
	"testing"

	"fredb/internal/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper: compare byte slice arrays
func equalByteSlices(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// Helper: create leaf node for testing
func newLeafNode(keys, values [][]byte) *base.Node {
	return &base.Node{
		PageID:  1,
		Dirty:   false,
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
	}
}

// ApplyLeafUpdate Tests

func TestApplyLeafUpdate_BasicUpdate(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafUpdate(node, 1, []byte("v2-updated"))

	if !bytes.Equal(node.Values[1], []byte("v2-updated")) {
		t.Errorf("expected Values[1] = 'v2-updated', got '%s'", node.Values[1])
	}
	if !bytes.Equal(node.Values[0], []byte("v1")) {
		t.Errorf("expected Values[0] = 'v1', got '%s'", node.Values[0])
	}
	if !bytes.Equal(node.Values[2], []byte("v3")) {
		t.Errorf("expected Values[2] = 'v3', got '%s'", node.Values[2])
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
	if node.NumKeys != 3 {
		t.Errorf("expected NumKeys = 3, got %d", node.NumKeys)
	}
	expectedKeys := [][]byte{[]byte("apple"), []byte("banana"), []byte("cherry")}
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("keys should remain unchanged")
	}
}

func TestApplyLeafUpdate_FirstPosition(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("key1"), []byte("key2")},
		[][]byte{[]byte("val1"), []byte("val2")},
	)

	ApplyLeafUpdate(node, 0, []byte("new-val1"))

	if !bytes.Equal(node.Values[0], []byte("new-val1")) {
		t.Errorf("expected Values[0] = 'new-val1', got '%s'", node.Values[0])
	}
	if !bytes.Equal(node.Values[1], []byte("val2")) {
		t.Errorf("expected Values[1] = 'val2', got '%s'", node.Values[1])
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
	if node.NumKeys != 2 {
		t.Errorf("expected NumKeys = 2, got %d", node.NumKeys)
	}
}

func TestApplyLeafUpdate_LastPosition(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafUpdate(node, 2, []byte("v3-new"))

	if !bytes.Equal(node.Values[2], []byte("v3-new")) {
		t.Errorf("expected Values[2] = 'v3-new', got '%s'", node.Values[2])
	}
	if !bytes.Equal(node.Values[0], []byte("v1")) {
		t.Errorf("expected Values[0] = 'v1', got '%s'", node.Values[0])
	}
	if !bytes.Equal(node.Values[1], []byte("v2")) {
		t.Errorf("expected Values[1] = 'v2', got '%s'", node.Values[1])
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafUpdate_EmptyValue(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("key")},
		[][]byte{[]byte("original")},
	)

	ApplyLeafUpdate(node, 0, []byte(""))

	if !bytes.Equal(node.Values[0], []byte("")) {
		t.Errorf("expected Values[0] = '', got '%s'", node.Values[0])
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
	if node.NumKeys != 1 {
		t.Errorf("expected NumKeys = 1, got %d", node.NumKeys)
	}
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

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a', 'b', 'c'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1', 'v2', 'v3'], got %v", node.Values)
	}
	if node.NumKeys != 3 {
		t.Errorf("expected NumKeys = 3, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafInsert_Beginning(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("b"), []byte("c")},
		[][]byte{[]byte("v2"), []byte("v3")},
	)

	ApplyLeafInsert(node, 0, []byte("a"), []byte("v1"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a', 'b', 'c'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1', 'v2', 'v3'], got %v", node.Values)
	}
	if node.NumKeys != 3 {
		t.Errorf("expected NumKeys = 3, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafInsert_End(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)

	ApplyLeafInsert(node, 2, []byte("c"), []byte("v3"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a', 'b', 'c'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1', 'v2', 'v3'], got %v", node.Values)
	}
	if node.NumKeys != 3 {
		t.Errorf("expected NumKeys = 3, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafInsert_EmptyNode(t *testing.T) {
	node := newLeafNode(
		[][]byte{},
		[][]byte{},
	)

	ApplyLeafInsert(node, 0, []byte("first"), []byte("value1"))

	expectedKeys := [][]byte{[]byte("first")}
	expectedValues := [][]byte{[]byte("value1")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['first'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['value1'], got %v", node.Values)
	}
	if node.NumKeys != 1 {
		t.Errorf("expected NumKeys = 1, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafInsert_SingleElement(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)

	ApplyLeafInsert(node, 1, []byte("b"), []byte("v2"))

	expectedKeys := [][]byte{[]byte("a"), []byte("b")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a', 'b'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1', 'v2'], got %v", node.Values)
	}
	if node.NumKeys != 2 {
		t.Errorf("expected NumKeys = 2, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
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

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a', 'c'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1', 'v3'], got %v", node.Values)
	}
	if node.NumKeys != 2 {
		t.Errorf("expected NumKeys = 2, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafDelete_First(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafDelete(node, 0)

	expectedKeys := [][]byte{[]byte("b"), []byte("c")}
	expectedValues := [][]byte{[]byte("v2"), []byte("v3")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['b', 'c'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v2', 'v3'], got %v", node.Values)
	}
	if node.NumKeys != 2 {
		t.Errorf("expected NumKeys = 2, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafDelete_Last(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)

	ApplyLeafDelete(node, 2)

	expectedKeys := [][]byte{[]byte("a"), []byte("b")}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a', 'b'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1', 'v2'], got %v", node.Values)
	}
	if node.NumKeys != 2 {
		t.Errorf("expected NumKeys = 2, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafDelete_OnlyElement(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a")},
		[][]byte{[]byte("v1")},
	)

	ApplyLeafDelete(node, 0)

	expectedKeys := [][]byte{}
	expectedValues := [][]byte{}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = [], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = [], got %v", node.Values)
	}
	if node.NumKeys != 0 {
		t.Errorf("expected NumKeys = 0, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

func TestApplyLeafDelete_DownToOne(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("a"), []byte("b")},
		[][]byte{[]byte("v1"), []byte("v2")},
	)

	ApplyLeafDelete(node, 1)

	expectedKeys := [][]byte{[]byte("a")}
	expectedValues := [][]byte{[]byte("v1")}

	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("expected Keys = ['a'], got %v", node.Keys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("expected Values = ['v1'], got %v", node.Values)
	}
	if node.NumKeys != 1 {
		t.Errorf("expected NumKeys = 1, got %d", node.NumKeys)
	}
	if !node.Dirty {
		t.Errorf("expected Dirty = true, got false")
	}
}

// ApplyBranchRemoveSeparator Tests

func TestApplyBranchRemoveSeparator_BranchNode_Middle(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[]base.PageID{1, 2, 3, 4},
	)

	ApplyBranchRemoveSeparator(node, 1)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k3")}
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}

	if node.Values != nil {
		t.Errorf("Values = %v, want nil", node.Values)
	}

	expectedChildren := []base.PageID{1, 2, 4}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("Children length = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i := range expectedChildren {
		if node.Children[i] != expectedChildren[i] {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], expectedChildren[i])
		}
	}

	if node.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", node.NumKeys)
	}

	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
}

func TestApplyBranchRemoveSeparator_BranchNode_First(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{10, 20, 30},
	)

	ApplyBranchRemoveSeparator(node, 0)

	expectedKeys := [][]byte{[]byte("k2")}
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}

	expectedChildren := []base.PageID{10, 30}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("Children length = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i := range expectedChildren {
		if node.Children[i] != expectedChildren[i] {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], expectedChildren[i])
		}
	}

	if node.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", node.NumKeys)
	}

	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
}

func TestApplyBranchRemoveSeparator_BranchNode_Last(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[]base.PageID{1, 2, 3, 4},
	)

	ApplyBranchRemoveSeparator(node, 2)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k2")}
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}

	expectedChildren := []base.PageID{1, 2, 3}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("Children length = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i := range expectedChildren {
		if node.Children[i] != expectedChildren[i] {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], expectedChildren[i])
		}
	}

	if node.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", node.NumKeys)
	}

	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
}

func TestApplyBranchRemoveSeparator_LeafNode(t *testing.T) {
	node := newLeafNode(
		[][]byte{[]byte("k1"), []byte("k2"), []byte("k3")},
		[][]byte{[]byte("v1"), []byte("v2"), []byte("v3")},
	)
	node.Children = []base.PageID{1, 2, 3, 4}

	ApplyBranchRemoveSeparator(node, 1)

	expectedKeys := [][]byte{[]byte("k1"), []byte("k3")}
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}

	expectedValues := [][]byte{[]byte("v1"), []byte("v3")}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("Values = %v, want %v", node.Values, expectedValues)
	}

	expectedChildren := []base.PageID{1, 2, 4}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("Children length = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i := range expectedChildren {
		if node.Children[i] != expectedChildren[i] {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], expectedChildren[i])
		}
	}

	if node.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", node.NumKeys)
	}

	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
}

func TestApplyBranchRemoveSeparator_DownToOneKey(t *testing.T) {
	node := newBranchNode(
		[][]byte{[]byte("k1"), []byte("k2")},
		[]base.PageID{1, 2, 3},
	)

	ApplyBranchRemoveSeparator(node, 0)

	expectedKeys := [][]byte{[]byte("k2")}
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}

	expectedChildren := []base.PageID{1, 3}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("Children length = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i := range expectedChildren {
		if node.Children[i] != expectedChildren[i] {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], expectedChildren[i])
		}
	}

	if node.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", node.NumKeys)
	}

	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedNodeKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedNodeKeys)
	}

	expectedNodeValues := [][]byte{[]byte("v3"), []byte("v5"), []byte("v6")}
	if !equalByteSlices(node.Values, expectedNodeValues) {
		t.Errorf("node.Values = %v, want %v", node.Values, expectedNodeValues)
	}

	if node.NumKeys != 3 {
		t.Errorf("node.NumKeys = %v, want 3", node.NumKeys)
	}

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("a"), []byte("b")}
	if !equalByteSlices(leftSibling.Keys, expectedLeftKeys) {
		t.Errorf("leftSibling.Keys = %v, want %v", leftSibling.Keys, expectedLeftKeys)
	}

	expectedLeftValues := [][]byte{[]byte("v1"), []byte("v2")}
	if !equalByteSlices(leftSibling.Values, expectedLeftValues) {
		t.Errorf("leftSibling.Values = %v, want %v", leftSibling.Values, expectedLeftValues)
	}

	if leftSibling.NumKeys != 2 {
		t.Errorf("leftSibling.NumKeys = %v, want 2", leftSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("c")) {
		t.Errorf("parent.Keys[0] = %v, want 'c'", parent.Keys[0])
	}

	// Verify all dirty
	if !node.Dirty {
		t.Error("node.Dirty = false, want true")
	}
	if !leftSibling.Dirty {
		t.Error("leftSibling.Dirty = false, want true")
	}
	if !parent.Dirty {
		t.Error("parent.Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedNodeKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedNodeKeys)
	}

	expectedNodeValues := [][]byte{[]byte("v2"), []byte("v4")}
	if !equalByteSlices(node.Values, expectedNodeValues) {
		t.Errorf("node.Values = %v, want %v", node.Values, expectedNodeValues)
	}

	if node.NumKeys != 2 {
		t.Errorf("node.NumKeys = %v, want 2", node.NumKeys)
	}

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("a")}
	if !equalByteSlices(leftSibling.Keys, expectedLeftKeys) {
		t.Errorf("leftSibling.Keys = %v, want %v", leftSibling.Keys, expectedLeftKeys)
	}

	if leftSibling.NumKeys != 1 {
		t.Errorf("leftSibling.NumKeys = %v, want 1", leftSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("b")) {
		t.Errorf("parent.Keys[0] = %v, want 'b'", parent.Keys[0])
	}

	// Verify all dirty
	if !node.Dirty || !leftSibling.Dirty || !parent.Dirty {
		t.Error("All Dirty flags should be true")
	}
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
	if !equalByteSlices(node.Keys, expectedNodeKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedNodeKeys)
	}

	expectedNodeChildren := []base.PageID{4, 10, 11, 12}
	if len(node.Children) != len(expectedNodeChildren) {
		t.Fatalf("node.Children length = %v, want %v", len(node.Children), len(expectedNodeChildren))
	}
	for i := range expectedNodeChildren {
		if node.Children[i] != expectedNodeChildren[i] {
			t.Errorf("node.Children[%d] = %v, want %v", i, node.Children[i], expectedNodeChildren[i])
		}
	}

	if node.NumKeys != 3 {
		t.Errorf("node.NumKeys = %v, want 3", node.NumKeys)
	}

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("k1"), []byte("k2")}
	if !equalByteSlices(leftSibling.Keys, expectedLeftKeys) {
		t.Errorf("leftSibling.Keys = %v, want %v", leftSibling.Keys, expectedLeftKeys)
	}

	expectedLeftChildren := []base.PageID{1, 2, 3}
	if len(leftSibling.Children) != len(expectedLeftChildren) {
		t.Fatalf("leftSibling.Children length = %v, want %v", len(leftSibling.Children), len(expectedLeftChildren))
	}
	for i := range expectedLeftChildren {
		if leftSibling.Children[i] != expectedLeftChildren[i] {
			t.Errorf("leftSibling.Children[%d] = %v, want %v", i, leftSibling.Children[i], expectedLeftChildren[i])
		}
	}

	if leftSibling.NumKeys != 2 {
		t.Errorf("leftSibling.NumKeys = %v, want 2", leftSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("k3")) {
		t.Errorf("parent.Keys[0] = %v, want 'k3'", parent.Keys[0])
	}

	// Verify all dirty
	if !node.Dirty || !leftSibling.Dirty || !parent.Dirty {
		t.Error("All Dirty flags should be true")
	}
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
	if !equalByteSlices(node.Keys, expectedNodeKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedNodeKeys)
	}

	expectedNodeChildren := []base.PageID{3, 10, 11}
	if len(node.Children) != len(expectedNodeChildren) {
		t.Fatalf("node.Children length = %v, want %v", len(node.Children), len(expectedNodeChildren))
	}
	for i := range expectedNodeChildren {
		if node.Children[i] != expectedNodeChildren[i] {
			t.Errorf("node.Children[%d] = %v, want %v", i, node.Children[i], expectedNodeChildren[i])
		}
	}

	if node.NumKeys != 2 {
		t.Errorf("node.NumKeys = %v, want 2", node.NumKeys)
	}

	// Verify left sibling
	expectedLeftKeys := [][]byte{[]byte("k1")}
	if !equalByteSlices(leftSibling.Keys, expectedLeftKeys) {
		t.Errorf("leftSibling.Keys = %v, want %v", leftSibling.Keys, expectedLeftKeys)
	}

	expectedLeftChildren := []base.PageID{1, 2}
	if len(leftSibling.Children) != len(expectedLeftChildren) {
		t.Fatalf("leftSibling.Children length = %v, want %v", len(leftSibling.Children), len(expectedLeftChildren))
	}
	for i := range expectedLeftChildren {
		if leftSibling.Children[i] != expectedLeftChildren[i] {
			t.Errorf("leftSibling.Children[%d] = %v, want %v", i, leftSibling.Children[i], expectedLeftChildren[i])
		}
	}

	if leftSibling.NumKeys != 1 {
		t.Errorf("leftSibling.NumKeys = %v, want 1", leftSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("k2")) {
		t.Errorf("parent.Keys[0] = %v, want 'k2'", parent.Keys[0])
	}

	// Verify all dirty
	if !node.Dirty || !leftSibling.Dirty || !parent.Dirty {
		t.Error("All Dirty flags should be true")
	}
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
	if !bytes.Equal(parent.Keys[1], []byte("n")) {
		t.Errorf("parent.Keys[1] = %v, want 'n'", parent.Keys[1])
	}

	// Verify other parent keys are unchanged
	if !bytes.Equal(parent.Keys[0], []byte("e")) {
		t.Errorf("parent.Keys[0] = %v, want 'e'", parent.Keys[0])
	}

	if !bytes.Equal(parent.Keys[2], []byte("z")) {
		t.Errorf("parent.Keys[2] = %v, want 'z'", parent.Keys[2])
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedKeys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("node.Values = %v, want %v", node.Values, expectedValues)
	}
	if node.NumKeys != 3 {
		t.Errorf("node.NumKeys = %v, want 3", node.NumKeys)
	}

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("f"), []byte("g")}
	expectedRightValues := [][]byte{[]byte("v6"), []byte("v7")}
	if !equalByteSlices(rightSibling.Keys, expectedRightKeys) {
		t.Errorf("rightSibling.Keys = %v, want %v", rightSibling.Keys, expectedRightKeys)
	}
	if !equalByteSlices(rightSibling.Values, expectedRightValues) {
		t.Errorf("rightSibling.Values = %v, want %v", rightSibling.Values, expectedRightValues)
	}
	if rightSibling.NumKeys != 2 {
		t.Errorf("rightSibling.NumKeys = %v, want 2", rightSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("f")) {
		t.Errorf("parent.Keys[0] = %v, want 'f'", parent.Keys[0])
	}

	// Verify dirty flags
	if !node.Dirty {
		t.Error("node.Dirty = false, want true")
	}
	if !rightSibling.Dirty {
		t.Error("rightSibling.Dirty = false, want true")
	}
	if !parent.Dirty {
		t.Error("parent.Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedKeys)
	}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("node.Values = %v, want %v", node.Values, expectedValues)
	}
	if node.NumKeys != 2 {
		t.Errorf("node.NumKeys = %v, want 2", node.NumKeys)
	}

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("d")}
	if !equalByteSlices(rightSibling.Keys, expectedRightKeys) {
		t.Errorf("rightSibling.Keys = %v, want %v", rightSibling.Keys, expectedRightKeys)
	}
	if rightSibling.NumKeys != 1 {
		t.Errorf("rightSibling.NumKeys = %v, want 1", rightSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("d")) {
		t.Errorf("parent.Keys[0] = %v, want 'd'", parent.Keys[0])
	}

	// Verify dirty flags
	if !node.Dirty {
		t.Error("node.Dirty = false, want true")
	}
	if !rightSibling.Dirty {
		t.Error("rightSibling.Dirty = false, want true")
	}
	if !parent.Dirty {
		t.Error("parent.Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedKeys)
	}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("len(node.Children) = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i, c := range node.Children {
		if c != expectedChildren[i] {
			t.Errorf("node.Children[%d] = %v, want %v", i, c, expectedChildren[i])
		}
	}
	if node.NumKeys != 3 {
		t.Errorf("node.NumKeys = %v, want 3", node.NumKeys)
	}

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("k7"), []byte("k8")}
	expectedRightChildren := []base.PageID{11, 12, 13}
	if !equalByteSlices(rightSibling.Keys, expectedRightKeys) {
		t.Errorf("rightSibling.Keys = %v, want %v", rightSibling.Keys, expectedRightKeys)
	}
	if len(rightSibling.Children) != len(expectedRightChildren) {
		t.Fatalf("len(rightSibling.Children) = %v, want %v", len(rightSibling.Children), len(expectedRightChildren))
	}
	for i, c := range rightSibling.Children {
		if c != expectedRightChildren[i] {
			t.Errorf("rightSibling.Children[%d] = %v, want %v", i, c, expectedRightChildren[i])
		}
	}
	if rightSibling.NumKeys != 2 {
		t.Errorf("rightSibling.NumKeys = %v, want 2", rightSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("k6")) {
		t.Errorf("parent.Keys[0] = %v, want 'k6'", parent.Keys[0])
	}

	// Verify dirty flags
	if !node.Dirty {
		t.Error("node.Dirty = false, want true")
	}
	if !rightSibling.Dirty {
		t.Error("rightSibling.Dirty = false, want true")
	}
	if !parent.Dirty {
		t.Error("parent.Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("node.Keys = %v, want %v", node.Keys, expectedKeys)
	}
	if len(node.Children) != len(expectedChildren) {
		t.Fatalf("len(node.Children) = %v, want %v", len(node.Children), len(expectedChildren))
	}
	for i, c := range node.Children {
		if c != expectedChildren[i] {
			t.Errorf("node.Children[%d] = %v, want %v", i, c, expectedChildren[i])
		}
	}
	if node.NumKeys != 2 {
		t.Errorf("node.NumKeys = %v, want 2", node.NumKeys)
	}

	// Verify rightSibling
	expectedRightKeys := [][]byte{[]byte("k5")}
	expectedRightChildren := []base.PageID{11, 12}
	if !equalByteSlices(rightSibling.Keys, expectedRightKeys) {
		t.Errorf("rightSibling.Keys = %v, want %v", rightSibling.Keys, expectedRightKeys)
	}
	if len(rightSibling.Children) != len(expectedRightChildren) {
		t.Fatalf("len(rightSibling.Children) = %v, want %v", len(rightSibling.Children), len(expectedRightChildren))
	}
	for i, c := range rightSibling.Children {
		if c != expectedRightChildren[i] {
			t.Errorf("rightSibling.Children[%d] = %v, want %v", i, c, expectedRightChildren[i])
		}
	}
	if rightSibling.NumKeys != 1 {
		t.Errorf("rightSibling.NumKeys = %v, want 1", rightSibling.NumKeys)
	}

	// Verify parent
	if !bytes.Equal(parent.Keys[0], []byte("k4")) {
		t.Errorf("parent.Keys[0] = %v, want 'k4'", parent.Keys[0])
	}

	// Verify dirty flags
	if !node.Dirty {
		t.Error("node.Dirty = false, want true")
	}
	if !rightSibling.Dirty {
		t.Error("rightSibling.Dirty = false, want true")
	}
	if !parent.Dirty {
		t.Error("parent.Dirty = false, want true")
	}
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
	if !bytes.Equal(parent.Keys[1], []byte("q")) {
		t.Errorf("parent.Keys[1] = %v, want 'q'", parent.Keys[1])
	}

	// Verify other parent keys unchanged
	if !bytes.Equal(parent.Keys[0], []byte("e")) {
		t.Errorf("parent.Keys[0] = %v, want 'e'", parent.Keys[0])
	}
	if !bytes.Equal(parent.Keys[2], []byte("z")) {
		t.Errorf("parent.Keys[2] = %v, want 'z'", parent.Keys[2])
	}
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
	if !equalByteSlices(left.Keys, expectedKeys) {
		t.Errorf("left.Keys = %v, want %v", left.Keys, expectedKeys)
	}
	if !equalByteSlices(left.Values, expectedValues) {
		t.Errorf("left.Values = %v, want %v", left.Values, expectedValues)
	}
	if left.NumKeys != 4 {
		t.Errorf("left.NumKeys = %v, want 4", left.NumKeys)
	}
	if !left.Dirty {
		t.Error("left.Dirty = false, want true")
	}
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
	if !equalByteSlices(left.Keys, expectedKeys) {
		t.Errorf("left.Keys = %v, want %v", left.Keys, expectedKeys)
	}
	if !equalByteSlices(left.Values, expectedValues) {
		t.Errorf("left.Values = %v, want %v", left.Values, expectedValues)
	}
	if left.NumKeys != 2 {
		t.Errorf("left.NumKeys = %v, want 2", left.NumKeys)
	}
	if !left.Dirty {
		t.Error("left.Dirty = false, want true")
	}
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
	if !equalByteSlices(left.Keys, expectedKeys) {
		t.Errorf("left.Keys = %v, want %v", left.Keys, expectedKeys)
	}
	if !equalByteSlices(left.Values, expectedValues) {
		t.Errorf("left.Values = %v, want %v", left.Values, expectedValues)
	}
	if left.NumKeys != 2 {
		t.Errorf("left.NumKeys = %v, want 2", left.NumKeys)
	}
	if !left.Dirty {
		t.Error("left.Dirty = false, want true")
	}
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
	if !equalByteSlices(left.Keys, expectedKeys) {
		t.Errorf("left.Keys = %v, want %v", left.Keys, expectedKeys)
	}
	if left.Values != nil {
		t.Errorf("left.Values = %v, want nil", left.Values)
	}
	expectedChildren := []base.PageID{1, 2, 3, 10, 11, 12}
	if len(left.Children) != len(expectedChildren) {
		t.Fatalf("len(left.Children) = %v, want %v", len(left.Children), len(expectedChildren))
	}
	for i, c := range left.Children {
		if c != expectedChildren[i] {
			t.Errorf("left.Children[%d] = %v, want %v", i, c, expectedChildren[i])
		}
	}
	if left.NumKeys != 5 {
		t.Errorf("left.NumKeys = %v, want 5", left.NumKeys)
	}
	if !left.Dirty {
		t.Error("left.Dirty = false, want true")
	}
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
	if !equalByteSlices(left.Keys, expectedKeys) {
		t.Errorf("left.Keys = %v, want %v", left.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{1, 2, 10, 11}
	if len(left.Children) != len(expectedChildren) {
		t.Fatalf("len(left.Children) = %v, want %v", len(left.Children), len(expectedChildren))
	}
	for i, c := range left.Children {
		if c != expectedChildren[i] {
			t.Errorf("left.Children[%d] = %v, want %v", i, c, expectedChildren[i])
		}
	}
	if left.NumKeys != 3 {
		t.Errorf("left.NumKeys = %v, want 3", left.NumKeys)
	}
	if !left.Dirty {
		t.Error("left.Dirty = false, want true")
	}
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
	if !equalByteSlices(left.Keys, expectedKeys) {
		t.Errorf("left.Keys = %v, want %v", left.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{1, 10, 11}
	if len(left.Children) != len(expectedChildren) {
		t.Fatalf("len(left.Children) = %v, want %v", len(left.Children), len(expectedChildren))
	}
	for i, c := range left.Children {
		if c != expectedChildren[i] {
			t.Errorf("left.Children[%d] = %v, want %v", i, c, expectedChildren[i])
		}
	}
	if left.NumKeys != 2 {
		t.Errorf("left.NumKeys = %v, want 2", left.NumKeys)
	}
	if !left.Dirty {
		t.Error("left.Dirty = false, want true")
	}
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
	if left.Values == nil {
		t.Error("left.Values = nil, want not nil")
	}

	// Verify values are correct
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}
	if !equalByteSlices(left.Values, expectedValues) {
		t.Errorf("left.Values = %v, want %v", left.Values, expectedValues)
	}
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
	if left.Values != nil {
		t.Errorf("left.Values = %v, want nil", left.Values)
	}
}

// NewBranchRoot Tests

func TestNewBranchRoot_Basic(t *testing.T) {
	leftChild := &base.Node{PageID: 10}
	rightChild := &base.Node{PageID: 20}

	root := NewBranchRoot(leftChild, rightChild, []byte("m"), []byte("mid-value"), 100)

	if root.PageID != 100 {
		t.Errorf("PageID = %v, want 100", root.PageID)
	}
	if !root.Dirty {
		t.Error("Dirty = false, want true")
	}
	if root.IsLeaf() {
		t.Error("IsLeaf = true, want false")
	}
	if root.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", root.NumKeys)
	}
	if !equalByteSlices(root.Keys, [][]byte{[]byte("m")}) {
		t.Errorf("Keys = %v, want [[m]]", root.Keys)
	}
	if !equalByteSlices(root.Values, [][]byte{[]byte("mid-value")}) {
		t.Errorf("Values = %v, want [[mid-value]]", root.Values)
	}
	if len(root.Children) != 2 || root.Children[0] != 10 || root.Children[1] != 20 {
		t.Errorf("Children = %v, want [10 20]", root.Children)
	}
}

func TestNewBranchRoot_EmptyKey(t *testing.T) {
	leftChild := &base.Node{PageID: 5}
	rightChild := &base.Node{PageID: 6}

	root := NewBranchRoot(leftChild, rightChild, []byte(""), []byte(""), 1)

	if !equalByteSlices(root.Keys, [][]byte{[]byte("")}) {
		t.Errorf("Keys = %v, want [[]]", root.Keys)
	}
	if !equalByteSlices(root.Values, [][]byte{[]byte("")}) {
		t.Errorf("Values = %v, want [[]]", root.Values)
	}
	if len(root.Children) != 2 || root.Children[0] != 5 || root.Children[1] != 6 {
		t.Errorf("Children = %v, want [5 6]", root.Children)
	}
	if root.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", root.NumKeys)
	}
	if root.IsLeaf() {
		t.Error("IsLeaf = true, want false")
	}
}

func TestNewBranchRoot_LargePageIDs(t *testing.T) {
	leftChild := &base.Node{PageID: 999999}
	rightChild := &base.Node{PageID: 1000000}

	root := NewBranchRoot(leftChild, rightChild, []byte("sep"), []byte("v"), 1)

	if len(root.Children) != 2 || root.Children[0] != 999999 || root.Children[1] != 1000000 {
		t.Errorf("Children = %v, want [999999 1000000]", root.Children)
	}
	if root.PageID != 1 {
		t.Errorf("PageID = %v, want 1", root.PageID)
	}
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
	if !equalByteSlices(parent.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", parent.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{1, 10, 11, 3}
	if len(parent.Children) != 4 {
		t.Fatalf("Children length = %v, want 4", len(parent.Children))
	}
	for i, c := range expectedChildren {
		if parent.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, parent.Children[i], c)
		}
	}
	if parent.NumKeys != 3 {
		t.Errorf("NumKeys = %v, want 3", parent.NumKeys)
	}
	if !parent.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(parent.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", parent.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{10, 11, 2}
	if len(parent.Children) != 3 {
		t.Fatalf("Children length = %v, want 3", len(parent.Children))
	}
	for i, c := range expectedChildren {
		if parent.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, parent.Children[i], c)
		}
	}
	if parent.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", parent.NumKeys)
	}
	if !parent.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(parent.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", parent.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{1, 2, 10, 11}
	if len(parent.Children) != 4 {
		t.Fatalf("Children length = %v, want 4", len(parent.Children))
	}
	for i, c := range expectedChildren {
		if parent.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, parent.Children[i], c)
		}
	}
	if parent.NumKeys != 3 {
		t.Errorf("NumKeys = %v, want 3", parent.NumKeys)
	}
	if !parent.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(parent.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", parent.Keys, expectedKeys)
	}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}
	if !equalByteSlices(parent.Values, expectedValues) {
		t.Errorf("Values = %v, want %v", parent.Values, expectedValues)
	}
	expectedChildren := []base.PageID{10, 11, 2}
	if len(parent.Children) != 3 {
		t.Fatalf("Children length = %v, want 3", len(parent.Children))
	}
	for i, c := range expectedChildren {
		if parent.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, parent.Children[i], c)
		}
	}
	if parent.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", parent.NumKeys)
	}
	if !parent.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(parent.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", parent.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{1, 10, 11}
	if len(parent.Children) != 3 {
		t.Fatalf("Children length = %v, want 3", len(parent.Children))
	}
	for i, c := range expectedChildren {
		if parent.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, parent.Children[i], c)
		}
	}
	if parent.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", parent.NumKeys)
	}
	if !parent.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2")}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("Values = %v, want %v", node.Values, expectedValues)
	}
	if node.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", node.NumKeys)
	}
	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}
	expectedValues := [][]byte{[]byte("v1")}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("Values = %v, want %v", node.Values, expectedValues)
	}
	if node.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", node.NumKeys)
	}
	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}
	if node.Values != nil {
		t.Errorf("Values = %v, want nil", node.Values)
	}
	expectedChildren := []base.PageID{1, 2}
	if len(node.Children) != 2 {
		t.Fatalf("Children length = %v, want 2", len(node.Children))
	}
	for i, c := range expectedChildren {
		if node.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], c)
		}
	}
	if node.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", node.NumKeys)
	}
	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{1, 2, 3}
	if len(node.Children) != 3 {
		t.Fatalf("Children length = %v, want 3", len(node.Children))
	}
	for i, c := range expectedChildren {
		if node.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], c)
		}
	}
	if node.NumKeys != 2 {
		t.Errorf("NumKeys = %v, want 2", node.NumKeys)
	}
	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}
	expectedValues := [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")}
	if !equalByteSlices(node.Values, expectedValues) {
		t.Errorf("Values = %v, want %v", node.Values, expectedValues)
	}
	if node.NumKeys != 3 {
		t.Errorf("NumKeys = %v, want 3", node.NumKeys)
	}
	if !node.Dirty {
		t.Error("Dirty = false, want true")
	}
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
	if !equalByteSlices(node.Keys, expectedKeys) {
		t.Errorf("Keys = %v, want %v", node.Keys, expectedKeys)
	}
	expectedChildren := []base.PageID{10, 20}
	if len(node.Children) != 2 {
		t.Fatalf("Children length = %v, want 2", len(node.Children))
	}
	for i, c := range expectedChildren {
		if node.Children[i] != c {
			t.Errorf("Children[%d] = %v, want %v", i, node.Children[i], c)
		}
	}
	if node.NumKeys != 1 {
		t.Errorf("NumKeys = %v, want 1", node.NumKeys)
	}
	if len(node.Children) != len(node.Keys)+1 {
		t.Errorf("len(Children) = %v, want len(Keys)+1 = %v", len(node.Children), len(node.Keys)+1)
	}
}

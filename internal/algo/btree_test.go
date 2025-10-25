package algo

import (
	"bytes"
	"flag"
	"testing"

	"github.com/alexhholmes/fredb/internal/base"

	"github.com/stretchr/testify/assert"
)

var _ = flag.Bool("slow", false, "run slow tests")

// Helper to create a test node
func makeLeafNode(keys, values [][]byte) *base.Node {
	return &base.Node{
		NumKeys: uint16(len(keys)),
		Keys:    keys,
		Values:  values,
	}
}

func makeBranchNode(keys [][]byte, children []base.PageID) *base.Node {
	return &base.Node{
		NumKeys:  uint16(len(keys)),
		Keys:     keys,
		Children: children,
	}
}

func TestFindChildIndex(t *testing.T) {
	tests := []struct {
		name string
		node *base.Node
		key  []byte
		want int
	}{
		{
			name: "empty_node",
			node: makeBranchNode(nil, []base.PageID{1}),
			key:  []byte("key"),
			want: 0,
		},
		{
			name: "key_less_than_first",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("a"),
			want: 0,
		},
		{
			name: "key_equal_first",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("b"),
			want: 1,
		},
		{
			name: "key_between_keys",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("c"),
			want: 1,
		},
		{
			name: "key_equal_last",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("d"),
			want: 2,
		},
		{
			name: "key_greater_than_all",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("z"),
			want: 2,
		},
		{
			name: "single_key",
			node: makeBranchNode([][]byte{[]byte("m")}, []base.PageID{1, 2}),
			key:  []byte("a"),
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindChildIndex(tt.node, tt.key)
			assert.Equal(t, tt.want, got, "FindChildIndex should return correct child index")
		})
	}
}

func TestFindKeyInLeaf(t *testing.T) {
	tests := []struct {
		name string
		node *base.Node
		key  []byte
		want int
	}{
		{
			name: "empty_leaf",
			node: makeLeafNode(nil, nil),
			key:  []byte("key"),
			want: -1,
		},
		{
			name: "key_found_first",
			node: makeLeafNode([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("a"),
			want: 0,
		},
		{
			name: "key_found_middle",
			node: makeLeafNode([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("b"),
			want: 1,
		},
		{
			name: "key_found_last",
			node: makeLeafNode([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("c"),
			want: 2,
		},
		{
			name: "key_not_found",
			node: makeLeafNode([][]byte{[]byte("a"), []byte("b"), []byte("c")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("d"),
			want: -1,
		},
		{
			name: "branch_node_returns_minus_one",
			node: makeBranchNode([][]byte{[]byte("a"), []byte("b")}, []base.PageID{1, 2, 3}),
			key:  []byte("a"),
			want: -1,
		},
		{
			name: "single_key_found",
			node: makeLeafNode([][]byte{[]byte("key")}, [][]byte{[]byte("val")}),
			key:  []byte("key"),
			want: 0,
		},
		{
			name: "single_key_not_found",
			node: makeLeafNode([][]byte{[]byte("key")}, [][]byte{[]byte("val")}),
			key:  []byte("other"),
			want: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindKeyInLeaf(tt.node, tt.key)
			assert.Equal(t, tt.want, got, "FindKeyInLeaf should return correct index")
		})
	}
}

func TestFindInsertPosition(t *testing.T) {
	tests := []struct {
		name string
		node *base.Node
		key  []byte
		want int
	}{
		{
			name: "empty_node",
			node: makeLeafNode(nil, nil),
			key:  []byte("key"),
			want: 0,
		},
		{
			name: "insert_before_all",
			node: makeLeafNode([][]byte{[]byte("b"), []byte("d"), []byte("f")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("a"),
			want: 0,
		},
		{
			name: "insert_between_first_and_second",
			node: makeLeafNode([][]byte{[]byte("b"), []byte("d"), []byte("f")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("c"),
			want: 1,
		},
		{
			name: "insert_between_second_and_third",
			node: makeLeafNode([][]byte{[]byte("b"), []byte("d"), []byte("f")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("e"),
			want: 2,
		},
		{
			name: "insert_after_all",
			node: makeLeafNode([][]byte{[]byte("b"), []byte("d"), []byte("f")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("z"),
			want: 3,
		},
		{
			name: "insert_equal_to_first_goes_before",
			node: makeLeafNode([][]byte{[]byte("b"), []byte("d"), []byte("f")}, [][]byte{[]byte("1"), []byte("2"), []byte("3")}),
			key:  []byte("b"),
			want: 0,
		},
		{
			name: "single_key_insert_before",
			node: makeLeafNode([][]byte{[]byte("m")}, [][]byte{[]byte("1")}),
			key:  []byte("a"),
			want: 0,
		},
		{
			name: "single_key_insert_after",
			node: makeLeafNode([][]byte{[]byte("m")}, [][]byte{[]byte("1")}),
			key:  []byte("z"),
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindInsertPosition(tt.node, tt.key)
			if got != tt.want {
				t.Errorf("FindInsertPosition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindDeleteChildIndex(t *testing.T) {
	tests := []struct {
		name string
		node *base.Node
		key  []byte
		want int
	}{
		{
			name: "empty_node",
			node: makeBranchNode(nil, []base.PageID{1}),
			key:  []byte("key"),
			want: 0,
		},
		{
			name: "key_less_than_first",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("a"),
			want: 0,
		},
		{
			name: "key_equal_first",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("b"),
			want: 1,
		},
		{
			name: "key_between_keys",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("c"),
			want: 1,
		},
		{
			name: "key_equal_last",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("d"),
			want: 2,
		},
		{
			name: "key_greater_than_all",
			node: makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3}),
			key:  []byte("z"),
			want: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FindDeleteChildIndex(tt.node, tt.key)
			if got != tt.want {
				t.Errorf("FindDeleteChildIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCalculateSplitPoint(t *testing.T) {
	// Create a leaf with enough keys to split
	makeFullLeaf := func(n int) *base.Node {
		keys := make([][]byte, n)
		vals := make([][]byte, n)
		for i := 0; i < n; i++ {
			keys[i] = []byte{byte('a' + i)}
			vals[i] = []byte{byte('0' + i)}
		}
		return makeLeafNode(keys, vals)
	}

	makeFullBranch := func(n int) *base.Node {
		keys := make([][]byte, n)
		children := make([]base.PageID, n+1)
		for i := 0; i < n; i++ {
			keys[i] = []byte{byte('a' + i)}
			children[i] = base.PageID(i + 1)
		}
		children[n] = base.PageID(n + 1)
		return makeBranchNode(keys, children)
	}

	tests := []struct {
		name               string
		node               *base.Node
		insertKey          []byte
		hint               SplitHint
		wantMid            int
		wantLeftCount      int
		wantRightCount     int
		wantSeparatorEqual []byte
	}{
		{
			name:               "balanced_leaf_typical_split",
			node:               makeFullLeaf(10),
			insertKey:          []byte{'e'}, // Middle key - balanced
			hint:               SplitBalanced,
			wantMid:            4,
			wantLeftCount:      5,
			wantRightCount:     5,
			wantSeparatorEqual: []byte{'f'}, // Keys[mid+1]=Keys[5] for leaf
		},
		{
			name:               "ascending_leaf_split_right_bias",
			node:               makeFullLeaf(10),
			insertKey:          []byte{'z'}, // Beyond rightmost → RightBias
			hint:               SplitBalanced,
			wantMid:            8,           // 90% of 10 = 9, clamped to len-2 = 8
			wantLeftCount:      9,           // Left gets most keys
			wantRightCount:     1,           // Right gets one key
			wantSeparatorEqual: []byte{'j'}, // Keys[mid+1]=Keys[9]
		},
		{
			name:               "descending_leaf_split_left_bias",
			node:               makeFullLeaf(10),
			insertKey:          []byte{'0'}, // Before leftmost → LeftBias
			hint:               SplitBalanced,
			wantMid:            1,           // 10% left
			wantLeftCount:      2,           // Left gets minimal keys
			wantRightCount:     8,           // Right gets most
			wantSeparatorEqual: []byte{'c'}, // Keys[mid+1]=Keys[2]
		},
		{
			name:               "branch_balanced_split",
			node:               makeFullBranch(10),
			insertKey:          []byte{'e'},
			hint:               SplitBalanced,
			wantMid:            4,
			wantLeftCount:      4,
			wantRightCount:     5,
			wantSeparatorEqual: []byte{'e'}, // Keys[mid] for branch
		},
		{
			name:               "branch_ascending_split",
			node:               makeFullBranch(10),
			insertKey:          []byte{'z'},
			hint:               SplitBalanced,
			wantMid:            8,           // Clamped to len-2
			wantLeftCount:      8,           // Most keys left
			wantRightCount:     1,           // Minimal right
			wantSeparatorEqual: []byte{'i'}, // Keys[mid]
		},
		{
			name:               "explicit_right_bias_hint",
			node:               makeFullLeaf(10),
			insertKey:          nil,
			hint:               SplitRightBias,
			wantMid:            8,
			wantLeftCount:      9,
			wantRightCount:     1,
			wantSeparatorEqual: []byte{'j'}, // Keys[mid+1]=Keys[9]
		},
		{
			name:               "explicit_left_bias_hint",
			node:               makeFullLeaf(10),
			insertKey:          nil,
			hint:               SplitLeftBias,
			wantMid:            1,
			wantLeftCount:      2,
			wantRightCount:     8,
			wantSeparatorEqual: []byte{'c'}, // Keys[mid+1]=Keys[2]
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sp := CalculateSplitPointWithHint(tt.node, tt.insertKey, tt.hint)

			if sp.Mid != tt.wantMid {
				t.Errorf("Mid = %v, want %v", sp.Mid, tt.wantMid)
			}
			if sp.LeftCount != tt.wantLeftCount {
				t.Errorf("LeftCount = %v, want %v", sp.LeftCount, tt.wantLeftCount)
			}
			if sp.RightCount != tt.wantRightCount {
				t.Errorf("RightCount = %v, want %v", sp.RightCount, tt.wantRightCount)
			}
			if !bytes.Equal(sp.SeparatorKey, tt.wantSeparatorEqual) {
				t.Errorf("SeparatorKey = %v, want %v", string(sp.SeparatorKey), string(tt.wantSeparatorEqual))
			}

			// Verify separator is a copy, not shared
			if tt.node.Type() == base.LeafType && sp.Mid+1 < len(tt.node.Keys) {
				if len(sp.SeparatorKey) > 0 && len(tt.node.Keys[sp.Mid+1]) > 0 {
					if &sp.SeparatorKey[0] == &tt.node.Keys[sp.Mid+1][0] {
						t.Error("SeparatorKey shares backing array with original (not CoW safe)")
					}
				}
			} else if tt.node.Type() == base.BranchType && sp.Mid < len(tt.node.Keys) {
				if len(sp.SeparatorKey) > 0 && len(tt.node.Keys[sp.Mid]) > 0 {
					if &sp.SeparatorKey[0] == &tt.node.Keys[sp.Mid][0] {
						t.Error("SeparatorKey shares backing array with original (not CoW safe)")
					}
				}
			}
		})
	}
}

func TestExtractRightPortion(t *testing.T) {
	tests := []struct {
		name           string
		node           *base.Node
		sp             SplitPoint
		wantKeysCount  int
		wantValsCount  int
		wantChildCount int
	}{
		{
			name: "leaf_extract",
			node: makeLeafNode(
				[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")},
				[][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5")},
			),
			sp:             SplitPoint{Mid: 2, LeftCount: 3, RightCount: 2},
			wantKeysCount:  2,
			wantValsCount:  2,
			wantChildCount: 0,
		},
		{
			name: "branch_extract",
			node: makeBranchNode(
				[][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")},
				[]base.PageID{1, 2, 3, 4, 5, 6},
			),
			sp:             SplitPoint{Mid: 2, LeftCount: 2, RightCount: 2},
			wantKeysCount:  2,
			wantValsCount:  0,
			wantChildCount: 3, // children[3..5] = 3 children
		},
		{
			name: "leaf_minimal_extract",
			node: makeLeafNode(
				[][]byte{[]byte("a"), []byte("b")},
				[][]byte{[]byte("1"), []byte("2")},
			),
			sp:             SplitPoint{Mid: 0, LeftCount: 1, RightCount: 1},
			wantKeysCount:  1,
			wantValsCount:  1,
			wantChildCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalKeys := make([][]byte, len(tt.node.Keys))
			copy(originalKeys, tt.node.Keys)

			keys, vals, children := ExtractRightPortion(tt.node, tt.sp)

			if len(keys) != tt.wantKeysCount {
				t.Errorf("keys count = %v, want %v", len(keys), tt.wantKeysCount)
			}
			if len(vals) != tt.wantValsCount {
				t.Errorf("vals count = %v, want %v", len(vals), tt.wantValsCount)
			}
			if len(children) != tt.wantChildCount {
				t.Errorf("children count = %v, want %v", len(children), tt.wantChildCount)
			}

			// Verify slice headers are new (CoW creates new [][]byte)
			// Individual []byte backing arrays CAN be shared (immutable under CoW)
			if len(keys) > 0 {
				// Slice header must be different (new array of pointers)
				if &keys == &tt.node.Keys {
					t.Error("extracted keys slice header is same as original (should be new)")
				}
			}

			// Verify original node is unchanged
			for i, key := range originalKeys {
				if !bytes.Equal(key, tt.node.Keys[i]) {
					t.Error("ExtractRightPortion modified original node keys")
				}
			}
		})
	}
}

func TestCanBorrowFrom(t *testing.T) {
	// MinFillRatio = 0.25, PageSize = 4096, so MinSize = 1024 bytes
	// Create nodes with specific sizes to test borrowing threshold

	makeKeys := func(count, size int) [][]byte {
		keys := make([][]byte, count)
		for i := 0; i < count; i++ {
			keys[i] = make([]byte, size)
		}
		return keys
	}
	makeValues := func(count, size int) [][]byte {
		values := make([][]byte, count)
		for i := 0; i < count; i++ {
			values[i] = make([]byte, size)
		}
		return values
	}

	tests := []struct {
		name string
		node *base.Node
		want bool
	}{
		{
			name: "can_borrow_above_min",
			// 20 keys * 100 bytes/entry = ~2344 bytes > 1024
			node: makeLeafNode(
				makeKeys(20, 50),
				makeValues(20, 50),
			),
			want: true,
		},
		{
			name: "cannot_borrow_at_min",
			// 16 keys * 46 bytes/entry = ~1016 bytes < 1024
			node: makeLeafNode(
				makeKeys(16, 23),
				makeValues(16, 23),
			),
			want: false,
		},
		{
			name: "cannot_borrow_below_min",
			// 10 keys * 46 bytes/entry = ~644 bytes < 1024
			node: makeLeafNode(
				makeKeys(10, 23),
				makeValues(10, 23),
			),
			want: false,
		},
		{
			name: "empty_node_cannot_borrow",
			node: makeLeafNode(nil, nil),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanBorrowFrom(tt.node)
			if got != tt.want {
				t.Errorf("CanBorrowFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractLastFromSibling(t *testing.T) {
	tests := []struct {
		name      string
		sibling   *base.Node
		wantKey   []byte
		wantValue []byte
		wantChild base.PageID
	}{
		{
			name: "leaf_extract_last",
			sibling: makeLeafNode(
				[][]byte{[]byte("a"), []byte("b"), []byte("c")},
				[][]byte{[]byte("1"), []byte("2"), []byte("3")},
			),
			wantKey:   []byte("c"),
			wantValue: []byte("3"),
			wantChild: 0,
		},
		{
			name: "branch_extract_last",
			sibling: makeBranchNode(
				[][]byte{[]byte("a"), []byte("b"), []byte("c")},
				[]base.PageID{1, 2, 3, 4},
			),
			wantKey:   []byte("c"),
			wantValue: nil,
			wantChild: 4,
		},
		{
			name: "single_key_leaf",
			sibling: makeLeafNode(
				[][]byte{[]byte("only")},
				[][]byte{[]byte("val")},
			),
			wantKey:   []byte("only"),
			wantValue: []byte("val"),
			wantChild: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := ExtractLastFromSibling(tt.sibling)

			if !bytes.Equal(data.Key, tt.wantKey) {
				t.Errorf("Key = %v, want %v", data.Key, tt.wantKey)
			}
			if !bytes.Equal(data.Value, tt.wantValue) {
				t.Errorf("Value = %v, want %v", data.Value, tt.wantValue)
			}
			if data.Child != tt.wantChild {
				t.Errorf("Child = %v, want %v", data.Child, tt.wantChild)
			}
		})
	}
}

func TestExtractFirstFromSibling(t *testing.T) {
	tests := []struct {
		name      string
		sibling   *base.Node
		wantKey   []byte
		wantValue []byte
		wantChild base.PageID
	}{
		{
			name: "leaf_extract_first",
			sibling: makeLeafNode(
				[][]byte{[]byte("a"), []byte("b"), []byte("c")},
				[][]byte{[]byte("1"), []byte("2"), []byte("3")},
			),
			wantKey:   []byte("a"),
			wantValue: []byte("1"),
			wantChild: 0,
		},
		{
			name: "branch_extract_first",
			sibling: makeBranchNode(
				[][]byte{[]byte("a"), []byte("b"), []byte("c")},
				[]base.PageID{1, 2, 3, 4},
			),
			wantKey:   []byte("a"),
			wantValue: nil,
			wantChild: 1,
		},
		{
			name: "single_key_leaf",
			sibling: makeLeafNode(
				[][]byte{[]byte("only")},
				[][]byte{[]byte("val")},
			),
			wantKey:   []byte("only"),
			wantValue: []byte("val"),
			wantChild: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := ExtractFirstFromSibling(tt.sibling)

			if !bytes.Equal(data.Key, tt.wantKey) {
				t.Errorf("Key = %v, want %v", data.Key, tt.wantKey)
			}
			if !bytes.Equal(data.Value, tt.wantValue) {
				t.Errorf("Value = %v, want %v", data.Value, tt.wantValue)
			}
			if data.Child != tt.wantChild {
				t.Errorf("Child = %v, want %v", data.Child, tt.wantChild)
			}
		})
	}
}

func TestInsertAt(t *testing.T) {
	tests := []struct {
		name  string
		slice [][]byte
		index int
		value []byte
		want  [][]byte
	}{
		{
			name:  "insert_at_beginning",
			slice: [][]byte{[]byte("b"), []byte("c")},
			index: 0,
			value: []byte("a"),
			want:  [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name:  "insert_in_middle",
			slice: [][]byte{[]byte("a"), []byte("c")},
			index: 1,
			value: []byte("b"),
			want:  [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name:  "insert_at_end",
			slice: [][]byte{[]byte("a"), []byte("b")},
			index: 2,
			value: []byte("c"),
			want:  [][]byte{[]byte("a"), []byte("b"), []byte("c")},
		},
		{
			name:  "insert_into_empty",
			slice: [][]byte{},
			index: 0,
			value: []byte("a"),
			want:  [][]byte{[]byte("a")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InsertAt(tt.slice, tt.index, tt.value)

			if len(got) != len(tt.want) {
				t.Fatalf("length = %v, want %v", len(got), len(tt.want))
			}

			for i := range got {
				if !bytes.Equal(got[i], tt.want[i]) {
					t.Errorf("got[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestRemoveAt(t *testing.T) {
	tests := []struct {
		name  string
		slice [][]byte
		index int
		want  [][]byte
	}{
		{
			name:  "remove_first",
			slice: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			index: 0,
			want:  [][]byte{[]byte("b"), []byte("c")},
		},
		{
			name:  "remove_middle",
			slice: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			index: 1,
			want:  [][]byte{[]byte("a"), []byte("c")},
		},
		{
			name:  "remove_last",
			slice: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			index: 2,
			want:  [][]byte{[]byte("a"), []byte("b")},
		},
		{
			name:  "remove_only_element",
			slice: [][]byte{[]byte("a")},
			index: 0,
			want:  [][]byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RemoveAt(tt.slice, tt.index)

			if len(got) != len(tt.want) {
				t.Fatalf("length = %v, want %v", len(got), len(tt.want))
			}

			for i := range got {
				if !bytes.Equal(got[i], tt.want[i]) {
					t.Errorf("got[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestRemoveChildAt(t *testing.T) {
	tests := []struct {
		name  string
		slice []base.PageID
		index int
		want  []base.PageID
	}{
		{
			name:  "remove_first",
			slice: []base.PageID{1, 2, 3},
			index: 0,
			want:  []base.PageID{2, 3},
		},
		{
			name:  "remove_middle",
			slice: []base.PageID{1, 2, 3},
			index: 1,
			want:  []base.PageID{1, 3},
		},
		{
			name:  "remove_last",
			slice: []base.PageID{1, 2, 3},
			index: 2,
			want:  []base.PageID{1, 2},
		},
		{
			name:  "remove_only_element",
			slice: []base.PageID{1},
			index: 0,
			want:  []base.PageID{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RemoveChildAt(tt.slice, tt.index)

			if len(got) != len(tt.want) {
				t.Fatalf("length = %v, want %v", len(got), len(tt.want))
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("got[%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// Test that read-only functions don't mutate input
func TestReadOnlyBehavior(t *testing.T) {
	t.Run("FindChildIndex_no_mutation", func(t *testing.T) {
		node := makeBranchNode([][]byte{[]byte("b"), []byte("d")}, []base.PageID{1, 2, 3})
		originalNumKeys := node.NumKeys
		_ = FindChildIndex(node, []byte("c"))
		if node.NumKeys != originalNumKeys {
			t.Error("FindChildIndex mutated node")
		}
	})

	t.Run("FindKeyInLeaf_no_mutation", func(t *testing.T) {
		node := makeLeafNode([][]byte{[]byte("a")}, [][]byte{[]byte("1")})
		originalNumKeys := node.NumKeys
		_ = FindKeyInLeaf(node, []byte("a"))
		if node.NumKeys != originalNumKeys {
			t.Error("FindKeyInLeaf mutated node")
		}
	})
}

// Test NumKeys consistency
func TestNumKeysConsistency(t *testing.T) {
	t.Run("NumKeys_matches_Keys_length", func(t *testing.T) {
		keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		node := makeLeafNode(keys, [][]byte{[]byte("1"), []byte("2"), []byte("3")})

		if int(node.NumKeys) != len(node.Keys) {
			t.Errorf("NumKeys = %v, len(Keys) = %v", node.NumKeys, len(node.Keys))
		}

		// Test with FindChildIndex which uses NumKeys
		idx := FindChildIndex(node, []byte("d"))
		if idx < 0 || idx > len(node.Keys) {
			t.Errorf("FindChildIndex returned out of bounds index: %v", idx)
		}
	})
}

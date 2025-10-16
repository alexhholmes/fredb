package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNodeCloneCoWSafety verifies that cloning creates shallow copies
// that preserve CoW semantics when pointers are replaced
func TestNodeCloneCoWSafety(t *testing.T) {
	t.Run("leaf node clone isolation", func(t *testing.T) {
		// Create original leaf node
		original := &Node{
			PageID:  42,
			Dirty:   false,
			NumKeys: 2,
			Keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
			},
			Values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
			},
		}

		// Clone the node
		cloned := original.Clone()

		// Verify clone properties
		assert.Equal(t, PageID(0), cloned.PageID, "clone should have zero PageID")
		assert.True(t, cloned.Dirty, "clone should be marked dirty")
		assert.Equal(t, original.NumKeys, cloned.NumKeys)

		// Verify shallow copy: pointers should be the same
		assert.Equal(t, &original.Keys[0][0], &cloned.Keys[0][0], "shallow copy should share backing arrays")
		assert.Equal(t, &original.Values[0][0], &cloned.Values[0][0], "shallow copy should share backing arrays")

		// Safe modification: replace pointer entirely
		newKey := []byte("newkey1")
		newValue := []byte("newvalue1")
		cloned.Keys[0] = newKey
		cloned.Values[0] = newValue

		// Verify CoW: original unchanged
		assert.Equal(t, []byte("key1"), original.Keys[0], "original key should be unchanged")
		assert.Equal(t, []byte("value1"), original.Values[0], "original value should be unchanged")
		assert.Equal(t, newKey, cloned.Keys[0], "cloned key should have new value")
		assert.Equal(t, newValue, cloned.Values[0], "cloned value should have new value")

		// Verify pointer replacement (not sharing)
		assert.NotEqual(t, &original.Keys[0][0], &cloned.Keys[0][0], "after replacement, should not share backing array")
		assert.NotEqual(t, &original.Values[0][0], &cloned.Values[0][0], "after replacement, should not share backing array")
	})

	t.Run("branch node clone isolation", func(t *testing.T) {
		// Create original branch node
		original := &Node{
			PageID:  100,
			Dirty:   false,
			NumKeys: 2,
			Keys: [][]byte{
				[]byte("sep1"),
				[]byte("sep2"),
			},
			Values: nil,
			Children: []PageID{10, 20, 30},
		}

		// Clone the node
		cloned := original.Clone()

		// Verify clone properties
		assert.Equal(t, PageID(0), cloned.PageID)
		assert.True(t, cloned.Dirty)
		assert.Equal(t, original.NumKeys, cloned.NumKeys)
		assert.Nil(t, cloned.Values)

		// Verify shallow copy
		assert.Equal(t, &original.Keys[0][0], &cloned.Keys[0][0], "shallow copy should share backing arrays")

		// Safe modification: replace pointer
		newKey := []byte("newsep1")
		cloned.Keys[0] = newKey
		cloned.Children[1] = PageID(999)

		// Verify CoW: original unchanged
		assert.Equal(t, []byte("sep1"), original.Keys[0], "original key should be unchanged")
		assert.Equal(t, PageID(20), original.Children[1], "original child should be unchanged")
		assert.Equal(t, newKey, cloned.Keys[0], "cloned key should have new value")
		assert.Equal(t, PageID(999), cloned.Children[1], "cloned child should have new value")
	})

	t.Run("unsafe mutation would corrupt original", func(t *testing.T) {
		// This test demonstrates what NOT to do
		// If you mutate the byte array contents directly, you break CoW

		original := &Node{
			PageID:  1,
			Dirty:   false,
			NumKeys: 1,
			Keys:    [][]byte{[]byte("test")},
			Values:  [][]byte{[]byte("value")},
		}

		cloned := original.Clone()

		// UNSAFE: mutating the byte array contents directly
		// This WOULD corrupt the original (but we expect it because of shallow copy)
		cloned.Keys[0][0] = 'X'

		// This demonstrates the shared backing array - both are affected
		assert.Equal(t, byte('X'), original.Keys[0][0], "shallow copy shares backing array, so mutation affects both")
		assert.Equal(t, byte('X'), cloned.Keys[0][0], "mutation affects cloned node as expected")

		// The SAFE pattern is to replace the pointer instead:
		cloned.Keys[0] = []byte("safe")
		assert.Equal(t, []byte("Xest"), original.Keys[0], "original still has mutated data")
		assert.Equal(t, []byte("safe"), cloned.Keys[0], "cloned has new allocation")
	})
}
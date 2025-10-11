package fredb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fredb/internal/base"
)

// Basic Operations Tests

func TestBTreeBasicOps(t *testing.T) {
	t.Parallel()

	// Test basic get/Set operations
	db, _ := setup(t)

	// Insert key-value pair
	err := db.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// get existing key
	val, err := db.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(val))

	// Update existing key
	err = db.Set([]byte("key1"), []byte("value2"))
	assert.NoError(t, err)

	val, err = db.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value2", string(val))

	// get non-existent key (should return ErrKeyNotFound)
	_, err = db.Get([]byte("nonexistent"))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestBTreeUpdate(t *testing.T) {
	t.Parallel()

	// Test that Set updates existing Keys rather than duplicating
	db, _ := setup(t)

	// Insert key with value1
	err := db.Set([]byte("testkey"), []byte("value1"))
	assert.NoError(t, err)

	// Update same key with value2
	err = db.Set([]byte("testkey"), []byte("value2"))
	assert.NoError(t, err)

	// Verify get returns value2
	val, err := db.Get([]byte("testkey"))
	assert.NoError(t, err)
	assert.Equal(t, "value2", string(val))

	// Insert more Keys to ensure no duplication
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		db.Set(key, val)
	}

	// Update the original key again
	err = db.Set([]byte("testkey"), []byte("value3"))
	assert.NoError(t, err)

	val, err = db.Get([]byte("testkey"))
	assert.NoError(t, err)
	assert.Equal(t, "value3", string(val))
}

// Node Splitting Tests

func TestBTreeSplitting(t *testing.T) {
	t.Parallel()

	// Test Node splitting when exceeding MaxKeysPerNode
	db, _ := setup(t)

	// Insert MaxKeysPerNode + 1 Keys to force a split
	keys := make(map[string]string)
	for i := 0; i <= base.MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify root splits (root should no longer be a leaf)
	assert.False(t, db.root.Load().IsLeaf, "Root should not be a leaf after splitting")

	// Check tree height increases (root should have Children)
	assert.NotEmpty(t, db.root.Load().Children, "Root should have Children after splitting")

	// Verify all Keys still retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}
}

func TestBTreeMultipleSplits(t *testing.T) {
	t.Parallel()

	// Test multiple levels of splitting
	db, _ := setup(t)

	// Insert enough Keys to cause multiple splits (3x MaxKeysPerNode should cause multiple levels)
	numKeys := base.MaxKeysPerNode * 3
	keys := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify tree structure remains valid (root is not a leaf for large tree)
	assert.False(t, db.root.Load().IsLeaf, "Root should not be a leaf after multiple splits")

	// Verify root has multiple Children
	assert.GreaterOrEqual(t, len(db.root.Load().Children), 2, "Root should have multiple Children after multiple splits")

	// All Keys retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Verify we can still insert after multiple splits
	testKey := []byte("test_after_splits")
	testValue := []byte("test_value")
	err := db.Set(testKey, testValue)
	assert.NoError(t, err)

	val, err := db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(testValue), string(val))
}

// Sequential vs Random Insert Tests

func TestSequentialInsert(t *testing.T) {
	t.Parallel()

	// Test inserting Keys in sequential order
	db, _ := setup(t)

	// Insert 1000 sequential Keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Check tree structure (likely right-heavy due to sequential insert)
	if db.root.Load().IsLeaf {
		t.Logf("Tree has single leaf root after %d sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d sequential inserts",
			db.root.Load().NumKeys, len(db.root.Load().Children), numKeys)
	}
}

func TestRandomInsert(t *testing.T) {
	t.Parallel()

	// Test inserting Keys in random order
	db, _ := setup(t)

	// Generate random Keys (using deterministic seed for reproducibility)
	numKeys := 1000
	keys := make(map[string]string)

	// Create Keys in random order using a simple shuffle
	indices := make([]int, numKeys)
	for i := 0; i < numKeys; i++ {
		indices[i] = i
	}

	// Simple deterministic shuffle
	for i := len(indices) - 1; i > 0; i-- {
		j := (i * 7) % (i + 1) // Deterministic "random"
		indices[i], indices[j] = indices[j], indices[i]
	}

	// Insert Keys in shuffled order
	for _, idx := range indices {
		key := fmt.Sprintf("key%08d", idx)
		value := fmt.Sprintf("value%08d", idx)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify all retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Check tree balance (random insertion typically produces more balanced trees)
	if !db.root.Load().IsLeaf {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d random inserts",
			db.root.Load().NumKeys, len(db.root.Load().Children), numKeys)

		// Check if root has reasonable number of Children (indicating some balance)
		if len(db.root.Load().Children) > 1 && len(db.root.Load().Children) < 10 {
			t.Logf("Tree appears relatively balanced with %d root Children", len(db.root.Load().Children))
		}
	}
}

func TestReverseSequentialInsert(t *testing.T) {
	t.Parallel()

	// Test inserting Keys in reverse order
	db, _ := setup(t)

	// Insert 1000 Keys in descending order
	numKeys := 1000
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Check tree structure (likely left-heavy due to reverse sequential insert)
	if db.root.Load().IsLeaf {
		t.Logf("Tree has single leaf root after %d reverse sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d reverse sequential inserts",
			db.root.Load().NumKeys, len(db.root.Load().Children), numKeys)

		// With reverse insertion, we expect most activity on the left side of the tree
		if len(db.root.Load().Children) > 0 {
			t.Logf("First child likely contains lower Keys due to reverse insertion pattern")
		}
	}
}

// Delete Tests

func TestBTreeDelete(t *testing.T) {
	t.Parallel()

	// Test basic delete operations
	db, _ := setup(t)

	// Insert some Keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, k := range keys {
		err := db.Set([]byte(k), []byte("value-"+k))
		assert.NoError(t, err)
	}

	// Delete middle key
	err := db.Delete([]byte("key3"))
	assert.NoError(t, err)

	// Verify key3 is gone
	_, err = db.Get([]byte("key3"))
	assert.Equal(t, ErrKeyNotFound, err)

	// Verify other Keys still exist
	for _, k := range []string{"key1", "key2", "key4", "key5"} {
		val, err := db.Get([]byte(k))
		if assert.NoError(t, err) {
			assert.Equal(t, "value-"+k, string(val))
		}
	}

	// Delete first key
	err = db.Delete([]byte("key1"))
	assert.NoError(t, err)

	// Delete last key
	err = db.Delete([]byte("key5"))
	assert.NoError(t, err)

	// Delete non-existent key
	err = db.Delete([]byte("nonexistent"))
	assert.Equal(t, ErrKeyNotFound, err)

	// Verify remaining Keys
	for _, k := range []string{"key2", "key4"} {
		val, err := db.Get([]byte(k))
		if assert.NoError(t, err) {
			assert.Equal(t, "value-"+k, string(val))
		}
	}
}

func TestBTreeDeleteAll(t *testing.T) {
	t.Parallel()

	// Test deleting all Keys from tree
	db, _ := setup(t)

	// Insert and then delete Keys one by one
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Delete all Keys in order
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Verify deleted
		_, err = db.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err)

		// Verify remaining Keys still exist
		for j := i + 1; j < numKeys && j < i+5; j++ {
			checkKey := fmt.Sprintf("key%04d", j)
			val, err := db.Get([]byte(checkKey))
			if assert.NoError(t, err) {
				expectedVal := fmt.Sprintf("value%04d", j)
				assert.Equal(t, expectedVal, string(val))
			}
		}
	}

	// Tree should be empty now
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		_, err := db.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

func TestBTreeSequentialDelete(t *testing.T) {
	t.Parallel()

	// Test sequential deletion pattern with tree structure checks
	db, _ := setup(t)

	// Insert enough Keys to create a multi-level tree
	numKeys := base.MaxKeysPerNode * 2 // Enough to cause splits
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Check initial tree structure
	initialIsLeaf := db.root.Load().IsLeaf
	initialRootKeys := int(db.root.Load().NumKeys)
	t.Logf("Initial tree: root IsLeaf=%v, NumKeys=%d", initialIsLeaf, initialRootKeys)

	// Delete Keys sequentially and monitor tree structure
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Log tree structure changes at key points
		if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
			t.Logf("After %d deletions: root IsLeaf=%v, NumKeys=%d",
				i+1, db.root.Load().IsLeaf, db.root.Load().NumKeys)
		}

		// Verify key is deleted
		_, err = db.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err)
	}

	// Final tree should be empty
	assert.Equal(t, uint16(0), db.root.Load().NumKeys, "Tree should be empty")
	assert.False(t, db.root.Load().IsLeaf, "Empty tree root should be branch")
}

func TestBTreeRandomDelete(t *testing.T) {
	t.Parallel()

	if !*slow {
		t.Skip("Skipping slow test; use -slow to enable")
	}

	for i := 0; i < 10; i++ {
		// Test random deletion pattern with tree structure checks
		db, _ := setup(t)

		// Insert Keys
		numKeys := base.MaxKeysPerNode * 2
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%06d", i)
			value := fmt.Sprintf("value%06d", i)
			keys[i] = key
			err := db.Set([]byte(key), []byte(value))
			assert.NoError(t, err)
		}

		// Check initial tree structure
		initialIsLeaf := db.root.Load().IsLeaf
		initialRootKeys := int(db.root.Load().NumKeys)
		t.Logf("Initial tree: root IsLeaf=%v, NumKeys=%d", initialIsLeaf, initialRootKeys)

		// Create random deletion order
		deleteOrder := make([]int, numKeys)
		for i := 0; i < numKeys; i++ {
			deleteOrder[i] = i
		}
		// Deterministic shuffle for reproducibility
		for i := len(deleteOrder) - 1; i > 0; i-- {
			j := (i * 7) % (i + 1)
			deleteOrder[i], deleteOrder[j] = deleteOrder[j], deleteOrder[i]
		}

		// Track which Keys are deleted
		deleted := make(map[string]bool)

		// Delete Keys randomly and monitor tree structure
		for i, idx := range deleteOrder {
			key := keys[idx]
			err := db.Delete([]byte(key))
			assert.NoError(t, err)
			deleted[key] = true

			// Log tree structure changes at key points
			if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
				t.Logf("After %d random deletions: root IsLeaf=%v, NumKeys=%d",
					i+1, db.root.Load().IsLeaf, db.root.Load().NumKeys)
			}

			// Verify deleted key is gone
			_, err = db.Get([]byte(key))
			assert.Equal(t, ErrKeyNotFound, err)

			// Verify some non-deleted Keys still exist (spot active)
			checked := 0
			for _, k := range keys {
				if !deleted[k] && checked < 5 {
					val, err := db.Get([]byte(k))
					if assert.NoError(t, err) {
						expectedVal := "value" + k[3:] // Concatenate value prefix with key suffix
						assert.Equal(t, expectedVal, string(val))
					}
					checked++
				}
			}
		}

		// Final tree should be empty
		assert.Equal(t, uint16(0), db.root.Load().NumKeys, "Tree should be empty")
		assert.False(t, db.root.Load().IsLeaf, "Empty tree root should be branch")

		// close database after each iteration
		db.Close()
	}
}

func TestBTreeReverseDelete(t *testing.T) {
	t.Parallel()

	// Test reverse sequential deletion pattern
	db, _ := setup(t)

	// Insert Keys
	numKeys := base.MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Check initial tree structure
	t.Logf("Initial tree: root IsLeaf=%v, NumKeys=%d", db.root.Load().IsLeaf, db.root.Load().NumKeys)

	// Delete Keys in reverse order
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Log tree structure at key points
		deletedCount := numKeys - i
		if deletedCount == numKeys/4 || deletedCount == numKeys/2 || deletedCount == 3*numKeys/4 {
			t.Logf("After %d reverse deletions: root IsLeaf=%v, NumKeys=%d",
				deletedCount, db.root.Load().IsLeaf, db.root.Load().NumKeys)
		}

		// Verify key is deleted
		_, err = db.Get([]byte(key))
		assert.Equal(t, ErrKeyNotFound, err)
	}

	// Final tree should be empty
	assert.Equal(t, uint16(0), db.root.Load().NumKeys, "Tree should be empty")
}

// Stress Tests

func TestBTreeStress(t *testing.T) {
	// Stress test with large number of operations
	// - Insert 10000 random key-value pairs
	// - Perform random get operations
	// - Update random existing Keys
	// - Verify data integrity throughout
	t.Skip("Not implemented")
}

func TestBTreeLargeValues(t *testing.T) {
	// Test with large value sizes
	// - Insert Keys with 1KB, 10KB Values
	// - Verify retrieval
	// - Check memory usage patterns
	t.Skip("Not implemented")
}

// PageManager Integration Tests

func TestPageCaching(t *testing.T) {
	// Test Page cache behavior
	// - Verify cache hit/miss behavior
	// - Test cache population on reads
	// - Verify Dirty Page tracking
	t.Skip("Not implemented")
}

func TestCloseFlush(t *testing.T) {
	// Test close() flushes Dirty pages
	// - Insert data
	// - Mark nodes Dirty
	// - close algo
	// - Verify WritePage called for all Dirty nodes
	t.Skip("Not implemented")
}

func TestLoadNode(t *testing.T) {
	// Test loadNode caching behavior
	// - Load same Node multiple times
	// - Verify cache returns same instance
	// - Verify no duplicate reads from PageManager
	t.Skip("Not implemented")
}

// Edge Cases

func TestEmptyTree(t *testing.T) {
	t.Parallel()

	// Test operations on empty tree
	db, _ := setup(t)

	// get from empty tree (should return ErrKeyNotFound)
	_, err := db.Get([]byte("nonexistent"))
	assert.Equal(t, ErrKeyNotFound, err)

	// Try multiple different Keys on empty tree
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte(""),
		[]byte("test"),
		[]byte("\x00\xff"),
	}

	for _, key := range testKeys {
		_, err = db.Get(key)
		assert.Equal(t, ErrKeyNotFound, err)
	}
}

func TestSingleKey(t *testing.T) {
	t.Parallel()

	// Test tree with single key
	db, _ := setup(t)

	// Insert one key
	testKey := []byte("single_key")
	testValue := []byte("single_value")

	err := db.Set(testKey, testValue)
	assert.NoError(t, err)

	// get that key
	val, err := db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(testValue), string(val))

	// get non-existent key
	_, err = db.Get([]byte("nonexistent"))
	assert.Equal(t, ErrKeyNotFound, err)

	// Update the key
	newValue := []byte("updated_value")
	err = db.Set(testKey, newValue)
	assert.NoError(t, err)

	val, err = db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(newValue), string(val))

	// Verify tree structure: root is always a branch (never leaf)
	assert.False(t, db.root.Load().IsLeaf, "Root should always be a branch node")
	assert.Equal(t, 1, len(db.root.Load().Children), "Root should have 1 child")

	// Load the child and verify it has the key
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	child, err := tx.loadNode(db.root.Load().Children[0])
	assert.NoError(t, err)
	assert.True(t, child.IsLeaf, "Child should be a leaf")
	assert.Equal(t, uint16(1), child.NumKeys, "Child should have exactly 1 key")
}

func TestDuplicateKeys(t *testing.T) {
	t.Parallel()

	// Test handling of duplicate key insertions
	db, _ := setup(t)

	key := []byte("duplicate_test")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Insert key with value1
	err := db.Set(key, value1)
	assert.NoError(t, err)

	// Verify value1 is stored
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value1), string(val))

	// Insert same key with value2
	err = db.Set(key, value2)
	assert.NoError(t, err)

	// Verify only one entry exists with value2
	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value2), string(val))

	// Add more Keys to ensure tree structure
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("value%d", i))
		db.Set(k, v)
	}

	// Update original key again with value3
	err = db.Set(key, value3)
	assert.NoError(t, err)

	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value3), string(val))
}

func TestBinaryKeys(t *testing.T) {
	t.Parallel()

	// Test with non-string binary Keys
	db, _ := setup(t)

	// Test data with various binary patterns
	testData := []struct {
		key   []byte
		value []byte
		desc  string
	}{
		{[]byte("\x00\x00\x00"), []byte("null_bytes"), "null bytes"},
		{[]byte("\xff\xff\xff"), []byte("high_bytes"), "high-bit bytes"},
		{[]byte("\x00\x01\x02\x03"), []byte("ascending"), "ascending bytes"},
		{[]byte("\xff\xfe\xfd\xfc"), []byte("descending"), "descending bytes"},
		{[]byte("normal\x00key"), []byte("embedded_null"), "embedded null"},
		{[]byte("\x80\x90\xa0\xb0"), []byte("high_bits"), "high bits set"},
		{[]byte{0, 1, 255, 254, 127, 128}, []byte("mixed"), "mixed binary"},
	}

	// Insert all binary Keys
	for _, td := range testData {
		err := db.Set(td.key, td.value)
		assert.NoError(t, err)
	}

	// Verify correct retrieval
	for _, td := range testData {
		val, err := db.Get(td.key)
		if assert.NoError(t, err) {
			assert.Equal(t, string(td.value), string(val))
		}
	}

	// Test ordering with binary Keys
	key1 := []byte{0x00, 0x01}
	key2 := []byte{0x00, 0x02}
	val1 := []byte("first")
	val2 := []byte("second")

	db.Set(key1, val1)
	db.Set(key2, val2)

	// Both should be retrievable
	v, err := db.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, string(val1), string(v))

	v, err = db.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, string(val2), string(v))
}

func TestZeroLengthKeys(t *testing.T) {
	t.Parallel()

	// Test with zero-length Keys
	db, _ := setup(t)

	// Insert empty key
	emptyKey := []byte{}
	emptyValue := []byte("empty_key_value")

	err := db.Set(emptyKey, emptyValue)
	assert.NoError(t, err)

	// Retrieve empty key
	val, err := db.Get(emptyKey)
	assert.NoError(t, err)
	assert.Equal(t, string(emptyValue), string(val))

	// Mix with non-empty Keys
	normalKeys := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("a"), []byte("value_a")},
		{[]byte("b"), []byte("value_b")},
		{[]byte(""), []byte("another_empty")}, // Update empty key
		{[]byte("c"), []byte("value_c")},
	}

	for _, kv := range normalKeys {
		err := db.Set(kv.key, kv.value)
		assert.NoError(t, err)
	}

	// Verify empty key was updated
	val, err = db.Get(emptyKey)
	assert.NoError(t, err)
	assert.Equal(t, "another_empty", string(val))

	// Verify all Keys are retrievable
	for _, kv := range normalKeys {
		val, err := db.Get(kv.key)
		if assert.NoError(t, err) {
			assert.Equal(t, string(kv.value), string(val))
		}
	}
}

func TestZeroLengthValues(t *testing.T) {
	t.Parallel()

	// Test with zero-length Values
	db, _ := setup(t)

	// Insert key with empty value
	key := []byte("key_with_empty_value")
	emptyValue := []byte{}

	err := db.Set(key, emptyValue)
	assert.NoError(t, err)

	// Retrieve and verify empty value
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Empty(t, val)

	// Mix empty and non-empty Values
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("normal1"), []byte("value1")},
		{[]byte("empty1"), []byte{}},
		{[]byte("normal2"), []byte("value2")},
		{[]byte("empty2"), []byte{}},
		{[]byte("normal3"), []byte("value3")},
	}

	for _, td := range testData {
		err := db.Set(td.key, td.value)
		assert.NoError(t, err)
	}

	// Verify all Values are correctly stored
	for _, td := range testData {
		val, err := db.Get(td.key)
		if assert.NoError(t, err) {
			assert.Equal(t, string(td.value), string(val))
		}
	}

	// Update a key from non-empty to empty value
	updateKey := []byte("update_test")
	db.Set(updateKey, []byte("initial_value"))

	val, err = db.Get(updateKey)
	assert.NoError(t, err)
	assert.Equal(t, "initial_value", string(val))

	// Update to empty value
	db.Set(updateKey, []byte{})

	val, err = db.Get(updateKey)
	assert.NoError(t, err)
	assert.Empty(t, val)
}

func TestPageOverflowLargeKey(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Key that exceeds MaxKeySize = 1024
	largeKey := make([]byte, 2000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	value := []byte("small_value")

	err := db.Set(largeKey, value)
	assert.Equal(t, ErrKeyTooLarge, err)
}

func TestPageOverflowLargeValue(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	key := []byte("small_key")
	// Value that's too large to fit in a Page
	// Max size = PageSize - PageHeaderSize - LeafElementSize = 4096 - 32 - 12 = 4052
	largeValue := make([]byte, 4060)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err := db.Set(key, largeValue)
	assert.Equal(t, ErrPageOverflow, err)
}

func TestPageOverflowCombinedSize(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Key + value that combined exceed PageSize
	// Key within MaxKeySize = 1024, but combined size > Page limit
	key := make([]byte, 1000)
	value := make([]byte, 3100) // Total 4100 > 4048
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 50) % 256)
	}

	err := db.Set(key, value)
	assert.Equal(t, ErrPageOverflow, err)
}

func TestPageOverflowBoundary(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test key+value that exactly fits (should succeed)
	// PageSize = 4096
	// PageHeaderSize = 32
	// LeafElementSize = 16 (2 * uint64)
	// Available = 4096 - 32 - 16 = 4048 bytes for key+value

	keySize := 1000
	valueSize := 3000 // Total 4000, should fit

	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	for i := range key {
		key[i] = 'k'
	}
	for i := range value {
		value[i] = 'v'
	}

	err := db.Set(key, value)
	assert.NoError(t, err)

	// Verify we can retrieve it
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, valueSize, len(retrieved))
}

func TestPageOverflowMaxKeyValue(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test with key at MaxKeySize = 1024 and large value
	// Should fit: PageSize - PageHeaderSize - LeafElementSize = 4048
	key := make([]byte, 1024)   // At MaxKeySize limit
	value := make([]byte, 3000) // Total = 4024, should fit in 4048

	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 128) % 256)
	}

	err := db.Set(key, value)
	assert.NoError(t, err)

	// Verify retrieval
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestBoundaryExactly64KeysNoUnderflow(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert enough Keys to create multi-level tree
	numKeys := base.MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	// Delete until we have a Node with exactly MinKeysPerNode=64 Keys
	// This should NOT trigger underflow
	deleteCount := numKeys - base.MinKeysPerNode
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)
	}

	// Verify remaining Keys exist
	for i := deleteCount; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			expectedValue := fmt.Sprintf("value%06d", i)
			assert.Equal(t, expectedValue, string(val))
		}
	}

	t.Logf("Tree root: IsLeaf=%v, NumKeys=%d", db.root.Load().IsLeaf, db.root.Load().NumKeys)
}

func TestBoundaryDelete63rdKeyTriggersUnderflow(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := base.MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	deleteCount := numKeys - base.MinKeysPerNode - 1
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)
	}

	key := fmt.Sprintf("key%06d", deleteCount)
	err := db.Delete([]byte(key))
	require.NoError(t, err)

	remainingKeys := numKeys - deleteCount - 1
	t.Logf("Remaining Keys after underflow: %d", remainingKeys)

	for i := deleteCount + 1; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		assert.NoError(t, err)
	}
}

func TestBoundaryInsert255ThenSplit(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert MaxKeysPerNode Keys (0 to 63 = 64 Keys)
	for i := 0; i < base.MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	// Root is always a branch (constraint), check child is full
	assert.False(t, db.root.Load().IsLeaf, "Root should always be a branch node")
	assert.Equal(t, 1, len(db.root.Load().Children), "Root should have 1 child before split")

	// Load child and verify it's full
	tx, err := db.Begin(false)
	require.NoError(t, err)
	child, err := tx.loadNode(db.root.Load().Children[0])
	require.NoError(t, err)
	tx.Rollback()

	assert.True(t, child.IsLeaf, "Child should be a leaf")
	assert.Equal(t, base.MaxKeysPerNode, int(child.NumKeys), "Child should be full with MaxKeysPerNode keys")

	// Insert one more key (65th key) to trigger split
	splitKey := fmt.Sprintf("key%06d", base.MaxKeysPerNode)
	splitValue := fmt.Sprintf("value%06d", base.MaxKeysPerNode)
	err = db.Set([]byte(splitKey), []byte(splitValue))
	require.NoError(t, err)

	// After inserting MaxKeysPerNode+1 Keys, root should be branch
	assert.False(t, db.root.Load().IsLeaf, "Root should be branch after split")
	assert.GreaterOrEqual(t, len(db.root.Load().Children), 2, "Root should have at least 2 Children after split")

	// Verify all Keys retrievable
	for i := 0; i <= base.MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		assert.NoError(t, err)
	}
}

func TestBoundaryRootWithOneKeyDeleteIt(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := base.MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	require.False(t, db.root.Load().IsLeaf, "Root should be branch Node")

	initialRootKeys := db.root.Load().NumKeys
	t.Logf("Initial root Keys: %d", initialRootKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)

		if i%(numKeys/4) == 0 {
			t.Logf("After %d deletions: root.IsLeaf=%v, root.NumKeys=%d",
				i+1, db.root.Load().IsLeaf, db.root.Load().NumKeys)
		}
	}

	assert.False(t, db.root.Load().IsLeaf, "Final root should be branch")
	assert.Equal(t, uint16(0), db.root.Load().NumKeys, "Final root should have 0 Keys")
}

func TestBoundarySiblingBorrowVsMerge(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := base.MaxKeysPerNode * 3
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	require.False(t, db.root.Load().IsLeaf, "Root should not be leaf for this test")

	t.Logf("Tree structure: root.NumKeys=%d, root.Children=%d",
		db.root.Load().NumKeys, len(db.root.Load().Children))

	deleteCount := numKeys / 2
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)
	}

	for i := deleteCount; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		assert.NoError(t, err)
	}

	t.Logf("After deletions: root.IsLeaf=%v, root.NumKeys=%d",
		db.root.Load().IsLeaf, db.root.Load().NumKeys)
}

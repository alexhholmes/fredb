package pkg

import (
	"bytes"
	"fmt"
	"testing"
)

// Basic Operations Tests

func TestBTreeBasicOps(t *testing.T) {
	t.Parallel()

	// Test basic Get/Set operations
	db := setupTestDB(t)

	// Insert key-value pair
	err := db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Errorf("Failed to set key1: %v", err)
	}

	// Get existing key
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}

	// Update existing key
	err = db.Set([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Errorf("Failed to update key1: %v", err)
	}

	val, err = db.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get updated key1: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2 after update, got %s", string(val))
	}

	// Get non-existent key (should return ErrKeyNotFound)
	_, err = db.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestBTreeUpdate(t *testing.T) {
	t.Parallel()

	// Test that Set updates existing keys rather than duplicating
	db := setupTestDB(t)

	// Insert key with value1
	err := db.Set([]byte("testkey"), []byte("value1"))
	if err != nil {
		t.Errorf("Failed to set testkey: %v", err)
	}

	// Update same key with value2
	err = db.Set([]byte("testkey"), []byte("value2"))
	if err != nil {
		t.Errorf("Failed to update testkey: %v", err)
	}

	// Verify Get returns value2
	val, err := db.Get([]byte("testkey"))
	if err != nil {
		t.Errorf("Failed to get testkey: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2, got %s", string(val))
	}

	// Insert more keys to ensure no duplication
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		db.Set(key, val)
	}

	// Update the original key again
	err = db.Set([]byte("testkey"), []byte("value3"))
	if err != nil {
		t.Errorf("Failed to update testkey again: %v", err)
	}

	val, err = db.Get([]byte("testkey"))
	if err != nil {
		t.Errorf("Failed to get testkey after second update: %v", err)
	}
	if string(val) != "value3" {
		t.Errorf("Expected value3, got %s", string(val))
	}
}

// Node Splitting Tests

func TestBTreeSplitting(t *testing.T) {
	t.Parallel()

	// Test node splitting when exceeding MaxKeysPerNode
	db := setupTestDB(t)

	// Insert MaxKeysPerNode + 1 keys to force a split
	keys := make(map[string]string)
	for i := 0; i <= MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify root splits (root should no longer be a leaf)
	if db.store.root.isLeaf {
		t.Errorf("Root should not be a leaf after splitting")
	}

	// Check tree height increases (root should have children)
	if len(db.store.root.children) == 0 {
		t.Errorf("Root should have children after splitting")
	}

	// Verify all keys still retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s after split: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}
}

func TestBTreeMultipleSplits(t *testing.T) {
	t.Parallel()

	// Test multiple levels of splitting
	db := setupTestDB(t)

	// Insert enough keys to cause multiple splits (3x MaxKeysPerNode should cause multiple levels)
	numKeys := MaxKeysPerNode * 3
	keys := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify tree structure remains valid (root is not a leaf for large tree)
	if db.store.root.isLeaf {
		t.Errorf("Root should not be a leaf after multiple splits")
	}

	// Verify root has multiple children
	if len(db.store.root.children) < 2 {
		t.Errorf("Root should have multiple children after multiple splits, got %d", len(db.store.root.children))
	}

	// All keys retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s after multiple splits: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Verify we can still insert after multiple splits
	testKey := []byte("test_after_splits")
	testValue := []byte("test_value")
	err := db.Set(testKey, testValue)
	if err != nil {
		t.Errorf("Failed to insert after multiple splits: %v", err)
	}

	val, err := db.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get test key after multiple splits: %v", err)
	}
	if string(val) != string(testValue) {
		t.Errorf("Expected %s, got %s", string(testValue), string(val))
	}
}

// Sequential vs Random Insert Tests

func TestSequentialInsert(t *testing.T) {
	t.Parallel()

	// Test inserting keys in sequential order
	db := setupTestDB(t)

	// Insert 1000 sequential keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Check tree structure (likely right-heavy due to sequential insert)
	if db.store.root.isLeaf {
		t.Logf("Tree has single leaf root after %d sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d keys and %d children after %d sequential inserts",
			db.store.root.numKeys, len(db.store.root.children), numKeys)
	}
}

func TestRandomInsert(t *testing.T) {
	t.Parallel()

	// Test inserting keys in random order
	db := setupTestDB(t)

	// Generate random keys (using deterministic seed for reproducibility)
	numKeys := 1000
	keys := make(map[string]string)

	// Create keys in random order using a simple shuffle
	indices := make([]int, numKeys)
	for i := 0; i < numKeys; i++ {
		indices[i] = i
	}

	// Simple deterministic shuffle
	for i := len(indices) - 1; i > 0; i-- {
		j := (i * 7) % (i + 1) // Deterministic "random"
		indices[i], indices[j] = indices[j], indices[i]
	}

	// Insert keys in shuffled order
	for _, idx := range indices {
		key := fmt.Sprintf("key%08d", idx)
		value := fmt.Sprintf("value%08d", idx)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify all retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Check tree balance (random insertion typically produces more balanced trees)
	if !db.store.root.isLeaf {
		t.Logf("Tree has internal root with %d keys and %d children after %d random inserts",
			db.store.root.numKeys, len(db.store.root.children), numKeys)

		// Check if root has reasonable number of children (indicating some balance)
		if len(db.store.root.children) > 1 && len(db.store.root.children) < 10 {
			t.Logf("Tree appears relatively balanced with %d root children", len(db.store.root.children))
		}
	}
}

func TestReverseSequentialInsert(t *testing.T) {
	t.Parallel()

	// Test inserting keys in reverse order
	db := setupTestDB(t)

	// Insert 1000 keys in descending order
	numKeys := 1000
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Check tree structure (likely left-heavy due to reverse sequential insert)
	if db.store.root.isLeaf {
		t.Logf("Tree has single leaf root after %d reverse sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d keys and %d children after %d reverse sequential inserts",
			db.store.root.numKeys, len(db.store.root.children), numKeys)

		// With reverse insertion, we expect most activity on the left side of the tree
		if len(db.store.root.children) > 0 {
			t.Logf("First child likely contains lower keys due to reverse insertion pattern")
		}
	}
}

// Delete Tests

func TestBTreeDelete(t *testing.T) {
	t.Parallel()

	// Test basic delete operations
	db := setupTestDB(t)

	// Insert some keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, k := range keys {
		err := db.Set([]byte(k), []byte("value-"+k))
		if err != nil {
			t.Errorf("Failed to set %s: %v", k, err)
		}
	}

	// Delete middle key
	err := db.Delete([]byte("key3"))
	if err != nil {
		t.Errorf("Failed to delete key3: %v", err)
	}

	// Verify key3 is gone
	_, err = db.Get([]byte("key3"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key3, got %v", err)
	}

	// Verify other keys still exist
	for _, k := range []string{"key1", "key2", "key4", "key5"} {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("Failed to get %s after delete: %v", k, err)
		}
		if string(val) != "value-"+k {
			t.Errorf("Wrong value for %s: got %s", k, string(val))
		}
	}

	// Delete first key
	err = db.Delete([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to delete key1: %v", err)
	}

	// Delete last key
	err = db.Delete([]byte("key5"))
	if err != nil {
		t.Errorf("Failed to delete key5: %v", err)
	}

	// Delete non-existent key
	err = db.Delete([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}

	// Verify remaining keys
	for _, k := range []string{"key2", "key4"} {
		val, err := db.Get([]byte(k))
		if err != nil {
			t.Errorf("Failed to get %s: %v", k, err)
		}
		if string(val) != "value-"+k {
			t.Errorf("Wrong value for %s: got %s", k, string(val))
		}
	}
}

func TestBTreeDeleteAll(t *testing.T) {
	t.Parallel()

	// Test deleting all keys from tree
	db := setupTestDB(t)

	// Insert and then delete keys one by one
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Delete all keys in order
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}

		// Verify deleted
		_, err = db.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted, got %v", key, err)
		}

		// Verify remaining keys still exist
		for j := i + 1; j < numKeys && j < i+5; j++ {
			checkKey := fmt.Sprintf("key%04d", j)
			val, err := db.Get([]byte(checkKey))
			if err != nil {
				t.Errorf("Key %s should still exist: %v", checkKey, err)
			}
			expectedVal := fmt.Sprintf("value%04d", j)
			if string(val) != expectedVal {
				t.Errorf("Wrong value for %s: got %s, expected %s", checkKey, string(val), expectedVal)
			}
		}
	}

	// Tree should be empty now
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		_, err := db.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should not exist in empty tree", key)
		}
	}
}

func TestBTreeSequentialDelete(t *testing.T) {
	t.Parallel()

	// Test sequential deletion pattern with tree structure checks
	db := setupTestDB(t)

	// Insert enough keys to create a multi-level tree
	numKeys := MaxKeysPerNode * 2 // Enough to cause splits
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Check initial tree structure
	initialIsLeaf := db.store.root.isLeaf
	initialRootKeys := int(db.store.root.numKeys)
	t.Logf("Initial tree: root isLeaf=%v, numKeys=%d", initialIsLeaf, initialRootKeys)

	// Delete keys sequentially and monitor tree structure
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}

		// Log tree structure changes at key points
		if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
			t.Logf("After %d deletions: root isLeaf=%v, numKeys=%d",
				i+1, db.store.root.isLeaf, db.store.root.numKeys)
		}

		// Verify key is deleted
		_, err = db.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted", key)
		}
	}

	// Final tree should be empty
	if db.store.root.numKeys != 0 {
		t.Errorf("Tree should be empty, but root has %d keys", db.store.root.numKeys)
	}
	if !db.store.root.isLeaf {
		t.Errorf("Empty tree root should be leaf")
	}
}

func TestBTreeRandomDelete(t *testing.T) {
	t.Parallel()

	if !*slow {
		t.Skip("Skipping slow test; use -slow to enable")
	}

	for i := 0; i < 10; i++ {
		// Test random deletion pattern with tree structure checks
		db := setupTestDB(t)

		// Insert keys
		numKeys := MaxKeysPerNode * 2
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%06d", i)
			value := fmt.Sprintf("value%06d", i)
			keys[i] = key
			err := db.Set([]byte(key), []byte(value))
			if err != nil {
				t.Errorf("Failed to set %s: %v", key, err)
			}
		}

		// Check initial tree structure
		initialIsLeaf := db.store.root.isLeaf
		initialRootKeys := int(db.store.root.numKeys)
		t.Logf("Initial tree: root isLeaf=%v, numKeys=%d", initialIsLeaf, initialRootKeys)

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

		// Track which keys are deleted
		deleted := make(map[string]bool)

		// Delete keys randomly and monitor tree structure
		for i, idx := range deleteOrder {
			key := keys[idx]
			err := db.Delete([]byte(key))
			if err != nil {
				t.Errorf("Failed to delete %s: %v", key, err)
			}
			deleted[key] = true

			// Log tree structure changes at key points
			if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
				t.Logf("After %d random deletions: root isLeaf=%v, numKeys=%d",
					i+1, db.store.root.isLeaf, db.store.root.numKeys)
			}

			// Verify deleted key is gone
			_, err = db.Get([]byte(key))
			if err != ErrKeyNotFound {
				t.Errorf("Key %s should be deleted", key)
			}

			// Verify some non-deleted keys still exist (spot check)
			checked := 0
			for _, k := range keys {
				if !deleted[k] && checked < 5 {
					val, err := db.Get([]byte(k))
					if err != nil {
						t.Errorf("Key %s should still exist: %v", k, err)
					}
					expectedVal := "value" + k[3:] // Concatenate value prefix with key suffix
					if string(val) != expectedVal {
						t.Errorf("Wrong value for %s: got %s", k, string(val))
					}
					checked++
				}
			}
		}

		// Final tree should be empty
		if db.store.root.numKeys != 0 {
			t.Errorf("Tree should be empty, but root has %d keys", db.store.root.numKeys)
		}
		if !db.store.root.isLeaf {
			t.Errorf("Empty tree root should be leaf")
		}

		// Close database after each iteration
		db.Close()
	}
}

func TestBTreeReverseDelete(t *testing.T) {
	t.Parallel()

	// Test reverse sequential deletion pattern
	db := setupTestDB(t)

	// Insert keys
	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Check initial tree structure
	t.Logf("Initial tree: root isLeaf=%v, numKeys=%d", db.store.root.isLeaf, db.store.root.numKeys)

	// Delete keys in reverse order
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}

		// Log tree structure at key points
		deletedCount := numKeys - i
		if deletedCount == numKeys/4 || deletedCount == numKeys/2 || deletedCount == 3*numKeys/4 {
			t.Logf("After %d reverse deletions: root isLeaf=%v, numKeys=%d",
				deletedCount, db.store.root.isLeaf, db.store.root.numKeys)
		}

		// Verify key is deleted
		_, err = db.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted", key)
		}
	}

	// Final tree should be empty
	if db.store.root.numKeys != 0 {
		t.Errorf("Tree should be empty, but root has %d keys", db.store.root.numKeys)
	}
}

// Stress Tests

func TestBTreeStress(t *testing.T) {
	// Stress test with large number of operations
	// - Insert 10000 random key-value pairs
	// - Perform random Get operations
	// - Update random existing keys
	// - Verify data integrity throughout
	t.Skip("Not implemented")
}

func TestBTreeLargeValues(t *testing.T) {
	// Test with large value sizes
	// - Insert keys with 1KB, 10KB values
	// - Verify retrieval
	// - Check memory usage patterns
	t.Skip("Not implemented")
}

// PageManager Integration Tests

func TestPageCaching(t *testing.T) {
	// Test page cache behavior
	// - Verify cache hit/miss behavior
	// - Test cache population on reads
	// - Verify dirty page tracking
	t.Skip("Not implemented")
}

func TestCloseFlush(t *testing.T) {
	// Test Close() flushes dirty pages
	// - Insert data
	// - Mark nodes dirty
	// - Close BTree
	// - Verify WritePage called for all dirty nodes
	t.Skip("Not implemented")
}

func TestLoadNode(t *testing.T) {
	// Test loadNode caching behavior
	// - Load same node multiple times
	// - Verify cache returns same instance
	// - Verify no duplicate reads from PageManager
	t.Skip("Not implemented")
}

// Edge Cases

func TestEmptyTree(t *testing.T) {
	t.Parallel()

	// Test operations on empty tree
	db := setupTestDB(t)

	// Get from empty tree (should return ErrKeyNotFound)
	_, err := db.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound from empty tree, got %v", err)
	}

	// Try multiple different keys on empty tree
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte(""),
		[]byte("test"),
		[]byte("\x00\xff"),
	}

	for _, key := range testKeys {
		_, err = db.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound for key %v, got %v", key, err)
		}
	}
}

func TestSingleKey(t *testing.T) {
	t.Parallel()

	// Test tree with single key
	db := setupTestDB(t)

	// Insert one key
	testKey := []byte("single_key")
	testValue := []byte("single_value")

	err := db.Set(testKey, testValue)
	if err != nil {
		t.Errorf("Failed to set single key: %v", err)
	}

	// Get that key
	val, err := db.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get single key: %v", err)
	}
	if string(val) != string(testValue) {
		t.Errorf("Expected %s, got %s", string(testValue), string(val))
	}

	// Get non-existent key
	_, err = db.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}

	// Update the key
	newValue := []byte("updated_value")
	err = db.Set(testKey, newValue)
	if err != nil {
		t.Errorf("Failed to update single key: %v", err)
	}

	val, err = db.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get updated single key: %v", err)
	}
	if string(val) != string(newValue) {
		t.Errorf("Expected %s after update, got %s", string(newValue), string(val))
	}

	// Verify tree is still a leaf (single key shouldn't cause split)
	if !db.store.root.isLeaf {
		t.Errorf("Root should be a leaf with single key")
	}
	if db.store.root.numKeys != 1 {
		t.Errorf("Root should have exactly 1 key, got %d", db.store.root.numKeys)
	}
}

func TestDuplicateKeys(t *testing.T) {
	t.Parallel()

	// Test handling of duplicate key insertions
	db := setupTestDB(t)

	key := []byte("duplicate_test")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Insert key with value1
	err := db.Set(key, value1)
	if err != nil {
		t.Errorf("Failed to set key: %v", err)
	}

	// Verify value1 is stored
	val, err := db.Get(key)
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}
	if string(val) != string(value1) {
		t.Errorf("Expected %s, got %s", string(value1), string(val))
	}

	// Insert same key with value2
	err = db.Set(key, value2)
	if err != nil {
		t.Errorf("Failed to update key: %v", err)
	}

	// Verify only one entry exists with value2
	val, err = db.Get(key)
	if err != nil {
		t.Errorf("Failed to get updated key: %v", err)
	}
	if string(val) != string(value2) {
		t.Errorf("Expected %s after update, got %s", string(value2), string(val))
	}

	// Add more keys to ensure tree structure
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("value%d", i))
		db.Set(k, v)
	}

	// Update original key again with value3
	err = db.Set(key, value3)
	if err != nil {
		t.Errorf("Failed to update key again: %v", err)
	}

	val, err = db.Get(key)
	if err != nil {
		t.Errorf("Failed to get key after second update: %v", err)
	}
	if string(val) != string(value3) {
		t.Errorf("Expected %s after second update, got %s", string(value3), string(val))
	}
}

func TestBinaryKeys(t *testing.T) {
	t.Parallel()

	// Test with non-string binary keys
	db := setupTestDB(t)

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

	// Insert all binary keys
	for _, td := range testData {
		err := db.Set(td.key, td.value)
		if err != nil {
			t.Errorf("Failed to set %s: %v", td.desc, err)
		}
	}

	// Verify correct retrieval
	for _, td := range testData {
		val, err := db.Get(td.key)
		if err != nil {
			t.Errorf("Failed to get %s: %v", td.desc, err)
			continue
		}
		if string(val) != string(td.value) {
			t.Errorf("For %s, expected %s, got %s", td.desc, string(td.value), string(val))
		}
	}

	// Test ordering with binary keys
	key1 := []byte{0x00, 0x01}
	key2 := []byte{0x00, 0x02}
	val1 := []byte("first")
	val2 := []byte("second")

	db.Set(key1, val1)
	db.Set(key2, val2)

	// Both should be retrievable
	v, err := db.Get(key1)
	if err != nil || string(v) != string(val1) {
		t.Errorf("Binary ordering test failed for key1")
	}

	v, err = db.Get(key2)
	if err != nil || string(v) != string(val2) {
		t.Errorf("Binary ordering test failed for key2")
	}
}

func TestZeroLengthKeys(t *testing.T) {
	t.Parallel()

	// Test with zero-length keys
	db := setupTestDB(t)

	// Insert empty key
	emptyKey := []byte{}
	emptyValue := []byte("empty_key_value")

	err := db.Set(emptyKey, emptyValue)
	if err != nil {
		t.Errorf("Failed to set empty key: %v", err)
	}

	// Retrieve empty key
	val, err := db.Get(emptyKey)
	if err != nil {
		t.Errorf("Failed to get empty key: %v", err)
	}
	if string(val) != string(emptyValue) {
		t.Errorf("Expected %s for empty key, got %s", string(emptyValue), string(val))
	}

	// Mix with non-empty keys
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
		if err != nil {
			t.Errorf("Failed to set key %v: %v", kv.key, err)
		}
	}

	// Verify empty key was updated
	val, err = db.Get(emptyKey)
	if err != nil {
		t.Errorf("Failed to get empty key after update: %v", err)
	}
	if string(val) != "another_empty" {
		t.Errorf("Expected 'another_empty' for empty key, got %s", string(val))
	}

	// Verify all keys are retrievable
	for _, kv := range normalKeys {
		val, err := db.Get(kv.key)
		if err != nil {
			t.Errorf("Failed to get key %v: %v", kv.key, err)
			continue
		}
		if string(val) != string(kv.value) {
			t.Errorf("For key %v, expected %s, got %s", kv.key, string(kv.value), string(val))
		}
	}
}

func TestZeroLengthValues(t *testing.T) {
	t.Parallel()

	// Test with zero-length values
	db := setupTestDB(t)

	// Insert key with empty value
	key := []byte("key_with_empty_value")
	emptyValue := []byte{}

	err := db.Set(key, emptyValue)
	if err != nil {
		t.Errorf("Failed to set key with empty value: %v", err)
	}

	// Retrieve and verify empty value
	val, err := db.Get(key)
	if err != nil {
		t.Errorf("Failed to get key with empty value: %v", err)
	}
	if len(val) != 0 {
		t.Errorf("Expected empty value, got %v with length %d", val, len(val))
	}

	// Mix empty and non-empty values
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
		if err != nil {
			t.Errorf("Failed to set key %s: %v", string(td.key), err)
		}
	}

	// Verify all values are correctly stored
	for _, td := range testData {
		val, err := db.Get(td.key)
		if err != nil {
			t.Errorf("Failed to get key %s: %v", string(td.key), err)
			continue
		}
		if string(val) != string(td.value) {
			t.Errorf("For key %s, expected value length %d, got length %d",
				string(td.key), len(td.value), len(val))
		}
	}

	// Update a key from non-empty to empty value
	updateKey := []byte("update_test")
	db.Set(updateKey, []byte("initial_value"))

	val, err = db.Get(updateKey)
	if err != nil || string(val) != "initial_value" {
		t.Errorf("Failed initial value setup")
	}

	// Update to empty value
	db.Set(updateKey, []byte{})

	val, err = db.Get(updateKey)
	if err != nil {
		t.Errorf("Failed to get key after updating to empty value: %v", err)
	}
	if len(val) != 0 {
		t.Errorf("Expected empty value after update, got %v", val)
	}
}

func TestPageOverflowLargeKey(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Key that's too large to fit in a page
	// Max size = PageSize - PageHeaderSize - LeafElementSize = 4096 - 32 - 12 = 4052
	largeKey := make([]byte, 4060)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	value := []byte("small_value")

	err := db.Set(largeKey, value)
	if err != ErrPageOverflow {
		t.Errorf("Expected ErrPageOverflow for large key, got: %v", err)
	}
}

func TestPageOverflowLargeValue(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	key := []byte("small_key")
	// Value that's too large to fit in a page
	// Max size = PageSize - PageHeaderSize - LeafElementSize = 4096 - 32 - 12 = 4052
	largeValue := make([]byte, 4060)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err := db.Set(key, largeValue)
	if err != ErrPageOverflow {
		t.Errorf("Expected ErrPageOverflow for large value, got: %v", err)
	}
}

func TestPageOverflowCombinedSize(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Key + value that individually fit but combined exceed PageSize
	// Max size = PageSize - PageHeaderSize - LeafElementSize = 4096 - 32 - 12 = 4052
	key := make([]byte, 2100)
	value := make([]byte, 2100)
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 50) % 256)
	}

	err := db.Set(key, value)
	if err != ErrPageOverflow {
		t.Errorf("Expected ErrPageOverflow for combined size, got: %v", err)
	}
}

func TestPageOverflowBoundary(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

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
	if err != nil {
		t.Errorf("Should fit in page, got error: %v", err)
	}

	// Verify we can retrieve it
	retrieved, err := db.Get(key)
	if err != nil {
		t.Errorf("Failed to get key after boundary insert: %v", err)
	}
	if len(retrieved) != valueSize {
		t.Errorf("Retrieved value wrong size: expected %d, got %d", valueSize, len(retrieved))
	}
}

func TestPageOverflowMaxKeyValue(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Test with maximum reasonable key+value size
	// Should fit: PageSize - PageHeaderSize - LeafElementSize = 4048
	maxSize := 4048

	key := make([]byte, maxSize/2)
	value := make([]byte, maxSize/2-100) // Leave some room for overhead

	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 128) % 256)
	}

	err := db.Set(key, value)
	if err != nil {
		t.Errorf("Max size key+value should fit, got error: %v", err)
	}

	// Verify retrieval
	retrieved, err := db.Get(key)
	if err != nil {
		t.Errorf("Failed to get max size key: %v", err)
	}
	if !bytes.Equal(retrieved, value) {
		t.Error("Retrieved value doesn't match for max size")
	}
}

func TestBoundaryExactly64KeysNoUnderflow(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert enough keys to create multi-level tree
	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Delete until we have a node with exactly MinKeysPerNode=64 keys
	// This should NOT trigger underflow
	deleteCount := numKeys - MinKeysPerNode
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Verify remaining keys exist
	for i := deleteCount; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should still exist: %v", key, err)
		}
		expectedValue := fmt.Sprintf("value%06d", i)
		if string(val) != expectedValue {
			t.Errorf("Wrong value for %s", key)
		}
	}

	t.Logf("Tree root: isLeaf=%v, numKeys=%d", db.store.root.isLeaf, db.store.root.numKeys)
}

func TestBoundaryDelete63rdKeyTriggersUnderflow(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	deleteCount := numKeys - MinKeysPerNode - 1
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	key := fmt.Sprintf("key%06d", deleteCount)
	err := db.Delete([]byte(key))
	if err != nil {
		t.Fatalf("Failed to trigger underflow: %v", err)
	}

	remainingKeys := numKeys - deleteCount - 1
	t.Logf("Remaining keys after underflow: %d", remainingKeys)

	for i := deleteCount + 1; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should exist after underflow: %v", key, err)
		}
	}
}

func TestBoundaryInsert255ThenSplit(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert MaxKeysPerNode keys (0 to 63 = 64 keys)
	for i := 0; i < MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// After MaxKeysPerNode keys, root should be full but still a leaf
	if !db.store.root.isLeaf {
		t.Error("Root should still be leaf with MaxKeysPerNode keys")
	}
	if db.store.root.numKeys != MaxKeysPerNode {
		t.Errorf("Expected %d keys, got %d", MaxKeysPerNode, db.store.root.numKeys)
	}

	// Insert one more key (65th key) to trigger split
	splitKey := fmt.Sprintf("key%06d", MaxKeysPerNode)
	splitValue := fmt.Sprintf("value%06d", MaxKeysPerNode)
	err := db.Set([]byte(splitKey), []byte(splitValue))
	if err != nil {
		t.Fatalf("Failed to trigger split: %v", err)
	}

	// After inserting MaxKeysPerNode+1 keys, root should be branch
	if db.store.root.isLeaf {
		t.Error("Root should be branch after split")
	}
	if len(db.store.root.children) < 2 {
		t.Errorf("Root should have at least 2 children after split, got %d", len(db.store.root.children))
	}

	// Verify all keys retrievable
	for i := 0; i <= MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found after split: %v", key, err)
		}
	}
}

func TestBoundaryRootWithOneKeyDeleteIt(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	if db.store.root.isLeaf {
		t.Fatal("Root should be branch node")
	}

	initialRootKeys := db.store.root.numKeys
	t.Logf("Initial root keys: %d", initialRootKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}

		if i%(numKeys/4) == 0 {
			t.Logf("After %d deletions: root.isLeaf=%v, root.numKeys=%d",
				i+1, db.store.root.isLeaf, db.store.root.numKeys)
		}
	}

	if !db.store.root.isLeaf {
		t.Error("Final root should be leaf")
	}
	if db.store.root.numKeys != 0 {
		t.Errorf("Final root should have 0 keys, got %d", db.store.root.numKeys)
	}
}

func TestBoundarySiblingBorrowVsMerge(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	numKeys := MaxKeysPerNode * 3
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	if db.store.root.isLeaf {
		t.Fatal("Root should not be leaf for this test")
	}

	t.Logf("Tree structure: root.numKeys=%d, root.children=%d",
		db.store.root.numKeys, len(db.store.root.children))

	deleteCount := numKeys / 2
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		if err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	for i := deleteCount; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s should exist: %v", key, err)
		}
	}

	t.Logf("After deletions: root.isLeaf=%v, root.numKeys=%d",
		db.store.root.isLeaf, db.store.root.numKeys)
}

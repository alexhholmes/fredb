package src

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Basic Operations Tests

func TestBTreeBasicOps(t *testing.T) {
	// Test basic Get/Set operations
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// Insert key-value pair
	err = bt.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Errorf("Failed to set key1: %v", err)
	}

	// Get existing key
	val, err := bt.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}

	// Update existing key
	err = bt.Set([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Errorf("Failed to update key1: %v", err)
	}

	val, err = bt.Get([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to get updated key1: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2 after update, got %s", string(val))
	}

	// Get non-existent key (should return ErrKeyNotFound)
	_, err = bt.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Close the tree
	err = bt.Close()
	if err != nil {
		t.Errorf("Failed to close BTree: %v", err)
	}
}

func TestBTreeUpdate(t *testing.T) {
	// Test that Set updates existing keys rather than duplicating
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert key with value1
	err = bt.Set([]byte("testkey"), []byte("value1"))
	if err != nil {
		t.Errorf("Failed to set testkey: %v", err)
	}

	// Update same key with value2
	err = bt.Set([]byte("testkey"), []byte("value2"))
	if err != nil {
		t.Errorf("Failed to update testkey: %v", err)
	}

	// Verify Get returns value2
	val, err := bt.Get([]byte("testkey"))
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
		bt.Set(key, val)
	}

	// Update the original key again
	err = bt.Set([]byte("testkey"), []byte("value3"))
	if err != nil {
		t.Errorf("Failed to update testkey again: %v", err)
	}

	val, err = bt.Get([]byte("testkey"))
	if err != nil {
		t.Errorf("Failed to get testkey after second update: %v", err)
	}
	if string(val) != "value3" {
		t.Errorf("Expected value3, got %s", string(val))
	}
}

// Node Splitting Tests

func TestBTreeSplitting(t *testing.T) {
	// Test node splitting when exceeding MaxKeysPerNode
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert MaxKeysPerNode + 1 keys to force a split
	keys := make(map[string]string)
	for i := 0; i <= MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		keys[key] = value

		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify root splits (root should no longer be a leaf)
	if bt.root.isLeaf {
		t.Errorf("Root should not be a leaf after splitting")
	}

	// Check tree height increases (root should have children)
	if len(bt.root.children) == 0 {
		t.Errorf("Root should have children after splitting")
	}

	// Verify all keys still retrievable
	for key, expectedValue := range keys {
		val, err := bt.Get([]byte(key))
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
	// Test multiple levels of splitting
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert enough keys to cause multiple splits (3x MaxKeysPerNode should cause multiple levels)
	numKeys := MaxKeysPerNode * 3
	keys := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		keys[key] = value

		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify tree structure remains valid (root is not a leaf for large tree)
	if bt.root.isLeaf {
		t.Errorf("Root should not be a leaf after multiple splits")
	}

	// Verify root has multiple children
	if len(bt.root.children) < 2 {
		t.Errorf("Root should have multiple children after multiple splits, got %d", len(bt.root.children))
	}

	// All keys retrievable
	for key, expectedValue := range keys {
		val, err := bt.Get([]byte(key))
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
	err = bt.Set(testKey, testValue)
	if err != nil {
		t.Errorf("Failed to insert after multiple splits: %v", err)
	}

	val, err := bt.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get test key after multiple splits: %v", err)
	}
	if string(val) != string(testValue) {
		t.Errorf("Expected %s, got %s", string(testValue), string(val))
	}
}

// Sequential vs Random Insert Tests

func TestSequentialInsert(t *testing.T) {
	// Test inserting keys in sequential order
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert 1000 sequential keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Check tree structure (likely right-heavy due to sequential insert)
	if bt.root.isLeaf {
		t.Logf("Tree has single leaf root after %d sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d keys and %d children after %d sequential inserts",
			bt.root.numKeys, len(bt.root.children), numKeys)
	}
}

func TestRandomInsert(t *testing.T) {
	// Test inserting keys in random order
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

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

		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify all retrievable
	for key, expectedValue := range keys {
		val, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Check tree balance (random insertion typically produces more balanced trees)
	if !bt.root.isLeaf {
		t.Logf("Tree has internal root with %d keys and %d children after %d random inserts",
			bt.root.numKeys, len(bt.root.children), numKeys)

		// Check if root has reasonable number of children (indicating some balance)
		if len(bt.root.children) > 1 && len(bt.root.children) < 10 {
			t.Logf("Tree appears relatively balanced with %d root children", len(bt.root.children))
		}
	}
}

func TestReverseSequentialInsert(t *testing.T) {
	// Test inserting keys in reverse order
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert 1000 keys in descending order
	numKeys := 1000
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := bt.Get([]byte(key))
		if err != nil {
			t.Errorf("Failed to get %s: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("For key %s, expected %s, got %s", key, expectedValue, string(val))
		}
	}

	// Check tree structure (likely left-heavy due to reverse sequential insert)
	if bt.root.isLeaf {
		t.Logf("Tree has single leaf root after %d reverse sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d keys and %d children after %d reverse sequential inserts",
			bt.root.numKeys, len(bt.root.children), numKeys)

		// With reverse insertion, we expect most activity on the left side of the tree
		if len(bt.root.children) > 0 {
			t.Logf("First child likely contains lower keys due to reverse insertion pattern")
		}
	}
}

// Delete Tests

func TestBTreeDelete(t *testing.T) {
	// Test basic delete operations
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert some keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, k := range keys {
		err := bt.Set([]byte(k), []byte("value-"+k))
		if err != nil {
			t.Errorf("Failed to set %s: %v", k, err)
		}
	}

	// Delete middle key
	err = bt.Delete([]byte("key3"))
	if err != nil {
		t.Errorf("Failed to delete key3: %v", err)
	}

	// Verify key3 is gone
	_, err = bt.Get([]byte("key3"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key3, got %v", err)
	}

	// Verify other keys still exist
	for _, k := range []string{"key1", "key2", "key4", "key5"} {
		val, err := bt.Get([]byte(k))
		if err != nil {
			t.Errorf("Failed to get %s after delete: %v", k, err)
		}
		if string(val) != "value-"+k {
			t.Errorf("Wrong value for %s: got %s", k, string(val))
		}
	}

	// Delete first key
	err = bt.Delete([]byte("key1"))
	if err != nil {
		t.Errorf("Failed to delete key1: %v", err)
	}

	// Delete last key
	err = bt.Delete([]byte("key5"))
	if err != nil {
		t.Errorf("Failed to delete key5: %v", err)
	}

	// Delete non-existent key
	err = bt.Delete([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}

	// Verify remaining keys
	for _, k := range []string{"key2", "key4"} {
		val, err := bt.Get([]byte(k))
		if err != nil {
			t.Errorf("Failed to get %s: %v", k, err)
		}
		if string(val) != "value-"+k {
			t.Errorf("Wrong value for %s: got %s", k, string(val))
		}
	}
}

func TestBTreeDeleteAll(t *testing.T) {
	// Test deleting all keys from tree
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert and then delete keys one by one
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Delete all keys in order
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		err := bt.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}

		// Verify deleted
		_, err = bt.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted, got %v", key, err)
		}

		// Verify remaining keys still exist
		for j := i + 1; j < numKeys && j < i+5; j++ {
			checkKey := fmt.Sprintf("key%04d", j)
			val, err := bt.Get([]byte(checkKey))
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
		_, err := bt.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should not exist in empty tree", key)
		}
	}
}

func TestBTreeSequentialDelete(t *testing.T) {
	// Test sequential deletion pattern with tree structure checks
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert enough keys to create a multi-level tree
	numKeys := MaxKeysPerNode * 2 // Enough to cause splits
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Check initial tree structure
	initialIsLeaf := bt.root.isLeaf
	initialRootKeys := int(bt.root.numKeys)
	t.Logf("Initial tree: root isLeaf=%v, numKeys=%d", initialIsLeaf, initialRootKeys)

	// Delete keys sequentially and monitor tree structure
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := bt.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}

		// Log tree structure changes at key points
		if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
			t.Logf("After %d deletions: root isLeaf=%v, numKeys=%d",
				i+1, bt.root.isLeaf, bt.root.numKeys)
		}

		// Verify key is deleted
		_, err = bt.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted", key)
		}
	}

	// Final tree should be empty
	if bt.root.numKeys != 0 {
		t.Errorf("Tree should be empty, but root has %d keys", bt.root.numKeys)
	}
	if !bt.root.isLeaf {
		t.Errorf("Empty tree root should be leaf")
	}
}

func TestBTreeRandomDelete(t *testing.T) {
	// Test random deletion pattern with tree structure checks
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert keys
	numKeys := MaxKeysPerNode * 2
	keys := make([]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		keys[i] = key
		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Check initial tree structure
	initialIsLeaf := bt.root.isLeaf
	initialRootKeys := int(bt.root.numKeys)
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
		err := bt.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}
		deleted[key] = true

		// Log tree structure changes at key points
		if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
			t.Logf("After %d random deletions: root isLeaf=%v, numKeys=%d",
				i+1, bt.root.isLeaf, bt.root.numKeys)
		}

		// Verify deleted key is gone
		_, err = bt.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted", key)
		}

		// Verify some non-deleted keys still exist (spot check)
		checked := 0
		for _, k := range keys {
			if !deleted[k] && checked < 5 {
				val, err := bt.Get([]byte(k))
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
	if bt.root.numKeys != 0 {
		t.Errorf("Tree should be empty, but root has %d keys", bt.root.numKeys)
	}
	if !bt.root.isLeaf {
		t.Errorf("Empty tree root should be leaf")
	}
}

func TestBTreeReverseDelete(t *testing.T) {
	// Test reverse sequential deletion pattern
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert keys
	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			t.Errorf("Failed to set %s: %v", key, err)
		}
	}

	// Check initial tree structure
	t.Logf("Initial tree: root isLeaf=%v, numKeys=%d", bt.root.isLeaf, bt.root.numKeys)

	// Delete keys in reverse order
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%06d", i)
		err := bt.Delete([]byte(key))
		if err != nil {
			t.Errorf("Failed to delete %s: %v", key, err)
		}

		// Log tree structure at key points
		deletedCount := numKeys - i
		if deletedCount == numKeys/4 || deletedCount == numKeys/2 || deletedCount == 3*numKeys/4 {
			t.Logf("After %d reverse deletions: root isLeaf=%v, numKeys=%d",
				deletedCount, bt.root.isLeaf, bt.root.numKeys)
		}

		// Verify key is deleted
		_, err = bt.Get([]byte(key))
		if err != ErrKeyNotFound {
			t.Errorf("Key %s should be deleted", key)
		}
	}

	// Final tree should be empty
	if bt.root.numKeys != 0 {
		t.Errorf("Tree should be empty, but root has %d keys", bt.root.numKeys)
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
	// Test operations on empty tree
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Get from empty tree (should return ErrKeyNotFound)
	_, err = bt.Get([]byte("nonexistent"))
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
		_, err = bt.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound for key %v, got %v", key, err)
		}
	}

	// Close empty tree should work without error
	err = bt.Close()
	if err != nil {
		t.Errorf("Failed to close empty tree: %v", err)
	}
}

func TestSingleKey(t *testing.T) {
	// Test tree with single key
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert one key
	testKey := []byte("single_key")
	testValue := []byte("single_value")

	err = bt.Set(testKey, testValue)
	if err != nil {
		t.Errorf("Failed to set single key: %v", err)
	}

	// Get that key
	val, err := bt.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get single key: %v", err)
	}
	if string(val) != string(testValue) {
		t.Errorf("Expected %s, got %s", string(testValue), string(val))
	}

	// Get non-existent key
	_, err = bt.Get([]byte("nonexistent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}

	// Update the key
	newValue := []byte("updated_value")
	err = bt.Set(testKey, newValue)
	if err != nil {
		t.Errorf("Failed to update single key: %v", err)
	}

	val, err = bt.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get updated single key: %v", err)
	}
	if string(val) != string(newValue) {
		t.Errorf("Expected %s after update, got %s", string(newValue), string(val))
	}

	// Verify tree is still a leaf (single key shouldn't cause split)
	if !bt.root.isLeaf {
		t.Errorf("Root should be a leaf with single key")
	}
	if bt.root.numKeys != 1 {
		t.Errorf("Root should have exactly 1 key, got %d", bt.root.numKeys)
	}
}

func TestDuplicateKeys(t *testing.T) {
	// Test handling of duplicate key insertions
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	key := []byte("duplicate_test")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Insert key with value1
	err = bt.Set(key, value1)
	if err != nil {
		t.Errorf("Failed to set key: %v", err)
	}

	// Verify value1 is stored
	val, err := bt.Get(key)
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}
	if string(val) != string(value1) {
		t.Errorf("Expected %s, got %s", string(value1), string(val))
	}

	// Insert same key with value2
	err = bt.Set(key, value2)
	if err != nil {
		t.Errorf("Failed to update key: %v", err)
	}

	// Verify only one entry exists with value2
	val, err = bt.Get(key)
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
		bt.Set(k, v)
	}

	// Update original key again with value3
	err = bt.Set(key, value3)
	if err != nil {
		t.Errorf("Failed to update key again: %v", err)
	}

	val, err = bt.Get(key)
	if err != nil {
		t.Errorf("Failed to get key after second update: %v", err)
	}
	if string(val) != string(value3) {
		t.Errorf("Expected %s after second update, got %s", string(value3), string(val))
	}
}

func TestBinaryKeys(t *testing.T) {
	// Test with non-string binary keys
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

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
		err := bt.Set(td.key, td.value)
		if err != nil {
			t.Errorf("Failed to set %s: %v", td.desc, err)
		}
	}

	// Verify correct retrieval
	for _, td := range testData {
		val, err := bt.Get(td.key)
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

	bt.Set(key1, val1)
	bt.Set(key2, val2)

	// Both should be retrievable
	v, err := bt.Get(key1)
	if err != nil || string(v) != string(val1) {
		t.Errorf("Binary ordering test failed for key1")
	}

	v, err = bt.Get(key2)
	if err != nil || string(v) != string(val2) {
		t.Errorf("Binary ordering test failed for key2")
	}
}

func TestZeroLengthKeys(t *testing.T) {
	// Test with zero-length keys
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert empty key
	emptyKey := []byte{}
	emptyValue := []byte("empty_key_value")

	err = bt.Set(emptyKey, emptyValue)
	if err != nil {
		t.Errorf("Failed to set empty key: %v", err)
	}

	// Retrieve empty key
	val, err := bt.Get(emptyKey)
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
		err := bt.Set(kv.key, kv.value)
		if err != nil {
			t.Errorf("Failed to set key %v: %v", kv.key, err)
		}
	}

	// Verify empty key was updated
	val, err = bt.Get(emptyKey)
	if err != nil {
		t.Errorf("Failed to get empty key after update: %v", err)
	}
	if string(val) != "another_empty" {
		t.Errorf("Expected 'another_empty' for empty key, got %s", string(val))
	}

	// Verify all keys are retrievable
	for _, kv := range normalKeys {
		val, err := bt.Get(kv.key)
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
	// Test with zero-length values
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Insert key with empty value
	key := []byte("key_with_empty_value")
	emptyValue := []byte{}

	err = bt.Set(key, emptyValue)
	if err != nil {
		t.Errorf("Failed to set key with empty value: %v", err)
	}

	// Retrieve and verify empty value
	val, err := bt.Get(key)
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
		err := bt.Set(td.key, td.value)
		if err != nil {
			t.Errorf("Failed to set key %s: %v", string(td.key), err)
		}
	}

	// Verify all values are correctly stored
	for _, td := range testData {
		val, err := bt.Get(td.key)
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
	bt.Set(updateKey, []byte("initial_value"))

	val, err = bt.Get(updateKey)
	if err != nil || string(val) != "initial_value" {
		t.Errorf("Failed initial value setup")
	}

	// Update to empty value
	bt.Set(updateKey, []byte{})

	val, err = bt.Get(updateKey)
	if err != nil {
		t.Errorf("Failed to get key after updating to empty value: %v", err)
	}
	if len(val) != 0 {
		t.Errorf("Expected empty value after update, got %v", val)
	}
}

// Concurrent Access Tests (via DB wrapper)

func TestDBConcurrency(t *testing.T) {
	// Test concurrent access through DB interface
	// - Launch 100 goroutines
	// - Mix of Get/Set operations
	// - Verify no races or corruption
	// - Check final state consistency
	db, err := NewDB("")
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	numGoroutines := 100
	opsPerGoroutine := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Track all written values for verification
	expectedValues := make(map[string]string)
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of operations: 70% writes, 30% reads
				if j%10 < 7 {
					// Write operation
					key := fmt.Sprintf("key-%d-%d", id, j)
					value := fmt.Sprintf("value-%d-%d", id, j)

					err := db.Set([]byte(key), []byte(value))
					if err != nil {
						t.Errorf("Set failed: %v", err)
						return
					}

					// Track what we wrote
					mu.Lock()
					expectedValues[key] = value
					mu.Unlock()
				} else {
					// Read operation - read a key we know should exist
					readID := id
					readJ := (j / 10) * 7 // Read from earlier writes
					key := fmt.Sprintf("key-%d-%d", readID, readJ)

					val, err := db.Get([]byte(key))
					// It's OK if key not found (not written yet)
					if err != nil && err != ErrKeyNotFound {
						t.Errorf("Get failed with unexpected error: %v", err)
						return
					}

					// If found, verify it matches expected pattern
					if err == nil {
						expectedPattern := fmt.Sprintf("value-%d-%d", readID, readJ)
						if string(val) != expectedPattern {
							t.Errorf("Value mismatch: got %s, expected pattern %s", string(val), expectedPattern)
							return
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - all written keys should be readable
	for key, expectedValue := range expectedValues {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key should exist: %s, error: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Value mismatch for key %s: got %s, expected %s", key, string(val), expectedValue)
		}
	}
}

func TestDBConcurrentReads(t *testing.T) {
	// Test concurrent reads don't block each other
	// - Insert test data
	// - Launch multiple reader goroutines
	// - Verify all complete successfully
	db, err := NewDB("")
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Insert test data
	testData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		testData[key] = value

		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Launch concurrent readers
	numReaders := 50
	readsPerReader := 100
	var wg sync.WaitGroup
	wg.Add(numReaders)

	// Track completion times to verify they run concurrently
	completionTimes := make([]time.Duration, numReaders)
	start := time.Now()

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()
			goStart := time.Now()

			for j := 0; j < readsPerReader; j++ {
				// Read random keys from test data
				key := fmt.Sprintf("key-%d", (id*readsPerReader+j)%1000)
				expectedValue := fmt.Sprintf("value-%d", (id*readsPerReader+j)%1000)

				val, err := db.Get([]byte(key))
				if err != nil {
					t.Errorf("Get failed for key %s: %v", key, err)
					return
				}

				if string(val) != expectedValue {
					t.Errorf("Value mismatch for key %s: got %s, expected %s", key, string(val), expectedValue)
					return
				}
			}

			completionTimes[id] = time.Since(goStart)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify reads completed concurrently
	// If reads were serialized, total time would be much longer
	var totalSequentialTime time.Duration
	for _, d := range completionTimes {
		totalSequentialTime += d
	}

	// Concurrent execution should be significantly faster than sequential
	// Allow for some overhead, but should be at least 5x faster with 50 goroutines
	if elapsed > totalSequentialTime/5 {
		t.Logf("Warning: Concurrent reads may not be truly concurrent. Elapsed: %v, Total sequential: %v", elapsed, totalSequentialTime)
	}
}

func TestDBConcurrentWrites(t *testing.T) {
	// Test concurrent writes are serialized correctly
	// - Launch multiple writer goroutines
	// - Each writes unique keys
	// - Verify all keys present at end
	db, err := NewDB("")
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	numWriters := 20
	writesPerWriter := 50
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Track what each writer wrote
	writtenKeys := make(map[string]string)
	var mu sync.Mutex

	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < writesPerWriter; j++ {
				// Each writer writes unique keys
				key := fmt.Sprintf("writer-%d-key-%d", id, j)
				value := fmt.Sprintf("writer-%d-value-%d", id, j)

				err := db.Set([]byte(key), []byte(value))
				if err != nil {
					t.Errorf("Set failed for writer %d: %v", id, err)
					return
				}

				// Track what was written
				mu.Lock()
				writtenKeys[key] = value
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify all keys are present and have correct values
	if len(writtenKeys) != numWriters*writesPerWriter {
		t.Errorf("Expected %d keys, got %d", numWriters*writesPerWriter, len(writtenKeys))
	}

	for key, expectedValue := range writtenKeys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Value mismatch for key %s: got %s, expected %s", key, string(val), expectedValue)
		}
	}

	// Test concurrent updates to same keys
	updateKey := []byte("concurrent-update-key")
	numUpdaters := 100
	wg.Add(numUpdaters)

	for i := 0; i < numUpdaters; i++ {
		go func(id int) {
			defer wg.Done()
			value := fmt.Sprintf("updater-%d", id)
			err := db.Set(updateKey, []byte(value))
			if err != nil {
				t.Errorf("Update failed for updater %d: %v", id, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify the key exists (value could be from any updater)
	_, err = db.Get(updateKey)
	if err != nil {
		t.Errorf("Concurrent update key not found: %v", err)
	}
}

// Benchmark Tests

func BenchmarkBTreeGet(b *testing.B) {
	// Benchmark Get performance
	// - Pre-populate tree with 10000 keys
	// - Measure random Get operations
	b.Skip("Not implemented")
}

func BenchmarkBTreeSet(b *testing.B) {
	// Benchmark Set performance
	// - Measure insertion of b.N keys
	// - Include both new keys and updates
	b.Skip("Not implemented")
}

func BenchmarkBTreeMixed(b *testing.B) {
	// Benchmark mixed workload (80% reads, 20% writes)
	// - Pre-populate tree
	// - Run mixed operations
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		b.Fatalf("Failed to create BTree: %v", err)
	}
	defer bt.Close()

	// Pre-populate with 10000 keys
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		err := bt.Set([]byte(key), []byte(value))
		if err != nil {
			b.Fatalf("Failed to populate tree: %v", err)
		}
	}

	// Reset timer after setup
	b.ResetTimer()

	// Run mixed workload
	for i := 0; i < b.N; i++ {
		// Use deterministic pattern based on iteration
		if i%5 < 4 {
			// 80% reads - read existing keys
			keyNum := (i * 7) % numKeys // Deterministic key selection
			key := fmt.Sprintf("key%08d", keyNum)

			_, err := bt.Get([]byte(key))
			if err != nil {
				b.Errorf("Read failed for key %s: %v", key, err)
			}
		} else {
			// 20% writes - mix of updates and new keys
			if i%10 < 9 {
				// Update existing key
				keyNum := (i * 13) % numKeys
				key := fmt.Sprintf("key%08d", keyNum)
				value := fmt.Sprintf("updated%08d", i)

				err := bt.Set([]byte(key), []byte(value))
				if err != nil {
					b.Errorf("Update failed for key %s: %v", key, err)
				}
			} else {
				// Insert new key
				key := fmt.Sprintf("newkey%08d", numKeys+i)
				value := fmt.Sprintf("newvalue%08d", i)

				err := bt.Set([]byte(key), []byte(value))
				if err != nil {
					b.Errorf("Insert failed for key %s: %v", key, err)
				}
			}
		}
	}
}

func BenchmarkBTreeSequentialInsert(b *testing.B) {
	// Benchmark sequential insertion pattern
	// - Insert keys in ascending order
	b.Skip("Not implemented")
}

func BenchmarkBTreeRandomInsert(b *testing.B) {
	// Benchmark random insertion pattern
	// - Insert keys in random order
	b.Skip("Not implemented")
}

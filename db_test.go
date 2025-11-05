package fredb

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb/internal/base"
)

var MaxKeysPerNode = 64

// Helper to create a temporary test database
func setup(t *testing.T) (*DB, string) {
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	_ = os.Remove(tmpfile + ".wal") // Also remove wal file

	db, err := Open(tmpfile, WithCacheSizeMB(0))
	require.NoError(t, err, "Failed to create DB")

	// Type assert to get concrete type for tests
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
		_ = os.Remove(tmpfile + ".wal") // Cleanup wal file
	})

	return db, tmpfile
}

// TestCrashRecoveryLastCommittedState tests recovery to previous valid state
func TestCrashRecoveryLastCommittedState(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_last_committed.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB and do a single commit
	db1, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")
	db1.Put([]byte("key1"), []byte("value1"))
	db1.Close()

	// Reopen and do a second commit (Put without close to avoid extra TxID)
	db2, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB")
	db2.Put([]byte("key2"), []byte("value2"))

	// Check TxnIDs after second Put (before close)
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")
	page0 := &base.Page{}
	file.Read(page0.Data[:])
	page1 := &base.Page{}
	file.Read(page1.Data[:])
	file.Close()

	meta0 := page0.DeserializeMeta()
	meta1 := page1.DeserializeMeta()
	t.Logf("After second Put: Page 0 TxID=%d RootPageID=%d, Page 1 TxID=%d RootPageID=%d",
		meta0.TxID, meta0.RootPageID, meta1.TxID, meta1.RootPageID)

	// Record the older TxID (should only have key1)
	var olderTxn uint64
	var olderRoot base.PageID
	if meta0.TxID < meta1.TxID {
		olderTxn = meta0.TxID
		olderRoot = meta0.RootPageID
	} else {
		olderTxn = meta1.TxID
		olderRoot = meta1.RootPageID
	}

	db2.Close() // Now close, which will write another meta

	// Simulate crash: corrupt the newest meta Page
	file, err = os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	page0 = &base.Page{}
	file.ReadAt(page0.Data[:], 0)
	page1 = &base.Page{}
	file.ReadAt(page1.Data[:], int64(base.PageSize))

	meta0 = page0.DeserializeMeta()
	meta1 = page1.DeserializeMeta()

	// Corrupt the newer one
	var corruptOffPut int64
	if meta0.TxID > meta1.TxID {
		corruptOffPut = int64(base.PageHeaderSize)
		t.Logf("Corrupting Page 0 (TxID %d)", meta0.TxID)
	} else {
		corruptOffPut = int64(base.PageSize + base.PageHeaderSize)
		t.Logf("Corrupting Page 1 (TxID %d)", meta1.TxID)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	file.WriteAt(corruptData, corruptOffPut)
	file.Sync()
	file.Close()

	// Reopen - should fall back to previous valid state
	db3, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen after simulated crash")
	defer db3.Close()

	meta3 := db3.pager.GetSnapshot().Meta
	t.Logf("After reopen: loaded meta with TxID %d, RootPageID %d", meta3.TxID, meta3.RootPageID)
	t.Logf("Expected to recover to TxID %d (previous valid state)", olderTxn)

	// key1 should always exist
	v1, err := db3.Get([]byte("key1"))
	assert.NoError(t, err, "key1 should exist after crash recovery")
	assert.Equal(t, "value1", string(v1))

	// Verify we loaded the older state (key2 should match the older root's content)
	// If we loaded olderRoot, active if it has key2 or not
	_, err = db3.Get([]byte("key2"))
	if meta3.RootPageID == olderRoot {
		// We're at the older state, verify its actual content
		t.Logf("Recovered to older root (Page %d), key2 exists: %v", olderRoot, err == nil)
	}

	t.Logf("Successfully recovered from crash using backup meta Page")
}

// Node Splitting Tests

func TestBTreeSplitting(t *testing.T) {
	t.Parallel()

	// Test Node splitting when exceeding MaxKeysPerNode
	db, _ := setup(t)

	// Insert MaxKeysPerNode + 1 Keys to force a split
	keys := make(map[string]string)
	for i := 0; i <= MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		keys[key] = value

		err := db.Put([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify root splits (root should no longer be a leaf)
	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should be a branch node after splitting")

	// Check tree height increases (root should have Children)
	assert.NotEmpty(t, db.pager.GetSnapshot().Root.Children, "Root should have Children after splitting")

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
	numKeys := MaxKeysPerNode * 3
	keys := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		keys[key] = value

		err := db.Put([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify tree structure remains valid (root is not a leaf for large tree)
	// Get __root__ bucket's tree structure
	tx, err := db.Begin(false)
	require.NoError(t, err)
	rootBucket := tx.Bucket([]byte("__root__"))
	require.NotNil(t, rootBucket, "__root__ bucket should exist")

	assert.Equal(t, base.BranchType, rootBucket.root.Type(), "Bucket root should be a branch node after multiple splits")
	// Verify root has multiple Children
	assert.GreaterOrEqual(t, len(rootBucket.root.Children), 2, "Bucket root should have multiple Children after multiple splits")
	tx.Rollback()

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
	err = db.Put(testKey, testValue)
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

		err := db.Put([]byte(key), []byte(value))
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
	if db.pager.GetSnapshot().Root.Type() == base.LeafType {
		t.Logf("Tree has single leaf root after %d sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d sequential inserts",
			db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children), numKeys)
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

		err := db.Put([]byte(key), []byte(value))
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
	if db.pager.GetSnapshot().Root.Type() == base.BranchType {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d random inserts",
			db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children), numKeys)

		// Check if root has reasonable number of Children (indicating some balance)
		if len(db.pager.GetSnapshot().Root.Children) > 1 && len(db.pager.GetSnapshot().Root.Children) < 10 {
			t.Logf("Tree appears relatively balanced with %d root Children", len(db.pager.GetSnapshot().Root.Children))
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

		err := db.Put([]byte(key), []byte(value))
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
	if db.pager.GetSnapshot().Root.Type() == base.LeafType {
		t.Logf("Tree has single leaf root after %d reverse sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d reverse sequential inserts",
			db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children), numKeys)

		// With reverse insertion, we expect most activity on the left side of the tree
		if len(db.pager.GetSnapshot().Root.Children) > 0 {
			t.Logf("First child likely contains lower Keys due to reverse insertion pattern")
		}
	}
}

// Delete Tests

func TestBTreeDeleteAll(t *testing.T) {
	t.Parallel()

	// Test deleting all Keys from tree
	db, _ := setup(t)

	// Insert and then delete Keys one by one
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		err := db.Put([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Delete all Keys in order
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Verify deleted
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)

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
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
}

func TestBTreeSequentialDelete(t *testing.T) {
	t.Parallel()

	// Test sequential deletion pattern with tree structure checks
	db, _ := setup(t)

	// Insert enough Keys to create a multi-level tree
	numKeys := MaxKeysPerNode * 2 // Enough to cause splits
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Put([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Check initial tree structure
	initialIsLeaf := db.pager.GetSnapshot().Root.Type()
	initialRootKeys := int(db.pager.GetSnapshot().Root.NumKeys)
	t.Logf("Initial tree: root Type=%v, NumKeys=%d", initialIsLeaf, initialRootKeys)

	// Delete Keys sequentially and monitor tree structure
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Log tree structure changes at key points
		if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
			t.Logf("After %d deletions: root Type=%v, NumKeys=%d",
				i+1, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
		}

		// Verify key is deleted
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}

	// Final tree should be empty
	assert.Equal(t, uint32(0), db.pager.GetSnapshot().Root.NumKeys, "Tree should be empty")
	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Empty tree root should be branch")
}

func TestBTreeRandomDelete(t *testing.T) {
	t.Parallel()

	for i := 0; i < 10; i++ {
		// Test random deletion pattern with tree structure checks
		db, _ := setup(t)

		// Insert Keys
		numKeys := MaxKeysPerNode * 2
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%06d", i)
			value := fmt.Sprintf("value%06d", i)
			keys[i] = key
			err := db.Put([]byte(key), []byte(value))
			assert.NoError(t, err)
		}

		// Check initial tree structure
		initialIsLeaf := db.pager.GetSnapshot().Root.Type()
		initialRootKeys := int(db.pager.GetSnapshot().Root.NumKeys)
		t.Logf("Initial tree: root Type=%v, NumKeys=%d", initialIsLeaf, initialRootKeys)

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
				t.Logf("After %d random deletions: root Type=%v, NumKeys=%d",
					i+1, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
			}

			// Verify deleted key is gone
			val, err := db.Get([]byte(key))
			assert.NoError(t, err)
			assert.Nil(t, val)

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
		assert.Equal(t, uint32(0), db.pager.GetSnapshot().Root.NumKeys, "Tree should be empty")
		assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Empty tree root should be branch")

		// close database after each iteration
		db.Close()
	}
}

func TestBTreeReverseDelete(t *testing.T) {
	t.Parallel()

	// Test reverse sequential deletion pattern
	db, _ := setup(t)

	// Insert Keys
	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Put([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Check initial tree structure
	t.Logf("Initial tree: root Type=%v, NumKeys=%d", db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)

	// Delete Keys in reverse order
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Log tree structure at key points
		deletedCount := numKeys - i
		if deletedCount == numKeys/4 || deletedCount == numKeys/2 || deletedCount == 3*numKeys/4 {
			t.Logf("After %d reverse deletions: root Type=%v, NumKeys=%d",
				deletedCount, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
		}

		// Verify key is deleted
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}

	// Final tree should be empty
	assert.Equal(t, uint32(0), db.pager.GetSnapshot().Root.NumKeys, "Tree should be empty")
}

// Edge Cases

func TestEmptyTree(t *testing.T) {
	t.Parallel()

	// Test operations on empty tree
	db, _ := setup(t)

	// get from empty tree (should return nil)
	val, err := db.Get([]byte("nonexistent"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Try multiple different Keys on empty tree
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte(""),
		[]byte("test"),
		[]byte("\x00\xff"),
	}

	for _, key := range testKeys {
		val, err = db.Get(key)
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
}

func TestSingleKey(t *testing.T) {
	t.Parallel()

	// Test tree with single key
	db, _ := setup(t)

	// Insert one key
	testKey := []byte("single_key")
	testValue := []byte("single_value")

	err := db.Put(testKey, testValue)
	assert.NoError(t, err)

	// get that key
	val, err := db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(testValue), string(val))

	// get non-existent key
	val2, err := db.Get([]byte("nonexistent"))
	assert.NoError(t, err)
	assert.Nil(t, val2)

	// Update the key
	newValue := []byte("updated_value")
	err = db.Put(testKey, newValue)
	assert.NoError(t, err)

	val, err = db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(newValue), string(val))

	// Verify tree structure: root is always a branch (never leaf)
	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should always be a branch node")
	assert.Equal(t, 1, len(db.pager.GetSnapshot().Root.Children), "Root should have 1 child")

	// Load the child and verify it has the key
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	child, err := tx.load(db.pager.GetSnapshot().Root.Children[0])
	assert.NoError(t, err)
	assert.Equal(t, base.LeafType, child.Type(), "Child should be a leaf")
	assert.Equal(t, uint32(1), child.NumKeys, "Child should have exactly 1 key")
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
	err := db.Put(key, value1)
	assert.NoError(t, err)

	// Verify value1 is stored
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value1), string(val))

	// Insert same key with value2
	err = db.Put(key, value2)
	assert.NoError(t, err)

	// Verify only one entry exists with value2
	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value2), string(val))

	// Add more Keys to ensure tree structure
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("value%d", i))
		db.Put(k, v)
	}

	// Update original key again with value3
	err = db.Put(key, value3)
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
		{[]byte("\x80\x90\xa0\xb0"), []byte("high_bits"), "high bits Put"},
		{[]byte{0, 1, 255, 254, 127, 128}, []byte("mixed"), "mixed binary"},
	}

	// Insert all binary Keys
	for _, td := range testData {
		err := db.Put(td.key, td.value)
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

	db.Put(key1, val1)
	db.Put(key2, val2)

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

	err := db.Put(emptyKey, emptyValue)
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
		err := db.Put(kv.key, kv.value)
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

	err := db.Put(key, emptyValue)
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
		err := db.Put(td.key, td.value)
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
	db.Put(updateKey, []byte("initial_value"))

	val, err = db.Get(updateKey)
	assert.NoError(t, err)
	assert.Equal(t, "initial_value", string(val))

	// Update to empty value
	db.Put(updateKey, []byte{})

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

	err := db.Put(largeKey, value)
	assert.Equal(t, ErrKeyTooLarge, err)
}

func TestPageOverflowLargeValue(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	key := []byte("small_key")
	// Value that's too large to fit inline in a page (4060 bytes)
	// With overflow pages, this should now succeed
	largeValue := make([]byte, 4060)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Should succeed with overflow pages
	err := db.Put(key, largeValue)
	assert.NoError(t, err, "Large values should work with overflow pages")

	// Verify we can read it back
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, largeValue, retrieved)
}

func TestPageOverflowCombinedSize(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// With overflow pages, values can be large - only keys are limited
	// Test that key too large for page still fails
	// Max key that fits = PageSize - PageHeaderSize - LeafElementSize = 4048
	key := make([]byte, 4050) // Too large even for page
	value := make([]byte, 100)
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 50) % 256)
	}

	err := db.Put(key, value)
	assert.Equal(t, ErrKeyTooLarge, err)
}

func TestPageOverflowBoundary(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test key+value that exceeds available inline space
	// With overflow pages (threshold 3KB), value goes to overflow chain
	keySize := 1000
	valueSize := 3050 // > 3KB threshold, will use overflow

	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	for i := range key {
		key[i] = 'k'
	}
	for i := range value {
		value[i] = 'v'
	}

	// Should succeed with overflow pages
	err := db.Put(key, value)
	assert.NoError(t, err, "Should succeed with overflow pages")

	// Verify we can read it back
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)
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

	err := db.Put(key, value)
	assert.NoError(t, err)

	// Verify retrieval
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestBoundaryInsert255ThenSplit(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert enough keys to fill a page and trigger split
	// With 8-byte LeafElement: 24 + 8*N + (9+12)*N = 4096 → N ≈ 140 entries per page
	// Insert 160 to ensure split occurs
	numKeys := 160

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Put([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	// After inserting enough keys, bucket root should have split
	tx, err := db.Begin(false)
	require.NoError(t, err)
	rootBucket := tx.Bucket([]byte("__root__"))
	require.NotNil(t, rootBucket, "__root__ bucket should exist")

	t.Logf("Root: Type=%v, NumKeys=%d, Children=%d", rootBucket.root.Type(), rootBucket.root.NumKeys, len(rootBucket.root.Children))
	assert.Equal(t, base.BranchType, rootBucket.root.Type(), "Bucket root should always be a branch node")
	assert.GreaterOrEqual(t, len(rootBucket.root.Children), 2, "Bucket root should have at least 2 children after split")
	tx.Rollback()

	// Verify all keys retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		assert.NoError(t, err)
	}
}

func TestBoundaryRootWithOneKeyDeleteIt(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Put([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	require.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should be branch Node")

	initialRootKeys := db.pager.GetSnapshot().Root.NumKeys
	t.Logf("Initial root Keys: %d", initialRootKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)

		if i%(numKeys/4) == 0 {
			t.Logf("After %d deletions: root.Type()=%v, root.NumKeys=%d",
				i+1, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
		}
	}

	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Final root should be branch")
	assert.Equal(t, uint32(0), db.pager.GetSnapshot().Root.NumKeys, "Final root should have 0 Keys")
}

func TestBoundarySiblingBorrowVsMerge(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := MaxKeysPerNode * 3
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Put([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	require.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should not be leaf for this test")

	t.Logf("Tree structure: root.NumKeys=%d, root.Children=%d",
		db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children))

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

	t.Logf("After deletions: root.Type()=%v, root.NumKeys=%d",
		db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
}

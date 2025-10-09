package fredb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCursorSequentialScan(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys 1-100
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	// Sequential scan from beginning
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key000")))

	count := 0
	for cursor.Valid() {
		count++
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		expectedValue := []byte(fmt.Sprintf("value%d", count))

		assert.Equal(t, expectedKey, cursor.Key(), "Key mismatch at position %d", count)
		assert.Equal(t, expectedValue, cursor.Value(), "Value mismatch at position %d", count)

		if !cursor.Next() {
			break
		}
	}

	assert.Equal(t, 100, count)
}

func TestCursorReverseScan(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys 1-50
	for i := 1; i <= 50; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	// Seek to last key
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key999")))

	// Should be invalid (no key >= key999)
	assert.False(t, cursor.Valid(), "Expected invalid cursor after seeking past end")

	// Seek to actual last key
	require.NoError(t, cursor.Seek([]byte("key050")))

	require.True(t, cursor.Valid(), "Expected valid cursor on key050")

	// Reverse scan from key050 down to key001
	count := 50
	for cursor.Valid() {
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		expectedValue := []byte(fmt.Sprintf("value%d", count))

		assert.Equal(t, expectedKey, cursor.Key(), "Key mismatch at position %d", count)
		assert.Equal(t, expectedValue, cursor.Value(), "Value mismatch at position %d", count)

		count--
		if count == 0 {
			break
		}
		if !cursor.Prev() {
			break
		}
	}

	assert.Equal(t, 0, count, "Expected to scan down to key001")
}

func TestCursorRangeScan(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i*10)
	}

	// Range scan: [30, 70)
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key030")))

	var keys []string
	for cursor.Valid() {
		key := string(cursor.Key())
		if key >= "key070" {
			break
		}
		keys = append(keys, key)
		cursor.Next()
	}

	expectedKeys := []string{"key030", "key040", "key050", "key060"}
	assert.Equal(t, expectedKeys, keys)
}

func TestCursorEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Empty tree - cursor should be invalid
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("anykey")))

	assert.False(t, cursor.Valid(), "Expected invalid cursor on empty tree")

	// Next/Prev should return false
	assert.False(t, cursor.Next(), "Next() should return false on invalid cursor")
	assert.False(t, cursor.Prev(), "Prev() should return false on invalid cursor")
}

func TestCursorSeekNotFound(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys: key001, key003, key005, key007, key009
	for i := 1; i <= 9; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	// Seek to key002 (not present) - should land on key003
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key002")))

	require.True(t, cursor.Valid(), "Expected valid cursor")
	assert.Equal(t, []byte("key003"), cursor.Key())

	// Seek to key000 (before all Keys) - should land on key001
	require.NoError(t, cursor.Seek([]byte("key000")))

	require.True(t, cursor.Valid(), "Expected valid cursor")
	assert.Equal(t, []byte("key001"), cursor.Key())

	// Seek to key999 (after all Keys) - should be invalid
	require.NoError(t, cursor.Seek([]byte("key999")))

	assert.False(t, cursor.Valid(), "Expected invalid cursor")
}

func TestCursorAcrossSplits(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert enough Keys to trigger splits (200 Keys)
	// This should create multiple leaf nodes
	for i := 1; i <= 200; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	// Full scan should traverse all leaves via sibling pointers
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key00000")))

	count := 0
	prevKey := []byte{}
	for cursor.Valid() {
		count++

		// Verify Keys are in order
		if len(prevKey) > 0 {
			assert.Greater(t, bytes.Compare(cursor.Key(), prevKey), 0, "Keys out of order: %s should be > %s", cursor.Key(), prevKey)
		}
		prevKey = append([]byte{}, cursor.Key()...)

		if !cursor.Next() {
			break
		}
	}

	assert.Equal(t, 200, count, "Expected 200 Keys after splits")
}

func TestCursorAfterMerges(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert 100 Keys to create multiple nodes
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	// Delete every other key to trigger potential merges
	for i := 2; i <= 100; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		require.NoError(t, db.Delete(key), "Failed to delete key %d", i)
	}

	// Scan remaining Keys (odd numbers only)
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key00000")))

	count := 0
	expectedNum := 1
	for cursor.Valid() {
		count++
		expectedKey := []byte(fmt.Sprintf("key%05d", expectedNum))

		assert.Equal(t, expectedKey, cursor.Key(), "Key mismatch at position %d", count)

		expectedNum += 2
		if !cursor.Next() {
			break
		}
	}

	assert.Equal(t, 50, count, "Expected 50 Keys after deletions")
}

func TestCursorSeekSTART(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i*10)
	}

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(START) should position at first key
	require.NoError(t, cursor.Seek(START))

	require.True(t, cursor.Valid(), "Expected valid cursor after Seek(START)")

	assert.Equal(t, []byte("key010"), cursor.Key())

	// Scan all Keys from START
	count := 1
	for cursor.Next() {
		count++
	}

	assert.Equal(t, 10, count, "Expected 10 Keys from START")
}

func TestCursorSeekEND(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i*10)
	}

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(END) should position at last key
	require.NoError(t, cursor.Seek(END))

	require.True(t, cursor.Valid(), "Expected valid cursor after Seek(END)")

	assert.Equal(t, []byte("key100"), cursor.Key())

	// Verify this is actually the last key by trying Next()
	assert.False(t, cursor.Next(), "Next() after Seek(END) should return false")
}

func TestCursorSeekSTARTEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(START) on empty tree should be invalid
	require.NoError(t, cursor.Seek(START))

	assert.False(t, cursor.Valid(), "Expected invalid cursor on empty tree")
}

func TestCursorSeekENDEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(END) on empty tree should be invalid
	require.NoError(t, cursor.Seek(END))

	assert.False(t, cursor.Valid(), "Expected invalid cursor on empty tree")
}

func TestCursorRangeScanWithSTARTEND(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys 1-100
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Full range scan using START and END
	require.NoError(t, cursor.Seek(START))

	count := 0
	for cursor.Valid() && bytes.Compare(cursor.Key(), END) < 0 {
		count++
		cursor.Next()
	}

	assert.Equal(t, 100, count, "Expected 100 Keys in range [START, END)")
}

func TestCursorSeekFirstFunction(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys
	for i := 10; i <= 50; i += 10 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	tx, err := db.Begin(false)
	require.NoError(t, err, "Failed to begin transaction")
	defer tx.Rollback()

	cursor := tx.Cursor()

	// SeekFirst() should position at first key
	require.NoError(t, cursor.First(), "SeekFirst() failed")

	require.True(t, cursor.Valid(), "Expected valid cursor after SeekFirst()")

	assert.Equal(t, []byte("key010"), cursor.Key(), "Expected first key 'key010'")
}

func TestCursorSeekLastFunction(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert Keys
	for i := 10; i <= 50; i += 10 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert key %d", i)
	}

	tx, err := db.Begin(false)
	require.NoError(t, err, "Failed to begin transaction")
	defer tx.Rollback()

	cursor := tx.Cursor()

	// SeekLast() positions at last key
	require.NoError(t, cursor.Last(), "SeekLast() failed")

	require.True(t, cursor.Valid(), "Expected valid cursor after SeekLast()")

	assert.Equal(t, []byte("key050"), cursor.Key(), "Expected last key 'key050'")

	// Verify we can navigate backward
	assert.True(t, cursor.Prev(), "Expected Prev() to succeed from last key")

	assert.Equal(t, []byte("key040"), cursor.Key(), "Expected 'key040' after Prev()")
}

func TestCursorSeekFirstLastEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	tx, err := db.Begin(false)
	require.NoError(t, err, "Failed to begin transaction")
	defer tx.Rollback()

	cursor := tx.Cursor()

	// SeekFirst() on empty tree
	require.NoError(t, cursor.First(), "SeekFirst() failed")

	assert.False(t, cursor.Valid(), "Expected invalid cursor on empty tree after SeekFirst()")

	// SeekLast() on empty tree
	require.NoError(t, cursor.Last(), "SeekLast() failed")

	assert.False(t, cursor.Valid(), "Expected invalid cursor on empty tree after SeekLast()")
}

func TestCursorSeekENDComparison(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Create Keys near the maximum size (MaxKeySize = 1024)
	// END is 1024 bytes of 0xFF, so any valid key should compare less than END

	// Key 1: 1024 bytes of 0xFE (just below END)
	maxKey := make([]byte, MaxKeySize)
	for i := range maxKey {
		maxKey[i] = 0xFE
	}

	// Key 2: 1023 bytes of 0xFF + one 0xFE (also below END)
	nearMaxKey := make([]byte, MaxKeySize)
	for i := range nearMaxKey {
		nearMaxKey[i] = 0xFF
	}
	nearMaxKey[MaxKeySize-1] = 0xFE

	// Insert both Keys
	require.NoError(t, db.Set(maxKey, []byte("max_value")), "Failed to insert maxKey")
	require.NoError(t, db.Set(nearMaxKey, []byte("near_max_value")), "Failed to insert nearMaxKey")

	// Verify END compares greater than both Keys
	assert.Less(t, bytes.Compare(maxKey, END), 0, "Expected maxKey < END")
	assert.Less(t, bytes.Compare(nearMaxKey, END), 0, "Expected nearMaxKey < END")

	// Verify Seek(END) positions on the last key (lexicographically)
	tx, err := db.Begin(false)
	require.NoError(t, err, "Failed to begin transaction")
	defer tx.Rollback()

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek(END), "Seek(END) failed")

	require.True(t, cursor.Valid(), "Expected valid cursor after Seek(END)")

	// The last key lexicographically should be nearMaxKey (1023 0xFF + 1 0xFE)
	// because it compares greater than maxKey (1024 0xFE)
	expectedLastKey := nearMaxKey
	assert.Equal(t, expectedLastKey, cursor.Key(), "Expected Seek(END) to position on nearMaxKey")

	// Verify this is actually the last key
	assert.False(t, cursor.Next(), "Next() after Seek(END) should return false")
}

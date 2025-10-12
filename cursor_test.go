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

	db, _ := setup(t)

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
	k, v := cursor.Seek([]byte("key000"))

	count := 0
	for k != nil {
		count++
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		expectedValue := []byte(fmt.Sprintf("value%d", count))

		assert.Equal(t, expectedKey, k, "Key mismatch at position %d", count)
		assert.Equal(t, expectedValue, v, "Value mismatch at position %d", count)

		k, v = cursor.Next()
	}

	assert.Equal(t, 100, count)
}

func TestCursorReverseScan(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, v := cursor.Seek([]byte("key999"))

	// Should be invalid (no key >= key999)
	assert.Nil(t, k, "Expected nil key after seeking past end")

	// Seek to actual last key
	k, v = cursor.Seek([]byte("key050"))
	require.NotNil(t, k, "Expected valid cursor on key050")

	// Reverse scan from key050 down to key001
	count := 50
	for k != nil {
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		expectedValue := []byte(fmt.Sprintf("value%d", count))

		assert.Equal(t, expectedKey, k, "Key mismatch at position %d", count)
		assert.Equal(t, expectedValue, v, "Value mismatch at position %d", count)

		count--
		if count == 0 {
			break
		}
		k, v = cursor.Prev()
	}

	assert.Equal(t, 0, count, "Expected to scan down to key001")
}

func TestCursorRangeScan(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek([]byte("key030"))

	var keys []string
	for k != nil {
		key := string(k)
		if key >= "key070" {
			break
		}
		keys = append(keys, key)
		k, _ = cursor.Next()
	}

	expectedKeys := []string{"key030", "key040", "key050", "key060"}
	assert.Equal(t, expectedKeys, keys)
}

func TestCursorEmptyTree(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Empty tree - cursor should be invalid
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()
	k, _ := cursor.Seek([]byte("anykey"))

	assert.Nil(t, k, "Expected nil key on empty tree")

	// Next/Prev should return nil
	k, _ = cursor.Next()
	assert.Nil(t, k, "Next() should return nil on invalid cursor")
	k, _ = cursor.Prev()
	assert.Nil(t, k, "Prev() should return nil on invalid cursor")
}

func TestCursorSeekNotFound(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek([]byte("key002"))

	require.NotNil(t, k, "Expected valid cursor")
	assert.Equal(t, []byte("key003"), k)

	// Seek to key000 (before all Keys) - should land on key001
	k, _ = cursor.Seek([]byte("key000"))

	require.NotNil(t, k, "Expected valid cursor")
	assert.Equal(t, []byte("key001"), k)

	// Seek to key999 (after all Keys) - should be invalid
	k, _ = cursor.Seek([]byte("key999"))

	assert.Nil(t, k, "Expected nil key after seeking past end")
}

func TestCursorAcrossSplits(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek([]byte("key00000"))

	count := 0
	var prevKey []byte
	for k != nil {
		count++

		// Verify Keys are in order
		if len(prevKey) > 0 {
			assert.Greater(t, bytes.Compare(k, prevKey), 0, "Keys out of order: %s should be > %s", k, prevKey)
		}
		prevKey = append([]byte{}, k...)

		k, _ = cursor.Next()
	}

	assert.Equal(t, 200, count, "Expected 200 Keys after splits")
}

func TestCursorAfterMerges(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek([]byte("key00000"))

	count := 0
	expectedNum := 1
	for k != nil {
		count++
		expectedKey := []byte(fmt.Sprintf("key%05d", expectedNum))

		assert.Equal(t, expectedKey, k, "Key mismatch at position %d", count)

		expectedNum += 2
		k, _ = cursor.Next()
	}

	assert.Equal(t, 50, count, "Expected 50 Keys after deletions")
}

func TestCursorSeekSTART(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek(START)

	require.NotNil(t, k, "Expected valid cursor after Seek(START)")

	assert.Equal(t, []byte("key010"), k)

	// Scan all Keys from START
	count := 1
	k, _ = cursor.Next()
	for k != nil {
		count++
		k, _ = cursor.Next()
	}

	assert.Equal(t, 10, count, "Expected 10 Keys from START")
}

func TestCursorSeekEND(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek(END)

	require.NotNil(t, k, "Expected valid cursor after Seek(END)")

	assert.Equal(t, []byte("key100"), k)

	// Verify this is actually the last key by trying Next()
	k, _ = cursor.Next()
	assert.Nil(t, k, "Next() after Seek(END) should return nil")
}

func TestCursorSeekSTARTEmptyTree(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(START) on empty tree should be invalid
	k, _ := cursor.Seek(START)

	assert.Nil(t, k, "Expected nil key on empty tree")
}

func TestCursorSeekENDEmptyTree(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(END) on empty tree should be invalid
	k, _ := cursor.Seek(END)

	assert.Nil(t, k, "Expected nil key on empty tree")
}

func TestCursorRangeScanWithSTARTEND(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek(START)

	count := 0
	for k != nil && bytes.Compare(k, END) < 0 {
		count++
		k, _ = cursor.Next()
	}

	assert.Equal(t, 100, count, "Expected 100 Keys in range [START, END)")
}

func TestCursorSeekFirstFunction(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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

	// First() should position at first key
	k, _ := cursor.First()

	require.NotNil(t, k, "Expected valid cursor after First()")

	assert.Equal(t, []byte("key010"), k, "Expected first key 'key010'")
}

func TestCursorSeekLastFunction(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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

	// Last() positions at last key
	k, _ := cursor.Last()

	require.NotNil(t, k, "Expected valid cursor after Last()")

	assert.Equal(t, []byte("key050"), k, "Expected last key 'key050'")

	// Verify we can navigate backward
	k, _ = cursor.Prev()
	assert.NotNil(t, k, "Expected Prev() to succeed from last key")

	assert.Equal(t, []byte("key040"), k, "Expected 'key040' after Prev()")
}

func TestCursorSeekFirstLastEmptyTree(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	tx, err := db.Begin(false)
	require.NoError(t, err, "Failed to begin transaction")
	defer tx.Rollback()

	cursor := tx.Cursor()

	// First() on empty tree
	k, _ := cursor.First()

	assert.Nil(t, k, "Expected nil key on empty tree after First()")

	// Last() on empty tree
	k, _ = cursor.Last()

	assert.Nil(t, k, "Expected nil key on empty tree after Last()")
}

func TestCursorSeekENDComparison(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

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
	k, _ := cursor.Seek(END)

	require.NotNil(t, k, "Expected valid cursor after Seek(END)")

	// The last key lexicographically should be nearMaxKey (1023 0xFF + 1 0xFE)
	// because it compares greater than maxKey (1024 0xFE)
	expectedLastKey := nearMaxKey
	assert.Equal(t, expectedLastKey, k, "Expected Seek(END) to position on nearMaxKey")

	// Verify this is actually the last key
	k, _ = cursor.Next()
	assert.Nil(t, k, "Next() after Seek(END) should return nil")
}
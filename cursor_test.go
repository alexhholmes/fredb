package fredb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCursorSequentialScan(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys 1-100
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Sequential scan from beginning
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key000")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	count := 0
	for cursor.Valid() {
		count++
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		expectedValue := []byte(fmt.Sprintf("value%d", count))

		if !bytes.Equal(cursor.Key(), expectedKey) {
			t.Errorf("Key mismatch at position %d: got %s, want %s", count, cursor.Key(), expectedKey)
		}
		if !bytes.Equal(cursor.Value(), expectedValue) {
			t.Errorf("Value mismatch at position %d: got %s, want %s", count, cursor.Value(), expectedValue)
		}

		if !cursor.Next() {
			break
		}
	}

	if count != 100 {
		t.Errorf("Expected 100 keys, got %d", count)
	}
}

func TestCursorReverseScan(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys 1-50
	for i := 1; i <= 50; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Seek to last key
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key999")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	// Should be invalid (no key >= key999)
	if cursor.Valid() {
		t.Error("Expected invalid cursor after seeking past end")
	}

	// Seek to actual last key
	if err := cursor.Seek([]byte("key050")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor on key050")
	}

	// Reverse scan from key050 down to key001
	count := 50
	for cursor.Valid() {
		expectedKey := []byte(fmt.Sprintf("key%03d", count))
		expectedValue := []byte(fmt.Sprintf("value%d", count))

		if !bytes.Equal(cursor.Key(), expectedKey) {
			t.Errorf("Key mismatch at position %d: got %s, want %s", count, cursor.Key(), expectedKey)
		}
		if !bytes.Equal(cursor.Value(), expectedValue) {
			t.Errorf("Value mismatch at position %d: got %s, want %s", count, cursor.Value(), expectedValue)
		}

		count--
		if count == 0 {
			break
		}
		if !cursor.Prev() {
			break
		}
	}

	if count != 0 {
		t.Errorf("Expected to scan down to key001, stopped at count=%d", count)
	}
}

func TestCursorRangeScan(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*10, err)
		}
	}

	// Range scan: [30, 70)
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key030")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

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
	if len(keys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(keys))
	}

	for i, key := range keys {
		if key != expectedKeys[i] {
			t.Errorf("Key mismatch at position %d: got %s, want %s", i, key, expectedKeys[i])
		}
	}
}

func TestCursorEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Empty tree - cursor should be invalid
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("anykey")); err != nil {
		t.Fatalf("Seek on empty tree failed: %v", err)
	}

	if cursor.Valid() {
		t.Error("Expected invalid cursor on empty tree")
	}

	// Next/Prev should return false
	if cursor.Next() {
		t.Error("Next() should return false on invalid cursor")
	}

	if cursor.Prev() {
		t.Error("Prev() should return false on invalid cursor")
	}
}

func TestCursorSeekNotFound(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys: key001, key003, key005, key007, key009
	for i := 1; i <= 9; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Seek to key002 (not present) - should land on key003
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key002")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor")
	}

	if !bytes.Equal(cursor.Key(), []byte("key003")) {
		t.Errorf("Expected key003, got %s", cursor.Key())
	}

	// Seek to key000 (before all keys) - should land on key001
	if err := cursor.Seek([]byte("key000")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor")
	}

	if !bytes.Equal(cursor.Key(), []byte("key001")) {
		t.Errorf("Expected key001, got %s", cursor.Key())
	}

	// Seek to key999 (after all keys) - should be invalid
	if err := cursor.Seek([]byte("key999")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	if cursor.Valid() {
		t.Errorf("Expected invalid cursor, but got key %s", cursor.Key())
	}
}

func TestCursorAcrossSplits(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert enough keys to trigger splits (200 keys)
	// This should create multiple leaf nodes
	for i := 1; i <= 200; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Full scan should traverse all leaves via sibling pointers
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key00000")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	count := 0
	prevKey := []byte{}
	for cursor.Valid() {
		count++

		// Verify keys are in order
		if len(prevKey) > 0 && bytes.Compare(cursor.Key(), prevKey) <= 0 {
			t.Errorf("Keys out of order: %s should be > %s", cursor.Key(), prevKey)
		}
		prevKey = append([]byte{}, cursor.Key()...)

		if !cursor.Next() {
			break
		}
	}

	if count != 200 {
		t.Errorf("Expected 200 keys after splits, got %d", count)
	}
}

func TestCursorAfterMerges(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert 100 keys to create multiple nodes
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Delete every other key to trigger potential merges
	for i := 2; i <= 100; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		if err := db.Delete(key); err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Scan remaining keys (odd numbers only)
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key00000")); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}

	count := 0
	expectedNum := 1
	for cursor.Valid() {
		count++
		expectedKey := []byte(fmt.Sprintf("key%05d", expectedNum))

		if !bytes.Equal(cursor.Key(), expectedKey) {
			t.Errorf("Key mismatch at position %d: got %s, want %s", count, cursor.Key(), expectedKey)
		}

		expectedNum += 2
		if !cursor.Next() {
			break
		}
	}

	if count != 50 {
		t.Errorf("Expected 50 keys after deletions, got %d", count)
	}
}

func TestCursorSeekSTART(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*10, err)
		}
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(START) should position at first key
	if err := cursor.Seek(START); err != nil {
		t.Fatalf("Seek(START) failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor after Seek(START)")
	}

	if !bytes.Equal(cursor.Key(), []byte("key010")) {
		t.Errorf("Expected first key 'key010', got %s", cursor.Key())
	}

	// Scan all keys from START
	count := 1
	for cursor.Next() {
		count++
	}

	if count != 10 {
		t.Errorf("Expected 10 keys from START, got %d", count)
	}
}

func TestCursorSeekEND(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*10, err)
		}
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(END) should position at last key
	if err := cursor.Seek(END); err != nil {
		t.Fatalf("Seek(END) failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor after Seek(END)")
	}

	if !bytes.Equal(cursor.Key(), []byte("key100")) {
		t.Errorf("Expected last key 'key100', got %s", cursor.Key())
	}

	// Verify this is actually the last key by trying Next()
	if cursor.Next() {
		t.Errorf("Next() after Seek(END) should return false, got key %s", cursor.Key())
	}
}

func TestCursorSeekSTARTEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(START) on empty tree should be invalid
	if err := cursor.Seek(START); err != nil {
		t.Fatalf("Seek(START) failed: %v", err)
	}

	if cursor.Valid() {
		t.Error("Expected invalid cursor on empty tree")
	}
}

func TestCursorSeekENDEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Seek(END) on empty tree should be invalid
	if err := cursor.Seek(END); err != nil {
		t.Fatalf("Seek(END) failed: %v", err)
	}

	if cursor.Valid() {
		t.Error("Expected invalid cursor on empty tree")
	}
}

func TestCursorRangeScanWithSTARTEND(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys 1-100
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// Full range scan using START and END
	if err := cursor.Seek(START); err != nil {
		t.Fatalf("Seek(START) failed: %v", err)
	}

	count := 0
	for cursor.Valid() && bytes.Compare(cursor.Key(), END) < 0 {
		count++
		cursor.Next()
	}

	if count != 100 {
		t.Errorf("Expected 100 keys in range [START, END), got %d", count)
	}
}

func TestCursorSeekFirstFunction(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys
	for i := 10; i <= 50; i += 10 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// SeekFirst() should position at first key
	if err := cursor.SeekFirst(); err != nil {
		t.Fatalf("SeekFirst() failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor after SeekFirst()")
	}

	if !bytes.Equal(cursor.Key(), []byte("key010")) {
		t.Errorf("Expected first key 'key010', got %s", cursor.Key())
	}
}

func TestCursorSeekLastFunction(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Insert keys
	for i := 10; i <= 50; i += 10 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// SeekLast() positions at last key
	if err := cursor.SeekLast(); err != nil {
		t.Fatalf("SeekLast() failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor after SeekLast()")
	}

	if !bytes.Equal(cursor.Key(), []byte("key050")) {
		t.Errorf("Expected last key 'key050', got %s", cursor.Key())
	}

	// Verify we can navigate backward
	if !cursor.Prev() {
		t.Error("Expected Prev() to succeed from last key")
	}

	if !bytes.Equal(cursor.Key(), []byte("key040")) {
		t.Errorf("Expected 'key040' after Prev(), got %s", cursor.Key())
	}
}

func TestCursorSeekFirstLastEmptyTree(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()

	// SeekFirst() on empty tree
	if err := cursor.SeekFirst(); err != nil {
		t.Fatalf("SeekFirst() failed: %v", err)
	}

	if cursor.Valid() {
		t.Error("Expected invalid cursor on empty tree after SeekFirst()")
	}

	// SeekLast() on empty tree
	if err := cursor.SeekLast(); err != nil {
		t.Fatalf("SeekLast() failed: %v", err)
	}

	if cursor.Valid() {
		t.Error("Expected invalid cursor on empty tree after SeekLast()")
	}
}

func TestCursorSeekENDComparison(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)

	// Create keys near the maximum size (MaxKeySize = 1024)
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

	// Insert both keys
	if err := db.Set(maxKey, []byte("max_value")); err != nil {
		t.Fatalf("Failed to insert maxKey: %v", err)
	}
	if err := db.Set(nearMaxKey, []byte("near_max_value")); err != nil {
		t.Fatalf("Failed to insert nearMaxKey: %v", err)
	}

	// Verify END compares greater than both keys
	if bytes.Compare(maxKey, END) >= 0 {
		t.Errorf("Expected maxKey < END, but got maxKey >= END")
	}
	if bytes.Compare(nearMaxKey, END) >= 0 {
		t.Errorf("Expected nearMaxKey < END, but got nearMaxKey >= END")
	}

	// Verify Seek(END) positions on the last key (lexicographically)
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	cursor := tx.Cursor()
	if err := cursor.Seek(END); err != nil {
		t.Fatalf("Seek(END) failed: %v", err)
	}

	if !cursor.Valid() {
		t.Fatal("Expected valid cursor after Seek(END)")
	}

	// The last key lexicographically should be nearMaxKey (1023 0xFF + 1 0xFE)
	// because it compares greater than maxKey (1024 0xFE)
	expectedLastKey := nearMaxKey
	if !bytes.Equal(cursor.Key(), expectedLastKey) {
		t.Errorf("Expected Seek(END) to position on nearMaxKey, got different key")
	}

	// Verify this is actually the last key
	if cursor.Next() {
		t.Errorf("Next() after Seek(END) should return false")
	}
}

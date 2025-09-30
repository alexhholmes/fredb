package src

import (
	"bytes"
	"fmt"
	"testing"
)

// Helper functions for cursor tests

func setupTestBTree(t *testing.T) *BTree {
	pager := NewInMemoryPageManager()
	bt, err := NewBTree(pager)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}
	return bt
}

func cleanupBTree(bt *BTree) {
	bt.Close()
}

func TestCursorSequentialScan(t *testing.T) {
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Insert keys 1-100
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Sequential scan from beginning
	cursor := btree.NewCursor()
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
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Insert keys 1-50
	for i := 1; i <= 50; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Seek to last key
	cursor := btree.NewCursor()
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
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Insert keys 10, 20, 30, ..., 100
	for i := 1; i <= 10; i++ {
		key := []byte(fmt.Sprintf("key%03d", i*10))
		value := []byte(fmt.Sprintf("value%d", i*10))
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i*10, err)
		}
	}

	// Range scan: [30, 70)
	cursor := btree.NewCursor()
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
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Empty tree - cursor should be invalid
	cursor := btree.NewCursor()
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
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Insert keys: key001, key003, key005, key007, key009
	for i := 1; i <= 9; i += 2 {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Seek to key002 (not present) - should land on key003
	cursor := btree.NewCursor()
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
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Insert enough keys to trigger splits (200 keys)
	// This should create multiple leaf nodes
	for i := 1; i <= 200; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Full scan should traverse all leaves via sibling pointers
	cursor := btree.NewCursor()
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
	btree := setupTestBTree(t)
	defer cleanupBTree(btree)

	// Insert 100 keys to create multiple nodes
	for i := 1; i <= 100; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Delete every other key to trigger potential merges
	for i := 2; i <= 100; i += 2 {
		key := []byte(fmt.Sprintf("key%05d", i))
		if err := btree.Delete(key); err != nil {
			t.Fatalf("Failed to delete key %d: %v", i, err)
		}
	}

	// Scan remaining keys (odd numbers only)
	cursor := btree.NewCursor()
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
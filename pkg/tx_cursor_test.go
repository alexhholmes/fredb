package pkg

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestCursorAfterTxCommit tests that cursor becomes invalid after transaction commit
func TestCursorAfterTxCommit(t *testing.T) {
	db := setupTestDB(t)

	// Insert some test data
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := tx.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Create read transaction with cursor
	tx, err = db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}

	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key00")); err != nil {
		t.Fatal(err)
	}

	// Cursor should be valid
	if !cursor.Valid() {
		t.Error("Cursor should be valid before commit")
	}

	// Read first key
	if !bytes.Equal(cursor.Key(), []byte("key00")) {
		t.Errorf("Expected key00, got %s", cursor.Key())
	}

	// Commit the transaction
	if err := tx.Commit(); err == nil {
		t.Error("Read transaction should not support commit")
	}

	// Rollback instead
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Cursor operations should fail after rollback
	if cursor.Next() {
		t.Error("Next() should return false after transaction rollback")
	}

	if cursor.Prev() {
		t.Error("Prev() should return false after transaction rollback")
	}

	if err := cursor.Seek([]byte("key05")); err != ErrTxDone {
		t.Errorf("Seek() should return ErrTxDone after rollback, got %v", err)
	}

	if cursor.Valid() {
		t.Error("Cursor should be invalid after transaction rollback")
	}
}

// TestCursorAfterWriteTxCommit tests cursor behavior with write transaction
func TestCursorAfterWriteTxCommit(t *testing.T) {
	db := setupTestDB(t)

	// Create write transaction
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	// Insert data
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := tx.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	// Create cursor in same write transaction
	cursor := tx.Cursor()
	if err := cursor.Seek([]byte("key00")); err != nil {
		t.Fatal(err)
	}

	if !cursor.Valid() {
		t.Error("Cursor should be valid")
	}

	// Verify we see uncommitted data
	if !bytes.Equal(cursor.Key(), []byte("key00")) {
		t.Errorf("Expected key00, got %s", cursor.Key())
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Cursor should be invalid after commit
	if cursor.Next() {
		t.Error("Next() should return false after commit")
	}

	if err := cursor.Seek([]byte("key00")); err != ErrTxDone {
		t.Errorf("Seek() should return ErrTxDone after commit, got %v", err)
	}
}

// TestCursorSnapshotIsolation tests that cursors see consistent snapshots
func TestCursorSnapshotIsolation(t *testing.T) {
	db := setupTestDB(t)

	// Insert initial data
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := tx.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Start read transaction
	readTx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Rollback()

	// Create cursor on read transaction
	cursor := readTx.Cursor()
	if err := cursor.Seek([]byte("key05")); err != nil {
		t.Fatal(err)
	}

	if !cursor.Valid() {
		t.Fatal("Cursor should be valid")
	}

	// Verify initial value
	if !bytes.Equal(cursor.Value(), []byte("value5")) {
		t.Errorf("Expected value5, got %s", cursor.Value())
	}

	// Concurrent write transaction modifies key05
	writeTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	if err := writeTx.Set([]byte("key05"), []byte("modified5")); err != nil {
		t.Fatal(err)
	}

	if err := writeTx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Cursor should still see old value (snapshot isolation)
	if err := cursor.Seek([]byte("key05")); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(cursor.Value(), []byte("value5")) {
		t.Errorf("Cursor should see snapshot value 'value5', got %s", cursor.Value())
	}

	// New transaction should see new value
	newTx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer newTx.Rollback()

	val, err := newTx.Get([]byte("key05"))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(val, []byte("modified5")) {
		t.Errorf("New transaction should see 'modified5', got %s", val)
	}
}

// TestCursorConcurrentModifications tests cursor with concurrent modifications
func TestCursorConcurrentModifications(t *testing.T) {
	db := setupTestDB(t)

	// Insert initial data
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := tx.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Start multiple readers with cursors
	var wg sync.WaitGroup
	stopWriters := make(chan struct{})

	// Reader goroutines
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			tx, err := db.Begin(false)
			if err != nil {
				t.Error(err)
				return
			}
			defer tx.Rollback()

			cursor := tx.Cursor()
			if err := cursor.Seek([]byte("key0000")); err != nil {
				t.Error(err)
				return
			}

			// Scan through all keys
			count := 0
			for cursor.Valid() {
				// Verify we always see consistent snapshot
				key := cursor.Key()
				val := cursor.Value()

				expectedVal := fmt.Sprintf("value%d", count)
				if !bytes.Equal(val, []byte(expectedVal)) {
					// Could be modified value, that's ok in snapshot
					if !bytes.HasPrefix(val, []byte("modified")) {
						t.Errorf("Reader %d: unexpected value at %s: %s", readerID, key, val)
					}
				}

				count++
				if !cursor.Next() {
					break
				}
			}

			// Should see exactly 100 keys
			if count != 100 {
				t.Errorf("Reader %d: expected 100 keys, got %d", readerID, count)
			}
		}(r)
	}

	// Writer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10; i++ {
			select {
			case <-stopWriters:
				return
			default:
			}

			tx, err := db.Begin(true)
			if err != nil {
				continue // Another writer active
			}

			// Modify some keys
			for j := i * 10; j < (i+1)*10 && j < 100; j++ {
				key := []byte(fmt.Sprintf("key%04d", j))
				value := []byte(fmt.Sprintf("modified%d", j))
				tx.Set(key, value)
			}

			tx.Commit()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Let operations run
	time.Sleep(100 * time.Millisecond)
	close(stopWriters)
	wg.Wait()
}

// TestCursorWithDeletedKeys tests cursor behavior when keys are deleted
func TestCursorWithDeletedKeys(t *testing.T) {
	db := setupTestDB(t)

	// Insert initial data
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		if err := tx.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Start read transaction with cursor
	readTx, err := db.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	defer readTx.Rollback()

	cursor := readTx.Cursor()
	if err := cursor.Seek([]byte("key00")); err != nil {
		t.Fatal(err)
	}

	// Advance to middle
	for i := 0; i < 10; i++ {
		if !cursor.Next() {
			t.Fatal("Should be able to advance")
		}
	}

	// Should be at key10
	if !bytes.Equal(cursor.Key(), []byte("key10")) {
		t.Errorf("Expected key10, got %s", cursor.Key())
	}

	// Delete keys in another transaction
	writeTx, err := db.Begin(true)
	if err != nil {
		t.Fatal(err)
	}

	// Delete keys 05-14
	for i := 5; i < 15; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		if err := writeTx.Delete(key); err != nil {
			t.Fatal(err)
		}
	}

	if err := writeTx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Original cursor should still see all keys (snapshot)
	if err := cursor.Seek([]byte("key05")); err != nil {
		t.Fatal(err)
	}

	// Should successfully find key05 in snapshot
	if !cursor.Valid() {
		t.Error("Cursor should be valid on key05 in snapshot")
	}

	if !bytes.Equal(cursor.Key(), []byte("key05")) {
		t.Errorf("Expected key05 in snapshot, got %s", cursor.Key())
	}

	// Count remaining keys in snapshot
	count := 1 // Starting at key05
	for cursor.Next() {
		count++
	}

	// Should see all 15 remaining keys (05-19) in snapshot
	if count != 15 {
		t.Errorf("Expected 15 keys from key05 to key19 in snapshot, got %d", count)
	}
}

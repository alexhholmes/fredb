package fredb

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCursorAfterTxCommit tests that cursor becomes invalid after transaction commit
func TestCursorAfterTxCommit(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert some test data
	tx, err := db.Begin(true)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, tx.Set(key, value))
	}

	require.NoError(t, tx.Commit())

	// Create read transaction with cursor
	tx, err = db.Begin(false)
	require.NoError(t, err)

	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key00")))

	// Cursor should be valid
	assert.True(t, cursor.Valid(), "Cursor should be valid before commit")

	// Read first key
	assert.Equal(t, []byte("key00"), cursor.Key())

	// Commit the transaction
	assert.Error(t, tx.Commit(), "Read transaction should not support commit")

	// Rollback instead
	require.NoError(t, tx.Rollback())

	// Cursor operations should fail after rollback
	assert.False(t, cursor.Next(), "Next() should return false after transaction rollback")

	assert.False(t, cursor.Prev(), "Prev() should return false after transaction rollback")

	assert.Equal(t, ErrTxDone, cursor.Seek([]byte("key05")), "Seek() should return ErrTxDone after rollback")

	assert.False(t, cursor.Valid(), "Cursor should be invalid after transaction rollback")
}

// TestCursorAfterWriteTxCommit tests cursor behavior with write transaction
func TestCursorAfterWriteTxCommit(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Create write transaction
	tx, err := db.Begin(true)
	require.NoError(t, err)

	// Insert data
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, tx.Set(key, value))
	}

	// Create cursor in same write transaction
	cursor := tx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key00")))

	assert.True(t, cursor.Valid(), "Cursor should be valid")

	// Verify we see uncommitted data
	assert.Equal(t, []byte("key00"), cursor.Key())

	// Commit transaction
	require.NoError(t, tx.Commit())

	// Cursor should be invalid after commit
	assert.False(t, cursor.Next(), "Next() should return false after commit")

	assert.Equal(t, ErrTxDone, cursor.Seek([]byte("key00")), "Seek() should return ErrTxDone after commit")
}

// TestCursorSnapshotIsolation tests that cursors see consistent snapshots
func TestCursorSnapshotIsolation(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert initial data
	tx, err := db.Begin(true)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, tx.Set(key, value))
	}

	require.NoError(t, tx.Commit())

	// Start read transaction
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer readTx.Rollback()

	// Create cursor on read transaction
	cursor := readTx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key05")))

	require.True(t, cursor.Valid(), "Cursor should be valid")

	// Verify initial value
	assert.Equal(t, []byte("value5"), cursor.Value())

	// Concurrent write transaction modifies key05
	writeTx, err := db.Begin(true)
	require.NoError(t, err)

	require.NoError(t, writeTx.Set([]byte("key05"), []byte("modified5")))

	require.NoError(t, writeTx.Commit())

	// Cursor should still see old value (snapshot isolation)
	require.NoError(t, cursor.Seek([]byte("key05")))

	assert.Equal(t, []byte("value5"), cursor.Value(), "Cursor should see snapshot value 'value5'")

	// New transaction should see new value
	newTx, err := db.Begin(false)
	require.NoError(t, err)
	defer newTx.Rollback()

	val, err := newTx.Get([]byte("key05"))
	require.NoError(t, err)

	assert.Equal(t, []byte("modified5"), val, "New transaction should see 'modified5'")
}

// TestCursorConcurrentModifications tests cursor with concurrent modifications
func TestCursorConcurrentModifications(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert initial data
	tx, err := db.Begin(true)
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%04d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, tx.Set(key, value))
	}

	require.NoError(t, tx.Commit())

	// Start multiple readers with cursors
	var wg sync.WaitGroup
	stopWriters := make(chan struct{})

	// Reader goroutines
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			tx, err := db.Begin(false)
			if !assert.NoError(t, err) {
				return
			}
			defer tx.Rollback()

			cursor := tx.Cursor()
			if !assert.NoError(t, cursor.Seek([]byte("key0000"))) {
				return
			}

			// Scan through all Keys
			count := 0
			for cursor.Valid() {
				// Verify we always see consistent snapshot
				key := cursor.Key()
				val := cursor.Value()

				expectedVal := fmt.Sprintf("value%d", count)
				if !bytes.Equal(val, []byte(expectedVal)) {
					// Could be modified value, that's ok in snapshot
					assert.True(t, bytes.HasPrefix(val, []byte("modified")),
						"Reader %d: unexpected value at %s: %s", readerID, key, val)
				}

				count++
				if !cursor.Next() {
					break
				}
			}

			// Should see exactly 100 Keys
			assert.Equal(t, 100, count, "Reader %d: expected 100 Keys", readerID)
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

			// Modify some Keys
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

// TestCursorWithDeletedKeys tests cursor behavior when Keys are deleted
func TestCursorWithDeletedKeys(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert initial data
	tx, err := db.Begin(true)
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		require.NoError(t, tx.Set(key, value))
	}

	require.NoError(t, tx.Commit())

	// Start read transaction with cursor
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer readTx.Rollback()

	cursor := readTx.Cursor()
	require.NoError(t, cursor.Seek([]byte("key00")))

	// Advance to middle
	for i := 0; i < 10; i++ {
		require.True(t, cursor.Next(), "Should be able to advance")
	}

	// Should be at key10
	assert.Equal(t, []byte("key10"), cursor.Key())

	// Delete Keys in another transaction
	writeTx, err := db.Begin(true)
	require.NoError(t, err)

	// Delete Keys 05-14
	for i := 5; i < 15; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		require.NoError(t, writeTx.Delete(key))
	}

	require.NoError(t, writeTx.Commit())

	// Original cursor should still see all Keys (snapshot)
	require.NoError(t, cursor.Seek([]byte("key05")))

	// Should successfully find key05 in snapshot
	assert.True(t, cursor.Valid(), "Cursor should be valid on key05 in snapshot")

	assert.Equal(t, []byte("key05"), cursor.Key(), "Expected key05 in snapshot")

	// Count remaining Keys in snapshot
	count := 1 // Starting at key05
	for cursor.Next() {
		count++
	}

	// Should see all 15 remaining Keys (05-19) in snapshot
	assert.Equal(t, 15, count, "Expected 15 Keys from key05 to key19 in snapshot")
}

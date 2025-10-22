package fredb

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxBasicOperations(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test Update (write transaction)
	err := db.Update(func(tx *Tx) error {
		err := tx.Put([]byte("key1"), []byte("value1"))
		if err != nil {
			return err
		}
		err = tx.Put([]byte("key2"), []byte("value2"))
		if err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	// Test View (read transaction)
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		assert.Equal(t, "value1", string(val))

		val, err = tx.Get([]byte("key2"))
		if err != nil {
			return err
		}
		assert.Equal(t, "value2", string(val))
		return nil
	})
	require.NoError(t, err)
}

func TestTxCommitRollback(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test commit
	tx, err := db.Begin(true)
	require.NoError(t, err)
	err = tx.Put([]byte("committed"), []byte("value"))
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Verify committed data persists
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get([]byte("committed"))
		if err != nil {
			return err
		}
		assert.Equal(t, "value", string(val))
		return nil
	})
	require.NoError(t, err)

	// Test rollback
	tx, err = db.Begin(true)
	require.NoError(t, err)
	err = tx.Put([]byte("rolled-back"), []byte("should not exist"))
	require.NoError(t, err)
	err = tx.Rollback()
	require.NoError(t, err)

	// Verify rolled back data does not persist
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get([]byte("rolled-back"))
		require.NoError(t, err)
		assert.Nil(t, val)
		return nil
	})
	require.NoError(t, err)
}

func TestTxSingleWriter(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Start first write transaction
	tx1, err := db.Begin(true)
	require.NoError(t, err)
	defer tx1.Rollback()

	// Try to start second write transaction (should fail)
	_, err = db.Begin(true)
	assert.ErrorIs(t, err, ErrTxInProgress)

	// Commit first transaction
	err = tx1.Commit()
	require.NoError(t, err)

	// Now second write transaction should succeed
	tx2, err := db.Begin(true)
	require.NoError(t, err)
	tx2.Rollback()
}

func TestTxMultipleReaders(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert test data
	err := db.Update(func(tx *Tx) error {
		return tx.Put([]byte("key"), []byte("value"))
	})
	require.NoError(t, err)

	// Start multiple read transactions
	tx1, err := db.Begin(false)
	require.NoError(t, err)
	defer tx1.Rollback()

	tx2, err := db.Begin(false)
	require.NoError(t, err)
	defer tx2.Rollback()

	tx3, err := db.Begin(false)
	require.NoError(t, err)
	defer tx3.Rollback()

	// All should read successfully
	val1, err := tx1.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, "value", string(val1))

	val2, err := tx2.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, "value", string(val2))

	val3, err := tx3.Get([]byte("key"))
	require.NoError(t, err)
	assert.Equal(t, "value", string(val3))
}

func TestTxWriteOnReadOnly(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_write_readonly.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Try to write in read-only transaction
	err = db.View(func(tx *Tx) error {
		return tx.Put([]byte("key"), []byte("value"))
	})
	assert.ErrorIs(t, err, ErrTxNotWritable)

	// Try to delete in read-only transaction
	err = db.View(func(tx *Tx) error {
		return tx.Delete([]byte("key"))
	})
	assert.ErrorIs(t, err, ErrTxNotWritable)
}

func TestTxDoneCheck(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test operations after commit
	tx, err := db.Begin(true)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Operations after commit should fail
	err = tx.Put([]byte("key"), []byte("value"))
	assert.ErrorIs(t, err, ErrTxDone)

	_, err = tx.Get([]byte("key"))
	assert.ErrorIs(t, err, ErrTxDone)

	// Test operations after rollback
	tx2, err := db.Begin(false)
	require.NoError(t, err)
	err = tx2.Rollback()
	require.NoError(t, err)

	_, err = tx2.Get([]byte("key"))
	assert.ErrorIs(t, err, ErrTxDone)
}

func TestTxAutoRollback(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Update with error should auto-rollback
	err := db.Update(func(tx *Tx) error {
		err := tx.Put([]byte("key"), []byte("value"))
		if err != nil {
			return err
		}
		return fmt.Errorf("intentional error")
	})
	assert.Error(t, err)
	assert.Equal(t, "intentional error", err.Error())

	// Key should not exist (rolled back)
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get([]byte("key"))
		require.NoError(t, err)
		assert.Nil(t, val)
		return nil
	})
	require.NoError(t, err)
}

func TestTxMultipleBeginWithoutCommit(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Begin first write transaction
	tx1, err := db.Begin(true)
	require.NoError(t, err)

	// Attempt to begin second write transaction without committing first
	tx2, err := db.Begin(true)
	assert.ErrorIs(t, err, ErrTxInProgress)
	assert.Nil(t, tx2)

	// First transaction should still be valid
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err = tx1.Put(testKey, testValue)
	assert.NoError(t, err)

	// Commit first transaction
	err = tx1.Commit()
	assert.NoError(t, err)

	// Now second write transaction should succeed
	tx3, err := db.Begin(true)
	require.NoError(t, err)
	defer tx3.Rollback()

	// Verify we can access the committed data
	val, err := tx3.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(testValue), string(val))
}

func TestTxOperationsAfterClose(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert some data
	err := db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err)

	// Begin transaction before close
	tx, err := db.Begin(false)
	require.NoError(t, err)

	// Close database
	err = db.Close()
	assert.NoError(t, err)

	// Operations on transaction after DB.Close() should still work
	// because transaction captured snapshot
	val, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Logf("get after close returned error: %v (may be expected)", err)
	} else {
		assert.Equal(t, "value1", string(val))
	}

	// Rollback should not panic
	err = tx.Rollback()
	if err != nil {
		t.Logf("Rollback after close returned error: %v", err)
	}

	// New transaction after close should fail
	_, err = db.Begin(false)
	assert.Error(t, err)
}

func TestTxConcurrentWriteBegin(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Begin first write transaction
	tx1, err := db.Begin(true)
	require.NoError(t, err)
	defer tx1.Rollback()

	// Launch goroutines attempting to begin write transactions
	numAttempts := 10
	var wg sync.WaitGroup
	wg.Add(numAttempts)

	errors := make(chan error, numAttempts)

	for i := 0; i < numAttempts; i++ {
		go func(id int) {
			defer wg.Done()

			tx, err := db.Begin(true)
			if err == ErrTxInProgress {
				// Expected
				errors <- nil
				return
			}
			if err != nil {
				errors <- fmt.Errorf("goroutine %d: unexpected error: %v", id, err)
				return
			}

			// If we got a transaction, that's wrong
			tx.Rollback()
			errors <- fmt.Errorf("goroutine %d: should not have obtained write transaction", id)
		}(i)
	}

	wg.Wait()
	close(errors)

	// All should have gotten ErrTxInProgress
	errorCount := 0
	for err := range errors {
		if err != nil {
			t.Error(err)
			errorCount++
		}
	}

	assert.Equal(t, 0, errorCount, "goroutines had unexpected behavior")

	// Commit the first transaction
	err = tx1.Commit()
	assert.NoError(t, err)

	// Now a new write transaction should succeed
	tx2, err := db.Begin(true)
	assert.NoError(t, err)
	if tx2 != nil {
		tx2.Rollback()
	}
}

func TestTxWriteThenReadMultiple(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Write some data
	err := db.Set([]byte("initial"), []byte("data"))
	require.NoError(t, err)

	// Begin write transaction
	writeTx, err := db.Begin(true)
	require.NoError(t, err)

	// Write in write transaction
	err = writeTx.Put([]byte("write_key"), []byte("write_value"))
	assert.NoError(t, err)

	// Multiple read transactions should be allowed concurrently
	numReaders := 5
	var wg sync.WaitGroup
	wg.Add(numReaders)

	readerErrors := make(chan error, numReaders)

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()

			readTx, err := db.Begin(false)
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d: failed to begin: %v", id, err)
				return
			}
			defer readTx.Rollback()

			// Should see initial data
			val, err := readTx.Get([]byte("initial"))
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d: failed to get initial: %v", id, err)
				return
			}
			if string(val) != "data" {
				readerErrors <- fmt.Errorf("reader %d: wrong initial value", id)
				return
			}

			// Should NOT see uncommitted write
			val, err = readTx.Get([]byte("write_key"))
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d: failed to get write_key: %v", id, err)
				return
			}
			if val != nil {
				readerErrors <- fmt.Errorf("reader %d: should not see uncommitted write", id)
				return
			}

			readerErrors <- nil
		}(i)
	}

	wg.Wait()
	close(readerErrors)

	// Check all readers succeeded
	for err := range readerErrors {
		assert.NoError(t, err)
	}

	// Commit write transaction
	err = writeTx.Commit()
	assert.NoError(t, err)

	// New read transaction should see committed write
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer readTx.Rollback()

	val, err := readTx.Get([]byte("write_key"))
	assert.NoError(t, err)
	assert.Equal(t, "write_value", string(val))
}

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
		require.NoError(t, tx.Put(key, value))
	}

	require.NoError(t, tx.Commit())

	// Create read transaction with cursor
	tx, err = db.Begin(false)
	require.NoError(t, err)

	cursor := tx.Cursor()
	k, v := cursor.Seek([]byte("key00"))
	require.NotNil(t, k, "Seek should find key00")
	_ = v

	// Cursor should be valid
	assert.True(t, cursor.Valid(), "Cursor should be valid before commit")

	// Read first key
	assert.Equal(t, []byte("key00"), cursor.Key())

	// Commit the transaction
	assert.Error(t, tx.Commit(), "Read transaction should not support commit")

	// Rollback instead
	require.NoError(t, tx.Rollback())

	// Cursor operations should return nil after rollback
	k, v = cursor.Next()
	assert.Nil(t, k, "Next() should return nil key after transaction rollback")
	_ = v

	k, v = cursor.Prev()
	assert.Nil(t, k, "Prev() should return nil key after transaction rollback")

	k, v = cursor.Seek([]byte("key05"))
	assert.Nil(t, k, "Seek() should return nil key after rollback")

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
		require.NoError(t, tx.Put(key, value))
	}

	// Create cursor in same write transaction
	cursor := tx.Cursor()
	k, v := cursor.Seek([]byte("key00"))
	require.NotNil(t, k, "Seek should find key00")
	_ = v

	assert.True(t, cursor.Valid(), "Cursor should be valid")

	// Verify we see uncommitted data
	assert.Equal(t, []byte("key00"), cursor.Key())

	// Commit transaction
	require.NoError(t, tx.Commit())

	// Cursor should be invalid after commit
	k, v = cursor.Next()
	assert.Nil(t, k, "Next() should return nil key after commit")
	_ = v

	k, v = cursor.Seek([]byte("key00"))
	assert.Nil(t, k, "Seek() should return nil key after commit")
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
		require.NoError(t, tx.Put(key, value))
	}

	require.NoError(t, tx.Commit())

	// Start read transaction
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer readTx.Rollback()

	// Create cursor on read transaction
	cursor := readTx.Cursor()
	k, v := cursor.Seek([]byte("key05"))
	require.NotNil(t, k, "Seek should find key05")
	_ = v

	require.True(t, cursor.Valid(), "Cursor should be valid")

	// Verify initial value
	assert.Equal(t, []byte("value5"), cursor.Value())

	// Concurrent write transaction modifies key05
	writeTx, err := db.Begin(true)
	require.NoError(t, err)

	require.NoError(t, writeTx.Put([]byte("key05"), []byte("modified5")))

	require.NoError(t, writeTx.Commit())

	// Cursor should still see old value (snapshot isolation)
	k, v = cursor.Seek([]byte("key05"))
	require.NotNil(t, k, "Seek should still find key05 in snapshot")
	_ = v

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
		require.NoError(t, tx.Put(key, value))
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
			k, v := cursor.Seek([]byte("key0000"))
			if !assert.NotNil(t, k, "Seek should find key0000") {
				return
			}
			_ = v

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
				k, v = cursor.Next()
				if k == nil {
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
				tx.Put(key, value)
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
		require.NoError(t, tx.Put(key, value))
	}

	require.NoError(t, tx.Commit())

	// Start read transaction with cursor
	readTx, err := db.Begin(false)
	require.NoError(t, err)
	defer readTx.Rollback()

	cursor := readTx.Cursor()
	k, v := cursor.Seek([]byte("key00"))
	require.NotNil(t, k, "Seek should find key00")
	_ = v

	// Advance to middle
	for i := 0; i < 10; i++ {
		k, v = cursor.Next()
		require.NotNil(t, k, "Should be able to advance to position %d", i+1)
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
	k, v = cursor.Seek([]byte("key05"))
	require.NotNil(t, k, "Seek should still find key05 in snapshot")
	_ = v

	// Should successfully find key05 in snapshot
	assert.True(t, cursor.Valid(), "Cursor should be valid on key05 in snapshot")

	assert.Equal(t, []byte("key05"), cursor.Key(), "Expected key05 in snapshot")

	// Count remaining Keys in snapshot
	count := 1 // Starting at key05
	k, v = cursor.Next()
	for k != nil {
		count++
		k, v = cursor.Next()
	}

	// Should see all 15 remaining Keys (05-19) in snapshot
	assert.Equal(t, 15, count, "Expected 15 Keys from key05 to key19 in snapshot")
}

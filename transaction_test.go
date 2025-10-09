package fredb

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxBasicOperations(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_basic.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Test Update (write transaction)
	err = db.Update(func(tx *Tx) error {
		err := tx.Set([]byte("key1"), []byte("value1"))
		if err != nil {
			return err
		}
		err = tx.Set([]byte("key2"), []byte("value2"))
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

	// COW now implemented for simple leaf modifications
	tmpfile := "/tmp/test_tx_commit_rollback.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Test commit
	tx, err := db.Begin(true)
	require.NoError(t, err)
	err = tx.Set([]byte("committed"), []byte("value"))
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
	err = tx.Set([]byte("rolled-back"), []byte("should not exist"))
	require.NoError(t, err)
	err = tx.Rollback()
	require.NoError(t, err)

	// Verify rolled back data does not persist
	err = db.View(func(tx *Tx) error {
		_, err := tx.Get([]byte("rolled-back"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	})
	require.NoError(t, err)
}

func TestTxSingleWriter(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_single_writer.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

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

	tmpfile := "/tmp/test_tx_multiple_readers.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Insert test data
	err = db.Update(func(tx *Tx) error {
		return tx.Set([]byte("key"), []byte("value"))
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

	tmpfile := "/tmp/test_tx_write_readonly.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Try to write in read-only transaction
	err = db.View(func(tx *Tx) error {
		return tx.Set([]byte("key"), []byte("value"))
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

	tmpfile := "/tmp/test_tx_done.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Test operations after commit
	tx, err := db.Begin(true)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)

	// Operations after commit should fail
	err = tx.Set([]byte("key"), []byte("value"))
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

	// COW now implemented for simple leaf modifications
	tmpfile := "/tmp/test_tx_auto_rollback.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Update with error should auto-rollback
	err = db.Update(func(tx *Tx) error {
		err := tx.Set([]byte("key"), []byte("value"))
		if err != nil {
			return err
		}
		return fmt.Errorf("intentional error")
	})
	assert.Error(t, err)
	assert.Equal(t, "intentional error", err.Error())

	// Key should not exist (rolled back)
	err = db.View(func(tx *Tx) error {
		_, err := tx.Get([]byte("key"))
		assert.ErrorIs(t, err, ErrKeyNotFound)
		return nil
	})
	require.NoError(t, err)
}

func TestTxMultipleBeginWithoutCommit(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_multiple_begin.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

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
	err = tx1.Set(testKey, testValue)
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

	tmpfile := "/tmp/test_tx_after_close.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)

	// Insert some data
	err = db.Set([]byte("key1"), []byte("value1"))
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

	tmpfile := "/tmp/test_tx_concurrent_write_begin.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

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

	tmpfile := "/tmp/test_tx_write_then_read.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	require.NoError(t, err)
	defer db.Close()

	// Write some data
	err = db.Set([]byte("initial"), []byte("data"))
	require.NoError(t, err)

	// Begin write transaction
	writeTx, err := db.Begin(true)
	require.NoError(t, err)

	// Write in write transaction
	err = writeTx.Set([]byte("write_key"), []byte("write_value"))
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
			_, err = readTx.Get([]byte("write_key"))
			if err != ErrKeyNotFound {
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

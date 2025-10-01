package pkg

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestTxBasicOperations(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_basic.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
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
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Test View (read transaction)
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get([]byte("key1"))
		if err != nil {
			return err
		}
		if string(val) != "value1" {
			t.Errorf("Expected value1, got %s", string(val))
		}

		val, err = tx.Get([]byte("key2"))
		if err != nil {
			return err
		}
		if string(val) != "value2" {
			t.Errorf("Expected value2, got %s", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestTxCommitRollback(t *testing.T) {
	t.Parallel()

	// COW now implemented for simple leaf modifications
	tmpfile := "/tmp/test_tx_commit_rollback.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Test commit
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	err = tx.Set([]byte("committed"), []byte("value"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify committed data persists
	err = db.View(func(tx *Tx) error {
		val, err := tx.Get([]byte("committed"))
		if err != nil {
			return err
		}
		if string(val) != "value" {
			t.Errorf("Expected value, got %s", string(val))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}

	// Test rollback
	tx, err = db.Begin(true)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	err = tx.Set([]byte("rolled-back"), []byte("should not exist"))
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify rolled back data does not persist
	err = db.View(func(tx *Tx) error {
		_, err := tx.Get([]byte("rolled-back"))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestTxSingleWriter(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_single_writer.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Start first write transaction
	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer tx1.Rollback()

	// Try to start second write transaction (should fail)
	_, err = db.Begin(true)
	if err != ErrTxInProgress {
		t.Errorf("Expected ErrTxInProgress, got %v", err)
	}

	// Commit first transaction
	err = tx1.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Now second write transaction should succeed
	tx2, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin after commit should succeed: %v", err)
	}
	tx2.Rollback()
}

func TestTxMultipleReaders(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_multiple_readers.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Insert test data
	err = db.Update(func(tx *Tx) error {
		return tx.Set([]byte("key"), []byte("value"))
	})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Start multiple read transactions
	tx1, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Begin read tx1 failed: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Begin read tx2 failed: %v", err)
	}
	defer tx2.Rollback()

	tx3, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Begin read tx3 failed: %v", err)
	}
	defer tx3.Rollback()

	// All should read successfully
	val1, err := tx1.Get([]byte("key"))
	if err != nil || string(val1) != "value" {
		t.Errorf("tx1 read failed")
	}

	val2, err := tx2.Get([]byte("key"))
	if err != nil || string(val2) != "value" {
		t.Errorf("tx2 read failed")
	}

	val3, err := tx3.Get([]byte("key"))
	if err != nil || string(val3) != "value" {
		t.Errorf("tx3 read failed")
	}
}

func TestTxWriteOnReadOnly(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_write_readonly.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Try to write in read-only transaction
	err = db.View(func(tx *Tx) error {
		return tx.Set([]byte("key"), []byte("value"))
	})
	if err != ErrTxNotWritable {
		t.Errorf("Expected ErrTxNotWritable, got %v", err)
	}

	// Try to delete in read-only transaction
	err = db.View(func(tx *Tx) error {
		return tx.Delete([]byte("key"))
	})
	if err != ErrTxNotWritable {
		t.Errorf("Expected ErrTxNotWritable, got %v", err)
	}
}

func TestTxDoneCheck(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_done.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Test operations after commit
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Operations after commit should fail
	err = tx.Set([]byte("key"), []byte("value"))
	if err != ErrTxDone {
		t.Errorf("Expected ErrTxDone after commit, got %v", err)
	}

	_, err = tx.Get([]byte("key"))
	if err != ErrTxDone {
		t.Errorf("Expected ErrTxDone after commit, got %v", err)
	}

	// Test operations after rollback
	tx2, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	err = tx2.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	_, err = tx2.Get([]byte("key"))
	if err != ErrTxDone {
		t.Errorf("Expected ErrTxDone after rollback, got %v", err)
	}
}

func TestTxAutoRollback(t *testing.T) {
	t.Parallel()

	// COW now implemented for simple leaf modifications
	tmpfile := "/tmp/test_tx_auto_rollback.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Update with error should auto-rollback
	err = db.Update(func(tx *Tx) error {
		err := tx.Set([]byte("key"), []byte("value"))
		if err != nil {
			return err
		}
		return fmt.Errorf("intentional error")
	})
	if err == nil || err.Error() != "intentional error" {
		t.Errorf("Expected intentional error, got %v", err)
	}

	// Key should not exist (rolled back)
	err = db.View(func(tx *Tx) error {
		_, err := tx.Get([]byte("key"))
		if err != ErrKeyNotFound {
			t.Errorf("Expected ErrKeyNotFound after rollback, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View failed: %v", err)
	}
}

func TestTxMultipleBeginWithoutCommit(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_multiple_begin.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Begin first write transaction
	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}

	// Attempt to begin second write transaction without committing first
	tx2, err := db.Begin(true)
	if err != ErrTxInProgress {
		t.Errorf("Expected ErrTxInProgress for second write tx, got: %v", err)
	}
	if tx2 != nil {
		t.Error("Second transaction should be nil when ErrTxInProgress is returned")
		tx2.Rollback()
	}

	// First transaction should still be valid
	testKey := []byte("test_key")
	testValue := []byte("test_value")
	err = tx1.Set(testKey, testValue)
	if err != nil {
		t.Errorf("First transaction should still be valid: %v", err)
	}

	// Commit first transaction
	err = tx1.Commit()
	if err != nil {
		t.Errorf("Failed to commit first transaction: %v", err)
	}

	// Now second write transaction should succeed
	tx3, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Failed to begin write tx after commit: %v", err)
	}
	defer tx3.Rollback()

	// Verify we can access the committed data
	val, err := tx3.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get committed data: %v", err)
	}
	if string(val) != string(testValue) {
		t.Errorf("Expected %s, got %s", string(testValue), string(val))
	}
}

func TestTxOperationsAfterClose(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_after_close.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Insert some data
	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set data: %v", err)
	}

	// Begin transaction before close
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Close database
	err = db.Close()
	if err != nil {
		t.Errorf("Failed to close DB: %v", err)
	}

	// Operations on transaction after db.Close() should still work
	// because transaction captured snapshot
	val, err := tx.Get([]byte("key1"))
	if err != nil {
		t.Logf("Get after close returned error: %v (may be expected)", err)
	} else if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}

	// Rollback should not panic
	err = tx.Rollback()
	if err != nil {
		t.Logf("Rollback after close returned error: %v", err)
	}

	// New transaction after close should fail
	_, err = db.Begin(false)
	if err == nil {
		t.Error("Expected error when beginning transaction after close")
	}
}

func TestTxConcurrentWriteBegin(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_concurrent_write_begin.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Begin first write transaction
	tx1, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Failed to begin first transaction: %v", err)
	}
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

	if errorCount > 0 {
		t.Errorf("%d goroutines had unexpected behavior", errorCount)
	}

	// Commit the first transaction
	err = tx1.Commit()
	if err != nil {
		t.Errorf("Failed to commit: %v", err)
	}

	// Now a new write transaction should succeed
	tx2, err := db.Begin(true)
	if err != nil {
		t.Errorf("Should be able to begin write tx after commit: %v", err)
	}
	if tx2 != nil {
		tx2.Rollback()
	}
}

func TestTxWriteThenReadMultiple(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_write_then_read.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Write some data
	err = db.Set([]byte("initial"), []byte("data"))
	if err != nil {
		t.Fatalf("Failed to set initial data: %v", err)
	}

	// Begin write transaction
	writeTx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Failed to begin write transaction: %v", err)
	}

	// Write in write transaction
	err = writeTx.Set([]byte("write_key"), []byte("write_value"))
	if err != nil {
		t.Errorf("Failed to set in write tx: %v", err)
	}

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
		if err != nil {
			t.Error(err)
		}
	}

	// Commit write transaction
	err = writeTx.Commit()
	if err != nil {
		t.Errorf("Failed to commit write tx: %v", err)
	}

	// New read transaction should see committed write
	readTx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin read tx after commit: %v", err)
	}
	defer readTx.Rollback()

	val, err := readTx.Get([]byte("write_key"))
	if err != nil {
		t.Errorf("Should see committed write: %v", err)
	}
	if string(val) != "write_value" {
		t.Errorf("Expected write_value, got %s", string(val))
	}
}

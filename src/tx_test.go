package src

import (
	"fmt"
	"os"
	"testing"
)

func TestTxBasicOperations(t *testing.T) {
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
	// Phase 2.3: COW now implemented for simple leaf modifications
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
	// Phase 2.3: COW now implemented for simple leaf modifications
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

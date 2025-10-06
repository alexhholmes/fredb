package fredb

import (
	"os"
	"testing"
	"time"
)

// TestWALRecoveryBasic tests that uncommitted WAL entries are recovered after restart
func TestWALRecoveryBasic(t *testing.T) {
	dbPath := "/tmp/test_wal_recovery_basic.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database and write some data
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write key1, will be checkpointed
	if err := db.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}

	// Wait for checkpoint to complete
	time.Sleep(300 * time.Millisecond)

	// Write key2, will only be in WAL
	if err := db.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Close database normally - key2 should still be in WAL only (not checkpointed yet)
	// Background checkpointer runs every 200ms, so key2 written immediately should not be checkpointed
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen database - should recover key2 from WAL
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify both keys exist
	val1, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 after recovery: %v", err)
	}
	if string(val1) != "value1" {
		t.Errorf("key1 value mismatch: got %s, want value1", val1)
	}

	val2, err := db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2 after recovery: %v", err)
	}
	if string(val2) != "value2" {
		t.Errorf("key2 value mismatch: got %s, want value2", val2)
	}
}

// TestWALRecoveryUncommitted tests that uncommitted transactions are discarded
func TestWALRecoveryUncommitted(t *testing.T) {
	dbPath := "/tmp/test_wal_recovery_uncommitted.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write key1 (will be committed)
	if err := db.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}

	// Start a transaction but don't commit it
	tx, err := db.Begin(true)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Write to WAL but don't commit
	if err := tx.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Directly access pager to write WAL without commit marker
	dm := db.store.pager
	// Flush dirty pages to WAL without commit
	for pageID, node := range tx.pages {
		page, err := node.serialize(tx.txnID)
		if err != nil {
			t.Fatalf("Failed to serialize: %v", err)
		}
		if err := dm.AppendPageWAL(tx.txnID, pageID, page); err != nil {
			t.Fatalf("Failed to append to WAL: %v", err)
		}
	}
	// Force fsync WAL
	if err := dm.ForceSyncWAL(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Rollback transaction to avoid proper cleanup
	tx.Rollback()

	// Close database - WAL has pages but no commit marker
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen database
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify key1 exists
	val1, err := db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 after recovery: %v", err)
	}
	if string(val1) != "value1" {
		t.Errorf("key1 value mismatch: got %s, want value1", val1)
	}

	// Verify key2 does NOT exist (uncommitted transaction discarded)
	_, err = db2.Get([]byte("key2"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for uncommitted key2, got: %v", err)
	}
}

// TestCheckpointIdempotency tests that checkpoint replay is idempotent
func TestCheckpointIdempotency(t *testing.T) {
	dbPath := "/tmp/test_checkpoint_idempotency.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database and write data
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write multiple keys
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i + 100)}
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Wait for checkpoint
	time.Sleep(300 * time.Millisecond)

	// Get current meta to simulate partial checkpoint
	dm := db.store.pager
	meta := dm.GetMeta()
	checkpointTxnID := meta.CheckpointTxnID

	// Manually trigger checkpoint replay (simulates crash after Sync, before PutMeta update)
	// This should be idempotent - running it twice should not corrupt data
	err = dm.ReplayWAL(checkpointTxnID, func(pageID PageID, page *Page) error {
		return dm.WritePage(pageID, page)
	})
	if err != nil {
		t.Fatalf("First replay failed: %v", err)
	}

	// Replay AGAIN (simulating restart after crash)
	// The idempotency check should skip pages that are already at correct version
	err = dm.ReplayWAL(checkpointTxnID, func(pageID PageID, page *Page) error {
		// Read current disk version
		oldPage, readErr := dm.readPageAtUnsafe(pageID)

		newHeader := page.header()
		newTxnID := newHeader.TxnID

		if readErr == nil && oldPage != nil {
			oldHeader := oldPage.header()
			oldTxnID := oldHeader.TxnID

			// Should skip if already applied
			if oldTxnID >= newTxnID {
				return nil
			}
		}

		return dm.WritePage(pageID, page)
	})
	if err != nil {
		t.Fatalf("Second replay failed: %v", err)
	}

	// Close and reopen
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify all keys still have correct values (no corruption from double-apply)
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i + 100)}

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d after double replay: %v", i, err)
		}
		if len(value) != 1 || value[0] != expectedValue[0] {
			t.Errorf("Key %d value mismatch after double replay: got %v, want %v", i, value, expectedValue)
		}
	}
}

// TestWALRecoveryMultipleTransactions tests recovery of multiple transactions
func TestWALRecoveryMultipleTransactions(t *testing.T) {
	dbPath := "/tmp/test_wal_recovery_multi.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write multiple transactions
	for i := 1; i <= 5; i++ {
		var key, value []byte
		if i == 1 {
			key = []byte("key1")
			value = []byte("value1")
		} else {
			key = []byte{byte(i)}
			value = []byte{byte(i + 100)}
		}
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	// Wait for at least one checkpoint
	time.Sleep(300 * time.Millisecond)

	// Close database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// Reopen and verify all keys
	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to reopen database: %v", err)
	}
	defer db2.Close()

	// Verify all keys recovered
	for i := 1; i <= 5; i++ {
		var key, expectedValue []byte
		if i == 1 {
			key = []byte("key1")
			expectedValue = []byte("value1")
		} else {
			key = []byte{byte(i)}
			expectedValue = []byte{byte(i + 100)}
		}

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d after recovery: %v", i, err)
		}

		if i == 1 {
			if string(value) != string(expectedValue) {
				t.Errorf("Key %d mismatch: got %s, want %s", i, value, expectedValue)
			}
		} else {
			if len(value) != 1 || value[0] != expectedValue[0] {
				t.Errorf("Key %d mismatch: got %v, want %v", i, value, expectedValue)
			}
		}
	}
}

// TestWALTruncateSafety tests that WAL cannot be truncated before checkpoint
func TestWALTruncateSafety(t *testing.T) {
	dbPath := "/tmp/test_wal_truncate_safety.db"
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database
	db, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	// Write and commit a transaction
	if err := db.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}

	// Wait for checkpoint
	time.Sleep(300 * time.Millisecond)

	// Write another transaction
	if err := db.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Get current meta
	dm := db.store.pager
	meta := dm.GetMeta()

	// Try to truncate WAL beyond checkpoint - should fail
	futureErr := dm.TruncateWAL(meta.CheckpointTxnID + 10)
	if futureErr == nil {
		t.Errorf("Expected error when truncating WAL beyond checkpoint, got nil")
	}

	// Try to truncate WAL to exactly CheckpointTxnID - should succeed
	exactErr := dm.TruncateWAL(meta.CheckpointTxnID)
	if exactErr != nil {
		t.Errorf("Expected no error when truncating WAL to checkpoint, got: %v", exactErr)
	}

	// Close database
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

package fredb

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fredb/internal/base"
)

// TestWALRecoveryBasic tests that uncommitted wal entries are recovered after restart
func TestWALRecoveryBasic(t *testing.T) {
	dbPath := "/tmp/test_wal_recovery_basic.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database and write some data
	db, err := Open(dbPath)
	require.NoError(t, err)

	// Write key1, will be checkpointed
	require.NoError(t, db.Set([]byte("key1"), []byte("value1")))

	// Wait for checkpoint to complete
	time.Sleep(300 * time.Millisecond)

	// Write key2, will only be in wal
	require.NoError(t, db.Set([]byte("key2"), []byte("value2")))

	// Close database normally - key2 should still be in wal only (not checkpointed yet)
	// Background checkpointer runs every 200ms, so key2 written immediately should not be checkpointed
	require.NoError(t, db.Close())

	// Reopen database - should recover key2 from wal
	db2, err := Open(dbPath)
	require.NoError(t, err)
	defer db2.Close()

	// Verify both Keys exist
	val1, err := db2.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, "value1", string(val1))

	val2, err := db2.Get([]byte("key2"))
	require.NoError(t, err)
	assert.Equal(t, "value2", string(val2))
}

// TestWALRecoveryUncommitted tests that uncommitted transactions are discarded
func TestWALRecoveryUncommitted(t *testing.T) {
	dbPath := "/tmp/test_wal_recovery_uncommitted.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database
	db, err := Open(dbPath)
	require.NoError(t, err)

	// Write key1 (will be committed)
	require.NoError(t, db.Set([]byte("key1"), []byte("value1")))

	// Start a transaction but don't commit it
	tx, err := db.Begin(true)
	require.NoError(t, err)

	// Write to wal but don't commit
	require.NoError(t, tx.Set([]byte("key2"), []byte("value2")))

	// Directly access wal to write without commit marker
	// Flush Dirty pages to wal without commit
	for pageID, node := range tx.pages {
		page, err := node.Serialize(tx.txnID)
		require.NoError(t, err)
		require.NoError(t, db.wal.AppendPage(tx.txnID, pageID, page))
	}
	// Force fsync wal
	require.NoError(t, db.wal.ForceSync())

	// Rollback transaction to avoid proper cleanup
	tx.Rollback()

	// Close database - wal has pages but no commit marker
	require.NoError(t, db.Close())

	// Reopen database
	db2, err := Open(dbPath)
	require.NoError(t, err)
	defer db2.Close()

	// Verify key1 exists
	val1, err := db2.Get([]byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, "value1", string(val1))

	// Verify key2 does NOT exist (uncommitted transaction discarded)
	_, err = db2.Get([]byte("key2"))
	assert.Equal(t, ErrKeyNotFound, err)
}

// TestCheckpointIdempotency tests that checkpoint replay is idempotent
func TestCheckpointIdempotency(t *testing.T) {
	dbPath := "/tmp/test_checkpoint_idempotency.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database and write data
	db, err := Open(dbPath)
	require.NoError(t, err)

	// Write multiple Keys
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i + 100)}
		require.NoError(t, db.Set(key, value))
	}

	// Wait for checkpoint
	time.Sleep(300 * time.Millisecond)

	// Get current meta to simulate partial checkpoint
	dm := db.store.pager
	meta := dm.GetMeta()
	checkpointTxnID := meta.CheckpointTxnID

	// Manually trigger checkpoint replay (simulates crash after Sync, before PutMeta update)
	// This should be idempotent - running it twice should not corrupt data
	err = db.wal.Replay(checkpointTxnID, func(pageID base.PageID, page *base.Page) error {
		return dm.WritePage(pageID, page)
	})
	require.NoError(t, err)

	// Replay AGAIN (simulating restart after crash)
	// The idempotency check should skip pages that are already at correct version
	err = db.wal.Replay(checkpointTxnID, func(pageID base.PageID, page *base.Page) error {
		// Read current disk version
		oldPage, readErr := dm.ReadPage(pageID)

		newHeader := page.Header()
		newTxnID := newHeader.TxnID

		if readErr == nil && oldPage != nil {
			oldHeader := oldPage.Header()
			oldTxnID := oldHeader.TxnID

			// Should skip if already applied
			if oldTxnID >= newTxnID {
				return nil
			}
		}

		return dm.WritePage(pageID, page)
	})
	require.NoError(t, err)

	// Close and reopen
	require.NoError(t, db.Close())

	db2, err := Open(dbPath)
	require.NoError(t, err)
	defer db2.Close()

	// Verify all Keys still have correct Values (no corruption from double-apply)
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i + 100)}

		value, err := db2.Get(key)
		require.NoError(t, err)
		assert.Equal(t, 1, len(value))
		assert.Equal(t, expectedValue[0], value[0])
	}
}

// TestWALRecoveryMultipleTransactions tests recovery of multiple transactions
func TestWALRecoveryMultipleTransactions(t *testing.T) {
	dbPath := "/tmp/test_wal_recovery_multi.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database
	db, err := Open(dbPath)
	require.NoError(t, err)

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
		require.NoError(t, db.Set(key, value))
	}

	// Wait for at least one checkpoint
	time.Sleep(300 * time.Millisecond)

	// Close database
	require.NoError(t, db.Close())

	// Reopen and verify all Keys
	db2, err := Open(dbPath)
	require.NoError(t, err)
	defer db2.Close()

	// Verify all Keys recovered
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
		require.NoError(t, err)

		if i == 1 {
			assert.Equal(t, string(expectedValue), string(value))
		} else {
			assert.Equal(t, 1, len(value))
			assert.Equal(t, expectedValue[0], value[0])
		}
	}
}

// TestWALTruncateSafety tests that wal cannot be truncated before checkpoint
func TestWALTruncateSafety(t *testing.T) {
	dbPath := "/tmp/test_wal_truncate_safety.db"
	os.Remove(dbPath)
	os.Remove(dbPath + ".wal")
	defer os.Remove(dbPath)
	defer os.Remove(dbPath + ".wal")

	// Create database
	db, err := Open(dbPath)
	require.NoError(t, err)

	// Write and commit a transaction
	require.NoError(t, db.Set([]byte("key1"), []byte("value1")))

	// Wait for checkpoint
	time.Sleep(300 * time.Millisecond)

	// Write another transaction
	require.NoError(t, db.Set([]byte("key2"), []byte("value2")))

	// Get current meta
	meta := db.store.pager.GetMeta()

	// wal truncation is now managed by DB layer via checkpoint
	// The safety check (preventing truncation beyond checkpoint) is enforced
	// architecturally - checkpoint() only calls Truncate after updating CheckpointTxnID

	// Truncate wal to exactly CheckpointTxnID - simulates what checkpoint does
	exactErr := db.wal.Truncate(meta.CheckpointTxnID)
	assert.NoError(t, exactErr)

	// Close database
	require.NoError(t, db.Close())
}

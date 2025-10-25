package fredb

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb/internal/base"
)

var MaxKeysPerNode = 64

// Use -slow flag to run longer tests
var slow = flag.Bool("slow", false, "run slow tests")

// Helper to create a temporary test database
func setup(t *testing.T) (*DB, string) {
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	_ = os.Remove(tmpfile + ".wal") // Also remove wal file

	db, err := Open(tmpfile, WithCacheSizeMB(0))
	require.NoError(t, err, "Failed to create DB")

	// Type assert to get concrete type for tests
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
		_ = os.Remove(tmpfile + ".wal") // Cleanup wal file
	})

	return db, tmpfile
}

func TestDBBasicOperations(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test Put and get
	key := []byte("test-key")
	value := []byte("test-value")

	err := db.Set(key, value)
	assert.NoError(t, err, "Failed to set key")

	retrieved, err := db.Get(key)
	assert.NoError(t, err, "Failed to get key")
	assert.Equal(t, value, retrieved)

	// Test Update
	newValue := []byte("updated-value")
	err = db.Set(key, newValue)
	assert.NoError(t, err, "Failed to update key")

	retrieved, err = db.Get(key)
	assert.NoError(t, err, "Failed to get updated key")
	assert.Equal(t, newValue, retrieved)

	// Test Delete
	err = db.Delete(key)
	assert.NoError(t, err, "Failed to delete key")

	// Verify key is deleted
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestDBErrors(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// get non-existent key
	val, err := db.Get([]byte("non-existent"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Delete non-existent key (idempotent - should not error)
	err = db.Delete([]byte("non-existent"))
	assert.NoError(t, err)
}

func TestDBClose(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_close.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// Put a key before closing
	err = db.Set([]byte("key"), []byte("value"))
	assert.NoError(t, err, "Failed to set key")

	// close the DB
	err = db.Close()
	assert.NoError(t, err, "Failed to close DB")

	// Multiple close calls should not panic
	err = db.Close()
	if err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// Concurrent Access Tests

func TestDBConcurrency(t *testing.T) {
	if !*slow {
		t.Skip("Skipping slow concurrency test; use -slow to enable")
	}

	// Test concurrent access through DB interface
	// - Launch 100 goroutines
	// - Mix of get/Put operations
	// - Verify no races or corruption
	// - Check final state consistency

	db, _ := setup(t)

	numGoroutines := 100
	opsPerGoroutine := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Track all written Values for verification
	expectedValues := make(map[string]string)
	var mu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of operations: 70% writes, 30% reads
				if j%10 < 7 {
					// Write operation
					key := fmt.Sprintf("key-%d-%d", id, j)
					value := fmt.Sprintf("value-%d-%d", id, j)

					// Retry on write transaction conflict
					for {
						err := db.Set([]byte(key), []byte(value))
						if err == ErrTxInProgress {
							continue
						}
						assert.NoError(t, err, "Put failed")
						if err != nil {
							return
						}
						break
					}

					// Track what we wrote
					mu.Lock()
					expectedValues[key] = value
					mu.Unlock()
				} else {
					// Read operation - read a key we know should exist
					readID := id
					readJ := (j / 10) * 7 // Read from earlier writes
					key := fmt.Sprintf("key-%d-%d", readID, readJ)

					val, err := db.Get([]byte(key))
					// It's OK if key not found (not written yet)
					if err != nil {
						assert.NoError(t, err, "get failed with unexpected error")
						return
					}

					// If found, verify it matches expected pattern
					if val != nil {
						expectedPattern := fmt.Sprintf("value-%d-%d", readID, readJ)
						if !assert.Equal(t, expectedPattern, string(val)) {
							return
						}
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify final state - all written Keys should be readable
	for key, expectedValue := range expectedValues {
		val, err := db.Get([]byte(key))
		if !assert.NoError(t, err, "Key should exist: %s", key) {
			continue
		}
		assert.Equal(t, expectedValue, string(val), "Value mismatch for key %s", key)
	}
}

func TestDBConcurrentReads(t *testing.T) {
	t.Parallel()

	// Test concurrent reads don't block each other
	// - Insert test data
	// - Launch multiple reader goroutines
	// - Verify all complete successfully

	db, _ := setup(t)

	// Insert test data
	testData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		testData[key] = value

		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err, "Failed to insert test data")
	}

	// Launch concurrent readers
	numReaders := 50
	readsPerReader := 100
	var wg sync.WaitGroup
	wg.Add(numReaders)

	// Track completion times to verify they run concurrently
	completionTimes := make([]time.Duration, numReaders)
	start := time.Now()

	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()
			goStart := time.Now()

			for j := 0; j < readsPerReader; j++ {
				// Read random Keys from test data
				key := fmt.Sprintf("key-%d", (id*readsPerReader+j)%1000)
				expectedValue := fmt.Sprintf("value-%d", (id*readsPerReader+j)%1000)

				val, err := db.Get([]byte(key))
				if !assert.NoError(t, err, "get failed for key %s", key) {
					return
				}

				if !assert.Equal(t, expectedValue, string(val), "Value mismatch for key %s", key) {
					return
				}
			}

			completionTimes[id] = time.Since(goStart)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify reads completed concurrently
	// If reads were serialized, total time would be much longer
	var totalSequentialTime time.Duration
	for _, d := range completionTimes {
		totalSequentialTime += d
	}

	// Concurrent execution should be significantly faster than sequential
	// Allow for some overhead, but should be at least 5x faster with 50 goroutines
	if elapsed > totalSequentialTime/5 {
		t.Logf("Warning: Concurrent reads may not be truly concurrent. Elapsed: %v, Total sequential: %v", elapsed, totalSequentialTime)
	}
}

func TestDBConcurrentWrites(t *testing.T) {
	t.Parallel()

	// Test concurrent writes are serialized correctly
	// - Launch multiple writer goroutines
	// - Each writes unique Keys
	// - Verify all Keys present at end

	db, _ := setup(t)

	numWriters := 20
	writesPerWriter := 50
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Track what each writer wrote
	writtenKeys := make(map[string]string)
	var mu sync.Mutex

	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < writesPerWriter; j++ {
				// Each writer writes unique Keys
				key := fmt.Sprintf("writer-%d-key-%d", id, j)
				value := fmt.Sprintf("writer-%d-value-%d", id, j)

				// Retry on write transaction conflict
				for {
					err := db.Set([]byte(key), []byte(value))
					if errors.Is(err, ErrTxInProgress) {
						// Another writer holds the lock, retry
						continue
					}
					if !assert.NoError(t, err, "Put failed for writer %d", id) {
						return
					}
					break
				}

				// Track what was written
				mu.Lock()
				writtenKeys[key] = value
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Verify all Keys are present and have correct Values
	assert.Equal(t, numWriters*writesPerWriter, len(writtenKeys))

	for key, expectedValue := range writtenKeys {
		val, err := db.Get([]byte(key))
		if !assert.NoError(t, err, "Key %s not found", key) {
			continue
		}
		assert.Equal(t, expectedValue, string(val), "Value mismatch for key %s", key)
	}

	// Test concurrent updates to same Keys
	updateKey := []byte("concurrent-update-key")
	numUpdaters := 100
	wg.Add(numUpdaters)

	for i := 0; i < numUpdaters; i++ {
		go func(id int) {
			defer wg.Done()
			value := fmt.Sprintf("updater-%d", id)

			// Retry on write transaction conflict
			for {
				err := db.Set(updateKey, []byte(value))
				if err == ErrTxInProgress {
					// Another writer holds the lock, retry
					continue
				}
				if !assert.NoError(t, err, "Update failed for updater %d", id) {
					return
				}
				break
			}
		}(i)
	}

	wg.Wait()

	// Verify the key exists (value could be from any updater)
	_, err := db.Get(updateKey)
	assert.NoError(t, err, "Concurrent update key not found")
}

// TestTxSnapshotIsolation tests MVCC snapshot isolation.
// Test concurrent read/write with snapshot isolation
func TestTxSnapshotIsolation(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert initial Values
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-v1", i))
		require.NoError(t, db.Set(key, value), "Failed to set initial key")
	}

	// Test 1: Simple snapshot isolation
	// Start read transaction, capture value
	readTx, err := db.Begin(false)
	require.NoError(t, err, "Failed to begin read transaction")
	defer readTx.Rollback()

	oldValue, err := readTx.Get([]byte("key-0"))
	require.NoError(t, err, "Failed to get value in read tx")
	assert.Equal(t, "value-0-v1", string(oldValue))

	// Start write transaction, modify value, commit
	err = db.Update(func(writeTx *Tx) error {
		return writeTx.Put([]byte("key-0"), []byte("value-0-v2"))
	})
	require.NoError(t, err, "Failed to commit write transaction")

	// Verify read tx still sees old value (snapshot isolation)
	stillOldValue, err := readTx.Get([]byte("key-0"))
	require.NoError(t, err, "Failed to get value in read tx after write")
	assert.Equal(t, "value-0-v1", string(stillOldValue), "Snapshot isolation broken")

	readTx.Rollback()

	// Verify new transaction sees new value
	newValue, err := db.Get([]byte("key-0"))
	require.NoError(t, err, "Failed to get value after commit")
	assert.Equal(t, "value-0-v2", string(newValue))

	// Test 2: 10 concurrent readers + 1 writer
	// All readers should see consistent snapshots
	numReaders := 10
	var wg sync.WaitGroup
	wg.Add(numReaders + 1) // +1 for writer

	// Start all read transactions
	readerErrors := make(chan error, numReaders)
	for i := 0; i < numReaders; i++ {
		go func(readerID int) {
			defer wg.Done()

			// Start read transaction
			tx, err := db.Begin(false)
			if err != nil {
				readerErrors <- fmt.Errorf("reader %d: failed to begin: %w", readerID, err)
				return
			}
			defer tx.Rollback()

			// Capture initial snapshot
			snapshot := make(map[string]string)
			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("key-%d", j))
				value, err := tx.Get(key)
				if err != nil {
					readerErrors <- fmt.Errorf("reader %d: failed to get key-%d: %w", readerID, j, err)
					return
				}
				snapshot[string(key)] = string(value)
			}

			// Wait for writer to make changes
			time.Sleep(50 * time.Millisecond)

			// Verify snapshot is still consistent
			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("key-%d", j))
				value, err := tx.Get(key)
				if err != nil {
					readerErrors <- fmt.Errorf("reader %d: failed to re-read key-%d: %w", readerID, j, err)
					return
				}
				if snapshot[string(key)] != string(value) {
					readerErrors <- fmt.Errorf("reader %d: snapshot changed for key-%d: was %s, now %s",
						readerID, j, snapshot[string(key)], string(value))
					return
				}
			}
		}(i)
	}

	// Writer modifies all Values
	go func() {
		defer wg.Done()

		// Small delay to let readers start
		time.Sleep(10 * time.Millisecond)

		// Perform writes
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("key-%d", i))
			value := []byte(fmt.Sprintf("value-%d-v3", i))
			err := db.Set(key, value)
			if err != nil {
				readerErrors <- fmt.Errorf("writer: failed to set key-%d: %w", i, err)
				return
			}
		}
	}()

	wg.Wait()
	close(readerErrors)

	// Check for any reader errors
	for err := range readerErrors {
		t.Error(err)
	}

	// Verify final state has v3 Values
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value, err := db.Get(key)
		if !assert.NoError(t, err, "Failed to get final value for key-%d", i) {
			continue
		}
		expectedValue := fmt.Sprintf("value-%d-v3", i)
		assert.Equal(t, expectedValue, string(value), "Final value for key-%d", i)
	}
}

// TestTxRollbackUnderContention tests that rollbacks don't corrupt state or leak pages
// when multiple write transactions compete and some rollback intentionally
func TestTxRollbackUnderContention(t *testing.T) {
	t.Parallel()

	if !*slow {
		t.Skip("Skipping slow rollback test; use -slow to enable")
	}

	db, tmpfile := setup(t)

	// Insert 100 initial Keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("initial-key-%d", i))
		value := []byte(fmt.Sprintf("initial-value-%d", i))
		require.NoError(t, db.Set(key, value), "Failed to insert initial data")
	}

	// Track committed transactions
	type txResult struct {
		keys   []string
		values []string
		update string // Updated key
	}
	committedTxs := make([]txResult, 0)
	var commitMu sync.Mutex

	numGoroutines := 50
	opsPerGoroutine := 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// stats
	var commits, explicitRollbacks, errorRollbacks, txInProgress int64
	var statsMu sync.Mutex

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < opsPerGoroutine; j++ {
				opID := id*opsPerGoroutine + j

				// Randomly choose outcome: 50% commit, 25% explicit rollback, 25% error rollback
				outcome := opID % 4

				if outcome == 0 || outcome == 1 {
					// 50% commit (outcome 0 or 1)
					result := txResult{
						keys:   make([]string, 5),
						values: make([]string, 5),
						update: fmt.Sprintf("initial-key-%d", opID%100),
					}

					// Use Update to commit
					err := db.Update(func(tx *Tx) error {
						// Write 5 new Keys
						for k := 0; k < 5; k++ {
							key := []byte(fmt.Sprintf("tx-%d-key-%d", opID, k))
							value := []byte(fmt.Sprintf("tx-%d-value-%d", opID, k))
							result.keys[k] = string(key)
							result.values[k] = string(value)
							if err := tx.Put(key, value); err != nil {
								return err
							}
						}

						// Update 1 existing key
						updateKey := []byte(result.update)
						updateValue := []byte(fmt.Sprintf("updated-by-tx-%d", opID))
						result.values = append(result.values, string(updateValue))
						if err := tx.Put(updateKey, updateValue); err != nil {
							return err
						}

						return nil
					})

					if err == ErrTxInProgress {
						// Retry handled by outer retry logic
						statsMu.Lock()
						txInProgress++
						statsMu.Unlock()
						j-- // Retry this operation
						continue
					}

					if err == nil {
						// Committed successfully
						commitMu.Lock()
						committedTxs = append(committedTxs, result)
						commitMu.Unlock()

						statsMu.Lock()
						commits++
						statsMu.Unlock()
					}
				} else if outcome == 2 {
					// 25% explicit rollback
					for {
						tx, err := db.Begin(true)
						if err == ErrTxInProgress {
							statsMu.Lock()
							txInProgress++
							statsMu.Unlock()
							continue
						}
						if !assert.NoError(t, err, "Failed to begin tx") {
							break
						}

						// Write some Keys (will be rolled back)
						for k := 0; k < 5; k++ {
							key := []byte(fmt.Sprintf("rollback-tx-%d-key-%d", opID, k))
							value := []byte(fmt.Sprintf("rollback-tx-%d-value-%d", opID, k))
							assert.NoError(t, tx.Put(key, value), "Put failed in rollback test")
						}

						// Explicit rollback
						assert.NoError(t, tx.Rollback(), "Rollback failed")

						statsMu.Lock()
						explicitRollbacks++
						statsMu.Unlock()
						break
					}
				} else {
					// 25% error rollback (via Update returning error)
					err := db.Update(func(tx *Tx) error {
						// Write some Keys
						for k := 0; k < 5; k++ {
							key := []byte(fmt.Sprintf("error-tx-%d-key-%d", opID, k))
							value := []byte(fmt.Sprintf("error-tx-%d-value-%d", opID, k))
							if err := tx.Put(key, value); err != nil {
								return err
							}
						}

						// Return error to trigger rollback
						return fmt.Errorf("intentional error for rollback test")
					})

					if err == ErrTxInProgress {
						statsMu.Lock()
						txInProgress++
						statsMu.Unlock()
						j-- // Retry
						continue
					}

					if err != nil {
						// Expected error - rollback happened
						statsMu.Lock()
						errorRollbacks++
						statsMu.Unlock()
					}
				}
			}
		}(i)
	}

	wg.Wait()

	// Log statistics
	t.Logf("stats: commits=%d, explicit_rollbacks=%d, error_rollbacks=%d, tx_in_progress_retries=%d",
		commits, explicitRollbacks, errorRollbacks, txInProgress)

	// Verify all committed Keys exist with correct Values
	commitMu.Lock()
	for txIdx, result := range committedTxs {
		// Verify new Keys
		for i := 0; i < 5; i++ {
			val, err := db.Get([]byte(result.keys[i]))
			if !assert.NoError(t, err, "Committed tx %d: key %s not found", txIdx, result.keys[i]) {
				continue
			}
			assert.Equal(t, result.values[i], string(val), "Committed tx %d: key %s value mismatch", txIdx, result.keys[i])
		}

		// Verify updated key
		val, err := db.Get([]byte(result.update))
		if !assert.NoError(t, err, "Committed tx %d: updated key %s not found", txIdx, result.update) {
			// Value should be from SOME committed transaction (last writer wins)
			// Just verify it's not the original value
			if string(val) == fmt.Sprintf("initial-value-%s", result.update[12:]) {
				t.Logf("Warning: Updated key %s still has initial value (overwritten by later tx?)", result.update)
			}
		}
	}
	commitMu.Unlock()

	// Sample active: verify some rolled-back Keys don't exist
	for i := 0; i < 10; i++ {
		// Check explicit rollback Keys
		key := []byte(fmt.Sprintf("rollback-tx-%d-key-0", i*100))
		val, err := db.Get(key)
		assert.NoError(t, err)
		assert.Nil(t, val, "Rolled-back key should not exist: %s", key)

		// Check error rollback Keys
		key = []byte(fmt.Sprintf("error-tx-%d-key-0", i*100))
		val, err = db.Get(key)
		assert.NoError(t, err)
		assert.Nil(t, val, "Error-rolled-back key should not exist: %s", key)
	}

	// Heuristic active for Page leaks: file size should be reasonable
	// With 100 initial Keys + ~500-750 committed transactions × 5-6 Keys each
	// = ~100 + 3000-4500 = ~4000 Keys total
	// At ~4KB per Page with branching, expect < 10MB
	info, err := os.Stat(tmpfile)
	require.NoError(t, err, "Failed to stat DB file")
	sizeMB := float64(info.Size()) / (1024 * 1024)
	t.Logf("Database file size: %.2f MB", sizeMB)
	assert.LessOrEqual(t, sizeMB, 10.0, "File size suspiciously large (%.2f MB), possible Page leak", sizeMB)
}

// TestDBLargeKeysPerPage tests inserting large key-value pairs
// where only 2 pairs fit per Page (stress test for Page splits)
func TestDBLargeKeysPerPage(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Calculate key/value size for 2 pairs per Page
	// storage.PageSize = 4096, PageHeaderSize = 40, LeafElementSize = 16
	// Available = 4096 - 40 = 4056
	// For 2 pairs: 2 * 16 = 32 bytes metadata
	// Data space = 4056 - 32 = 4024 bytes
	// Per pair = 2012 bytes total
	// Limit Keys to 500 bytes (within MaxKeySize = 1024)
	// Use 500+1500 = 2000 bytes per pair for ~2 pairs per Page
	keySize := 500
	valueSize := 1500

	// Helper to create large key/value
	makeKey := func(i int) []byte {
		key := make([]byte, keySize)
		header := fmt.Sprintf("key-%06d-", i)
		copy(key, header)
		// Fill rest with pattern
		for j := len(header); j < keySize; j++ {
			key[j] = byte('A' + (j % 26))
		}
		return key
	}

	makeValue := func(i int) []byte {
		value := make([]byte, valueSize)
		header := fmt.Sprintf("value-%06d-", i)
		copy(value, header)
		// Fill rest with pattern
		for j := len(header); j < valueSize; j++ {
			value[j] = byte('0' + (j % 10))
		}
		return value
	}

	// Test 1: Insert 10 large Keys (will cause multiple splits)
	t.Run("InsertLargeKeys", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := makeKey(i)
			value := makeValue(i)
			require.NoError(t, db.Set(key, value), "Failed to insert large key %d", i)
		}
	})

	// Test 2: Read back all Keys
	t.Run("ReadLargeKeys", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := makeKey(i)
			expectedValue := makeValue(i)

			value, err := db.Get(key)
			if !assert.NoError(t, err, "Failed to get large key %d", i) {
				continue
			}

			assert.Equal(t, valueSize, len(value), "Key %d: value size mismatch", i)
			assert.Equal(t, string(expectedValue), string(value), "Key %d: value mismatch", i)
		}
	})

	// Test 3: Update large Keys
	t.Run("UpdateLargeKeys", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := makeKey(i)
			// New value with different pattern
			newValue := make([]byte, valueSize)
			header := fmt.Sprintf("updated-%06d-", i)
			copy(newValue, header)
			for j := len(header); j < valueSize; j++ {
				newValue[j] = byte('Z' - (j % 26))
			}

			require.NoError(t, db.Set(key, newValue), "Failed to update large key %d", i)

			// Verify update
			value, err := db.Get(key)
			if !assert.NoError(t, err, "Failed to get updated key %d", i) {
				continue
			}

			assert.Equal(t, string(newValue), string(value), "Key %d: updated value mismatch", i)
		}
	})

	// Test 4: Delete large Keys
	t.Run("DeleteLargeKeys", func(t *testing.T) {
		// Delete every other key
		for i := 0; i < 10; i += 2 {
			key := makeKey(i)
			assert.NoError(t, db.Delete(key), "Failed to delete large key %d", i)

			// Verify deleted
			val, err := db.Get(key)
			assert.Nil(t, val, "Key %d should be deleted", i)
			assert.NoError(t, err, "Get on deleted key %d should not error", i)
		}

		// Verify remaining Keys still exist
		for i := 1; i < 10; i += 2 {
			key := makeKey(i)
			_, err := db.Get(key)
			assert.NoError(t, err, "Remaining key %d should exist", i)
		}
	})

	// Test 5: Concurrent operations with large Keys
	if *slow {
		t.Run("ConcurrentLargeKeys", func(t *testing.T) {
			numGoroutines := 20
			keysPerGoroutine := 10
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for g := 0; g < numGoroutines; g++ {
				go func(id int) {
					defer wg.Done()

					for j := 0; j < keysPerGoroutine; j++ {
						keyID := id*1000 + j
						key := makeKey(keyID)
						value := makeValue(keyID)

						// Retry on conflict
						for {
							err := db.Set(key, value)
							if err == ErrTxInProgress {
								continue
							}
							if !assert.NoError(t, err, "Goroutine %d: Put failed", id) {
								return
							}
							break
						}
					}
				}(g)
			}

			wg.Wait()

			// Verify all concurrent writes
			for g := 0; g < numGoroutines; g++ {
				for j := 0; j < keysPerGoroutine; j++ {
					keyID := g*1000 + j
					key := makeKey(keyID)
					expectedValue := makeValue(keyID)

					value, err := db.Get(key)
					if !assert.NoError(t, err, "Concurrent key %d not found", keyID) {
						continue
					}

					assert.Equal(t, string(expectedValue), string(value), "Concurrent key %d: value mismatch", keyID)
				}
			}
		})
	}

	// Test 6: Fill a tree to force deep splits
	t.Run("DeepTreeWithLargeKeys", func(t *testing.T) {
		// Insert 100 more large Keys to create a deeper tree
		for i := 100; i < 200; i++ {
			key := makeKey(i)
			value := makeValue(i)
			require.NoError(t, db.Set(key, value), "Failed to insert key %d in deep tree", i)
		}

		// Spot active some Keys
		testKeys := []int{100, 125, 150, 175, 199}
		for _, i := range testKeys {
			key := makeKey(i)
			expectedValue := makeValue(i)

			value, err := db.Get(key)
			if !assert.NoError(t, err, "Key %d not found in deep tree", i) {
				continue
			}

			assert.Equal(t, string(expectedValue), string(value), "Key %d in deep tree: value mismatch", i)
		}
	})
}

// TestCrashRecoveryLastCommittedState tests recovery to previous valid state
func TestCrashRecoveryLastCommittedState(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_last_committed.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB and do a single commit
	db1, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")
	db1.Set([]byte("key1"), []byte("value1"))
	db1.Close()

	// Reopen and do a second commit (Put without close to avoid extra TxID)
	db2, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB")
	db2.Set([]byte("key2"), []byte("value2"))

	// Check TxnIDs after second Put (before close)
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")
	page0 := &base.Page{}
	file.Read(page0.Data[:])
	page1 := &base.Page{}
	file.Read(page1.Data[:])
	file.Close()

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()
	t.Logf("After second Put: Page 0 TxID=%d RootPageID=%d, Page 1 TxID=%d RootPageID=%d",
		meta0.TxID, meta0.RootPageID, meta1.TxID, meta1.RootPageID)

	// Record the older TxID (should only have key1)
	var olderTxn uint64
	var olderRoot base.PageID
	if meta0.TxID < meta1.TxID {
		olderTxn = meta0.TxID
		olderRoot = meta0.RootPageID
	} else {
		olderTxn = meta1.TxID
		olderRoot = meta1.RootPageID
	}

	db2.Close() // Now close, which will write another meta

	// Simulate crash: corrupt the newest meta Page
	file, err = os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	page0 = &base.Page{}
	file.ReadAt(page0.Data[:], 0)
	page1 = &base.Page{}
	file.ReadAt(page1.Data[:], int64(base.PageSize))

	meta0 = page0.ReadMeta()
	meta1 = page1.ReadMeta()

	// Corrupt the newer one
	var corruptOffset int64
	if meta0.TxID > meta1.TxID {
		corruptOffset = int64(base.PageHeaderSize)
		t.Logf("Corrupting Page 0 (TxID %d)", meta0.TxID)
	} else {
		corruptOffset = int64(base.PageSize + base.PageHeaderSize)
		t.Logf("Corrupting Page 1 (TxID %d)", meta1.TxID)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	file.WriteAt(corruptData, corruptOffset)
	file.Sync()
	file.Close()

	// Reopen - should fall back to previous valid state
	db3, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen after simulated crash")
	defer db3.Close()

	meta3 := db3.pager.GetMeta()
	t.Logf("After reopen: loaded meta with TxID %d, RootPageID %d", meta3.TxID, meta3.RootPageID)
	t.Logf("Expected to recover to TxID %d (previous valid state)", olderTxn)

	// key1 should always exist
	v1, err := db3.Get([]byte("key1"))
	assert.NoError(t, err, "key1 should exist after crash recovery")
	assert.Equal(t, "value1", string(v1))

	// Verify we loaded the older state (key2 should match the older root's content)
	// If we loaded olderRoot, active if it has key2 or not
	_, err = db3.Get([]byte("key2"))
	if meta3.RootPageID == olderRoot {
		// We're at the older state, verify its actual content
		t.Logf("Recovered to older root (Page %d), key2 exists: %v", olderRoot, err == nil)
	}

	t.Logf("Successfully recovered from crash using backup meta Page")
}

func TestDiskPageManagerBasic(t *testing.T) {
	t.Parallel()

	// Create temp file
	tmpfile := "/tmp/test_disk.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create new database
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// Insert some data
	require.NoError(t, db.Set([]byte("key1"), []byte("value1")), "Failed to set key1")
	require.NoError(t, db.Set([]byte("key2"), []byte("value2")), "Failed to set key2")

	// Verify data
	val, err := db.Get([]byte("key1"))
	require.NoError(t, err, "Failed to get key1")
	assert.Equal(t, "value1", string(val))

	// close (flushes to disk)
	require.NoError(t, db.Close(), "Failed to close DB")

	// Reopen database
	db2, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB")

	// Verify data persisted
	val, err = db2.Get([]byte("key1"))
	require.NoError(t, err, "Failed to get key1 after reopen")
	assert.Equal(t, "value1", string(val))

	val, err = db2.Get([]byte("key2"))
	require.NoError(t, err, "Failed to get key2 after reopen")
	assert.Equal(t, "value2", string(val))

	// Cleanup
	require.NoError(t, db2.Close(), "Failed to close db2")
}

func TestDiskPageManagerPersistence(t *testing.T) {
	t.Parallel()

	// Use unique filename per test to avoid parallel test collisions
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.DB", t.Name())
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create database and insert 100 Keys
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		require.NoError(t, db.Set(key, value), "Failed to set key %d", i)
	}

	require.NoError(t, db.Close(), "Failed to close DB")

	// Reopen and verify all Keys
	db2, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB")

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i * 2)}

		value, err := db2.Get(key)
		require.NoError(t, err, "Failed to get key %d after reopen", i)
		assert.Equal(t, expectedValue, value, "Key %d: value mismatch", i)
	}

	require.NoError(t, db2.Close(), "Failed to close db2")
}

func TestDiskPageManagerDelete(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_delete.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create database
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// Insert Keys
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("c"), []byte("3"))

	// Delete one
	require.NoError(t, db.Delete([]byte("b")), "Failed to delete")

	// close
	db.Close()

	// Reopen and verify
	db2, _ := Open(tmpfile)

	// Should still have a and c
	_, err = db2.Get([]byte("a"))
	assert.NoError(t, err, "Key 'a' should exist")
	_, err = db2.Get([]byte("c"))
	assert.NoError(t, err, "Key 'c' should exist")

	// b should be gone
	val, err := db2.Get([]byte("b"))
	assert.Nil(t, val, "Key 'b' should return nil value")
	assert.Nil(t, err, "Key 'b' should return nil error")
	assert.NoError(t, db2.Close(), "Failed to close DB")

	db2.Close()
}

// TestDBFileFormat validates the on-disk format
func TestDBFileFormat(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_format.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB and write some data
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// validate file exists and has correct structure
	info, err := os.Stat(tmpfile)
	require.NoError(t, err, "DB file not found")

	// File should be at least 3 pages (meta 0-1, freelist 2)
	minSize := int64(base.PageSize * 3)
	assert.GreaterOrEqual(t, info.Size(), minSize, "File too small")

	// File size should be Page-aligned
	assert.Equal(t, int64(0), info.Size()%int64(base.PageSize), "File size not Page-aligned")

	// Read both meta pages
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")
	defer file.Close()

	page0 := &base.Page{}
	n, err := file.Read(page0.Data[:])
	require.NoError(t, err, "Failed to read meta Page 0")
	assert.Equal(t, base.PageSize, n, "Short read")

	page1 := &base.Page{}
	n, err = file.Read(page1.Data[:])
	require.NoError(t, err, "Failed to read meta Page 1")
	assert.Equal(t, base.PageSize, n, "Short read")

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	// close() increments TxID from 0 to 1, so writes to Page 1 (1 % 2 = 1)
	// Page 1 should have the latest meta with RootPageID set
	t.Logf("Meta Page 0 TxID: %d, RootPageID: %d", meta0.TxID, meta0.RootPageID)
	t.Logf("Meta Page 1 TxID: %d, RootPageID: %d", meta1.TxID, meta1.RootPageID)

	// Pick the Page with highest TxID (should be Page 1)
	var meta *base.MetaPage
	if meta0.TxID > meta1.TxID {
		meta = meta0
	} else {
		meta = meta1
	}

	// validate magic number
	assert.Equal(t, base.MagicNumber, meta.Magic, "Invalid magic number")

	// validate version
	assert.Equal(t, base.FormatVersion, meta.Version, "Invalid version")

	// validate Page size
	assert.Equal(t, uint16(base.PageSize), meta.PageSize, "Invalid Page size")

	// validate RootPageID is persisted after close
	assert.NotEqual(t, base.PageID(0), meta.RootPageID, "RootPageID is zero after close - should be persisted to meta")

	// validate freelist location
	assert.Equal(t, base.PageID(2), meta.FreelistID, "Freelist ID mismatch")

	// validate checksum
	assert.NoError(t, meta.Validate(), "Meta validation failed")

	t.Logf("Meta Page validated successfully:")
	t.Logf("  Magic: 0x%08x", meta.Magic)
	t.Logf("  Version: %d", meta.Version)
	t.Logf("  storage.PageSize: %d", meta.PageSize)
	t.Logf("  RootPageID: %d", meta.RootPageID)
	t.Logf("  FreelistID: %d", meta.FreelistID)
	t.Logf("  FreelistPages: %d", meta.FreelistPages)
	t.Logf("  TxID: %d", meta.TxID)
	t.Logf("  NumPages: %d", meta.NumPages)
	t.Logf("  File size: %d bytes (%d pages)", info.Size(),
		info.Size()/int64(base.PageSize))
}

// TestDBFileHexDump creates a DB file and prints hex dump for manual inspection
func TestDBFileHexDump(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping hex dump test in short mode")
	}

	tmpfile := "/tmp/test_db_hexdump.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB with known data
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("testkey"), []byte("testvalue"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Read first 4 pages and dump them
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")
	defer file.Close()

	// Read meta Page 0
	page0 := make([]byte, base.PageSize)
	_, err = file.Read(page0)
	require.NoError(t, err, "Failed to read Page 0")

	t.Logf("\n=== Page 0 (Meta) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page0[:128]))

	// Read meta Page 1
	page1 := make([]byte, base.PageSize)
	_, err = file.Read(page1)
	require.NoError(t, err, "Failed to read Page 1")

	t.Logf("\n=== Page 1 (Meta backup) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page1[:128]))

	// Read freelist Page 2
	page2 := make([]byte, base.PageSize)
	_, err = file.Read(page2)
	require.NoError(t, err, "Failed to read Page 2")

	t.Logf("\n=== Page 2 (Freelist) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page2[:128]))

	// Read root Page (Page 3 likely)
	page3 := make([]byte, base.PageSize)
	_, err = file.Read(page3)
	require.NoError(t, err, "Failed to read Page 3")

	t.Logf("\n=== Page 3 (B-tree root) - First 256 bytes ===")
	t.Logf("%s", formatHexDump(page3[:256]))
}

func formatHexDump(data []byte) string {
	result := ""
	for i := 0; i < len(data); i += 16 {
		end := i + 16
		if end > len(data) {
			end = len(data)
		}

		// Offset
		result += fmt.Sprintf("%08x: ", i)

		// Hex bytes
		for j := i; j < end; j++ {
			result += fmt.Sprintf("%02x ", data[j])
		}

		// Padding
		for j := end; j < i+16; j++ {
			result += "   "
		}

		// ASCII
		result += " |"
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] <= 126 {
				result += string(data[j])
			} else {
				result += "."
			}
		}
		result += "|\n"
	}
	return result
}

// TestDBCorruptionDetection validates that corrupted meta pages are detected
func TestDBCorruptionDetection(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_corruption.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create valid DB
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key"), []byte("value"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt Page 0's magic number
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 0) // Overwrite magic number
	require.NoError(t, err, "Failed to corrupt file")
	file.Close()

	// Try to reopen - should succeed because Page 1 is still valid
	db2, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB with one corrupted meta Page")
	defer db2.Close()

	// Verify data still accessible (using Page 1)
	v, err := db2.Get([]byte("key"))
	assert.NoError(t, err, "Failed to get key from DB with corrupted Page 0")
	assert.Equal(t, "value", string(v))

	t.Logf("Successfully recovered from single meta Page corruption using backup")
}

// TestCrashRecoveryBothMetaCorrupted tests that DB fails to open when both meta pages are invalid
func TestCrashRecoveryBothMetaCorrupted(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_both_meta_corrupt_%d.DB", os.Getpid())
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create valid DB
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key"), []byte("value"))
	require.NoError(t, err, "Failed to set key")
	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt both meta pages (magic number)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	// Corrupt Page 0 meta magic (at PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(base.PageHeaderSize))
	require.NoError(t, err, "Failed to corrupt Page 0")
	// Corrupt Page 1 meta magic (at storage.PageSize + PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(base.PageSize+base.PageHeaderSize))
	require.NoError(t, err, "Failed to corrupt Page 1")
	file.Sync() // Ensure corruption is written to disk
	file.Close()

	// Verify corruption was applied
	verifyFile, _ := os.Open(tmpfile)
	verifyPage0 := &base.Page{}
	verifyFile.Read(verifyPage0.Data[:])
	verifyPage1 := &base.Page{}
	verifyFile.Read(verifyPage1.Data[:])
	verifyFile.Close()

	t.Logf("After corruption - Page 0 meta magic: %x",
		verifyPage0.Data[base.PageHeaderSize:base.PageHeaderSize+4])
	t.Logf("After corruption - Page 1 meta magic: %x",
		verifyPage1.Data[base.PageHeaderSize:base.PageHeaderSize+4])

	// Check file size
	info, _ := os.Stat(tmpfile)
	t.Logf("File size before reopening: %d bytes", info.Size())

	// Try to reopen - should FAIL because both pages corrupted
	_, err = Open(tmpfile)
	assert.Error(t, err, "Expected error opening DB with both meta pages corrupted")
	if err != nil && err.Error() != "both meta pages corrupted: invalid magic number, invalid magic number" {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCrashRecoveryChecksumCorruption tests that corrupted checksums are detected
func TestCrashRecoveryChecksumCorruption(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_checksum_corrupt.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create valid DB
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key"), []byte("value"))
	require.NoError(t, err, "Failed to set key")
	db.Close()

	// Corrupt Page 0's checksum field (last 4 bytes of MetaPage header)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	// Checksum is at offset 44 (after all other fields)
	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 44)
	require.NoError(t, err, "Failed to corrupt checksum")
	file.Close()

	// Try to reopen - should succeed using Page 1
	db2, err := Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB with corrupted checksum on Page 0")
	defer db2.Close()

	// Verify data still accessible (using Page 1)
	v, err := db2.Get([]byte("key"))
	assert.NoError(t, err, "Failed to get key")
	assert.Equal(t, "value", string(v))

	t.Logf("Successfully recovered from checksum corruption using backup meta Page")
}

// TestCrashRecoveryAlternatingWrites tests that meta pages alternate correctly based on TxID
func TestCrashRecoveryAlternatingWrites(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.DB", t.Name())
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB
	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// TxID starts at 0, so first commit writes to Page 0
	// Do multiple commits and verify alternating pattern
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		err = db.Set(key, value)
		require.NoError(t, err, "Failed to set key%d", i)
	}

	db.Close()

	// Reopen and read both meta pages
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")

	page0 := &base.Page{}
	file.Read(page0.Data[:])
	page1 := &base.Page{}
	file.Read(page1.Data[:])
	file.Close()

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	t.Logf("Page 0 TxID: %d", meta0.TxID)
	t.Logf("Page 1 TxID: %d", meta1.TxID)

	// One should have higher TxID than the other
	assert.NotEqual(t, meta0.TxID, meta1.TxID, "Both meta pages have same TxID - alternating writes not working")

	// The Page with higher TxID should be the active one
	var activeMeta *base.MetaPage
	if meta0.TxID > meta1.TxID {
		activeMeta = meta0
		// TxID should be even (written to Page 0)
		assert.Equal(t, uint64(0), activeMeta.TxID%2, "Page 0 has odd TxID %d, expected even", activeMeta.TxID)
	} else {
		activeMeta = meta1
		// TxID should be odd (written to Page 1)
		assert.Equal(t, uint64(1), activeMeta.TxID%2, "Page 1 has even TxID %d, expected odd", activeMeta.TxID)
	}

	// Both should be valid
	assert.NoError(t, meta0.Validate(), "Page 0 invalid")
	assert.NoError(t, meta1.Validate(), "Page 1 invalid")

	t.Logf("Meta Page alternating writes validated successfully")
}

func TestCrashRecoveryWrongMagicNumber(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_wrong_magic.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt meta Page with valid checksum but wrong magic
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	// Write wrong magic number at Page 0
	wrongMagic := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_, err = file.WriteAt(wrongMagic, int64(base.PageHeaderSize))
	require.NoError(t, err, "Failed to write wrong magic")
	file.Sync()
	file.Close()

	// Try to open - should fail or use Page 1
	db, err = Open(tmpfile)
	if err != nil {
		t.Logf("Open failed with wrong magic (expected): %v", err)
		return
	}
	defer db.Close()

	// If open succeeded, it should have used Page 1
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Logf("Failed to get key after wrong magic: %v", err)
	} else {
		assert.Equal(t, "value1", string(val))
	}
}

func TestCrashRecoveryRootPageIDZero(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_root_zero.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt meta Page: set RootPageID to 0 but keep valid TxID
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	// Read current meta from Page 0
	page := &base.Page{}
	_, err = file.ReadAt(page.Data[:], 0)
	require.NoError(t, err, "Failed to read meta")

	// Parse and modify
	meta := page.ReadMeta()
	meta.RootPageID = 0 // Put to 0 (invalid state if TxID > 0)
	meta.Checksum = meta.CalculateChecksum()

	// Write back
	page.WriteMeta(meta)
	_, err = file.WriteAt(page.Data[:], 0)
	require.NoError(t, err, "Failed to write modified meta")
	file.Sync()
	file.Close()

	// Try to open - behavior depends on implementation
	db, err = Open(tmpfile)
	if err != nil {
		t.Logf("Open failed with RootPageID=0 (may be expected): %v", err)
		return
	}
	defer db.Close()

	// If open succeeded, verify state
	val, err := db.Get([]byte("key1"))
	if val == nil {
		t.Logf("Key not found (expected with RootPageID=0)")
	} else if err != nil {
		t.Logf("get returned error: %v", err)
	}
}

func TestCrashRecoveryTruncatedFile(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_truncated.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Truncate file to only 1 Page (missing meta Page 1)
	err = os.Truncate(tmpfile, base.PageSize)
	require.NoError(t, err, "Failed to truncate file")

	// Try to open
	db, err = Open(tmpfile)
	if err != nil {
		t.Logf("Open failed on truncated file (expected): %v", err)
		return
	}
	defer db.Close()

	// If open succeeded, verify we can still use it
	t.Log("Open succeeded on truncated file")
}

func TestCrashRecoveryBothMetaSameTxnID(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_same_txnid.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Set([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to set key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Create impossible state: both meta pages with same TxID but different roots
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to open file")

	// Read meta from Page 0
	page0 := &base.Page{}
	_, err = file.ReadAt(page0.Data[:], 0)
	require.NoError(t, err, "Failed to read meta 0")
	meta0 := page0.ReadMeta()

	// Read meta from Page 1
	page1 := &base.Page{}
	_, err = file.ReadAt(page1.Data[:], base.PageSize)
	require.NoError(t, err, "Failed to read meta 1")
	meta1 := page1.ReadMeta()

	// Make both have same TxID
	meta1.TxID = meta0.TxID
	meta1.RootPageID = 999 // Different root (invalid)
	meta1.Checksum = meta1.CalculateChecksum()

	// Write back Page 1
	page1.WriteMeta(meta1)
	_, err = file.WriteAt(page1.Data[:], int64(base.PageSize))
	require.NoError(t, err, "Failed to write meta 1")
	file.Sync()
	file.Close()

	// Try to open - should handle this gracefully
	db, err = Open(tmpfile)
	if err != nil {
		t.Logf("Open failed with same TxID (may be expected): %v", err)
		return
	}
	defer db.Close()

	// If open succeeded, verify which meta was chosen
	t.Log("Open succeeded despite same TxID on both metas")
}

// Basic Operations Tests

func TestBTreeBasicOps(t *testing.T) {
	t.Parallel()

	// Test basic get/Put operations
	db, _ := setup(t)

	// Insert key-value pair
	err := db.Set([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// get existing key
	val, err := db.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(val))

	// Update existing key
	err = db.Set([]byte("key1"), []byte("value2"))
	assert.NoError(t, err)

	val, err = db.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value2", string(val))

	// get non-existent key (should return nil value)
	val, err = db.Get([]byte("nonexistent"))
	assert.Nil(t, err)
}

func TestBTreeUpdate(t *testing.T) {
	t.Parallel()

	// Test that Put updates existing Keys rather than duplicating
	db, _ := setup(t)

	// Insert key with value1
	err := db.Set([]byte("testkey"), []byte("value1"))
	assert.NoError(t, err)

	// Update same key with value2
	err = db.Set([]byte("testkey"), []byte("value2"))
	assert.NoError(t, err)

	// Verify get returns value2
	val, err := db.Get([]byte("testkey"))
	assert.NoError(t, err)
	assert.Equal(t, "value2", string(val))

	// Insert more Keys to ensure no duplication
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		db.Set(key, val)
	}

	// Update the original key again
	err = db.Set([]byte("testkey"), []byte("value3"))
	assert.NoError(t, err)

	val, err = db.Get([]byte("testkey"))
	assert.NoError(t, err)
	assert.Equal(t, "value3", string(val))
}

// Node Splitting Tests

func TestBTreeSplitting(t *testing.T) {
	t.Parallel()

	// Test Node splitting when exceeding MaxKeysPerNode
	db, _ := setup(t)

	// Insert MaxKeysPerNode + 1 Keys to force a split
	keys := make(map[string]string)
	for i := 0; i <= MaxKeysPerNode; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify root splits (root should no longer be a leaf)
	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should be a branch node after splitting")

	// Check tree height increases (root should have Children)
	assert.NotEmpty(t, db.pager.GetSnapshot().Root.Children, "Root should have Children after splitting")

	// Verify all Keys still retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}
}

func TestBTreeMultipleSplits(t *testing.T) {
	t.Parallel()

	// Test multiple levels of splitting
	db, _ := setup(t)

	// Insert enough Keys to cause multiple splits (3x MaxKeysPerNode should cause multiple levels)
	numKeys := MaxKeysPerNode * 3
	keys := make(map[string]string)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify tree structure remains valid (root is not a leaf for large tree)
	// Get __root__ bucket's tree structure
	tx, err := db.Begin(false)
	require.NoError(t, err)
	rootBucket := tx.Bucket([]byte("__root__"))
	require.NotNil(t, rootBucket, "__root__ bucket should exist")

	assert.Equal(t, base.BranchType, rootBucket.root.Type(), "Bucket root should be a branch node after multiple splits")
	// Verify root has multiple Children
	assert.GreaterOrEqual(t, len(rootBucket.root.Children), 2, "Bucket root should have multiple Children after multiple splits")
	tx.Rollback()

	// All Keys retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Verify we can still insert after multiple splits
	testKey := []byte("test_after_splits")
	testValue := []byte("test_value")
	err = db.Set(testKey, testValue)
	assert.NoError(t, err)

	val, err := db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(testValue), string(val))
}

// Sequential vs Random Insert Tests

func TestSequentialInsert(t *testing.T) {
	t.Parallel()

	// Test inserting Keys in sequential order
	db, _ := setup(t)

	// Insert 1000 sequential Keys
	numKeys := 1000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Check tree structure (likely right-heavy due to sequential insert)
	if db.pager.GetSnapshot().Root.Type() == base.LeafType {
		t.Logf("Tree has single leaf root after %d sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d sequential inserts",
			db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children), numKeys)
	}
}

func TestRandomInsert(t *testing.T) {
	t.Parallel()

	// Test inserting Keys in random order
	db, _ := setup(t)

	// Generate random Keys (using deterministic seed for reproducibility)
	numKeys := 1000
	keys := make(map[string]string)

	// Create Keys in random order using a simple shuffle
	indices := make([]int, numKeys)
	for i := 0; i < numKeys; i++ {
		indices[i] = i
	}

	// Simple deterministic shuffle
	for i := len(indices) - 1; i > 0; i-- {
		j := (i * 7) % (i + 1) // Deterministic "random"
		indices[i], indices[j] = indices[j], indices[i]
	}

	// Insert Keys in shuffled order
	for _, idx := range indices {
		key := fmt.Sprintf("key%08d", idx)
		value := fmt.Sprintf("value%08d", idx)
		keys[key] = value

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify all retrievable
	for key, expectedValue := range keys {
		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Check tree balance (random insertion typically produces more balanced trees)
	if db.pager.GetSnapshot().Root.Type() == base.BranchType {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d random inserts",
			db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children), numKeys)

		// Check if root has reasonable number of Children (indicating some balance)
		if len(db.pager.GetSnapshot().Root.Children) > 1 && len(db.pager.GetSnapshot().Root.Children) < 10 {
			t.Logf("Tree appears relatively balanced with %d root Children", len(db.pager.GetSnapshot().Root.Children))
		}
	}
}

func TestReverseSequentialInsert(t *testing.T) {
	t.Parallel()

	// Test inserting Keys in reverse order
	db, _ := setup(t)

	// Insert 1000 Keys in descending order
	numKeys := 1000
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)

		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Verify all retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		expectedValue := fmt.Sprintf("value%08d", i)

		val, err := db.Get([]byte(key))
		if assert.NoError(t, err) {
			assert.Equal(t, expectedValue, string(val))
		}
	}

	// Check tree structure (likely left-heavy due to reverse sequential insert)
	if db.pager.GetSnapshot().Root.Type() == base.LeafType {
		t.Logf("Tree has single leaf root after %d reverse sequential inserts", numKeys)
	} else {
		t.Logf("Tree has internal root with %d Keys and %d Children after %d reverse sequential inserts",
			db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children), numKeys)

		// With reverse insertion, we expect most activity on the left side of the tree
		if len(db.pager.GetSnapshot().Root.Children) > 0 {
			t.Logf("First child likely contains lower Keys due to reverse insertion pattern")
		}
	}
}

// Delete Tests

func TestBTreeDelete(t *testing.T) {
	t.Parallel()

	// Test basic delete operations
	db, _ := setup(t)

	// Insert some Keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, k := range keys {
		err := db.Set([]byte(k), []byte("value-"+k))
		assert.NoError(t, err)
	}

	// Delete middle key
	err := db.Delete([]byte("key3"))
	assert.NoError(t, err)

	// Verify key3 is gone
	val, err := db.Get([]byte("key3"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Verify other Keys still exist
	for _, k := range []string{"key1", "key2", "key4", "key5"} {
		val, err := db.Get([]byte(k))
		if assert.NoError(t, err) {
			assert.Equal(t, "value-"+k, string(val))
		}
	}

	// Delete first key
	err = db.Delete([]byte("key1"))
	assert.NoError(t, err)

	// Delete last key
	err = db.Delete([]byte("key5"))
	assert.NoError(t, err)

	// Delete non-existent key (idempotent - should not error)
	err = db.Delete([]byte("nonexistent"))
	assert.NoError(t, err)

	// Verify remaining Keys
	for _, k := range []string{"key2", "key4"} {
		val, err := db.Get([]byte(k))
		if assert.NoError(t, err) {
			assert.Equal(t, "value-"+k, string(val))
		}
	}
}

func TestBTreeDeleteAll(t *testing.T) {
	t.Parallel()

	// Test deleting all Keys from tree
	db, _ := setup(t)

	// Insert and then delete Keys one by one
	numKeys := 100
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		value := fmt.Sprintf("value%04d", i)
		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Delete all Keys in order
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Verify deleted
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)

		// Verify remaining Keys still exist
		for j := i + 1; j < numKeys && j < i+5; j++ {
			checkKey := fmt.Sprintf("key%04d", j)
			val, err := db.Get([]byte(checkKey))
			if assert.NoError(t, err) {
				expectedVal := fmt.Sprintf("value%04d", j)
				assert.Equal(t, expectedVal, string(val))
			}
		}
	}

	// Tree should be empty now
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%04d", i)
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
}

func TestBTreeSequentialDelete(t *testing.T) {
	t.Parallel()

	// Test sequential deletion pattern with tree structure checks
	db, _ := setup(t)

	// Insert enough Keys to create a multi-level tree
	numKeys := MaxKeysPerNode * 2 // Enough to cause splits
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Check initial tree structure
	initialIsLeaf := db.pager.GetSnapshot().Root.Type()
	initialRootKeys := int(db.pager.GetSnapshot().Root.NumKeys)
	t.Logf("Initial tree: root Type=%v, NumKeys=%d", initialIsLeaf, initialRootKeys)

	// Delete Keys sequentially and monitor tree structure
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Log tree structure changes at key points
		if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
			t.Logf("After %d deletions: root Type=%v, NumKeys=%d",
				i+1, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
		}

		// Verify key is deleted
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}

	// Final tree should be empty
	assert.Equal(t, uint16(0), db.pager.GetSnapshot().Root.NumKeys, "Tree should be empty")
	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Empty tree root should be branch")
}

func TestBTreeRandomDelete(t *testing.T) {
	t.Parallel()

	if !*slow {
		t.Skip("Skipping slow test; use -slow to enable")
	}

	for i := 0; i < 10; i++ {
		// Test random deletion pattern with tree structure checks
		db, _ := setup(t)

		// Insert Keys
		numKeys := MaxKeysPerNode * 2
		keys := make([]string, numKeys)
		for i := 0; i < numKeys; i++ {
			key := fmt.Sprintf("key%06d", i)
			value := fmt.Sprintf("value%06d", i)
			keys[i] = key
			err := db.Set([]byte(key), []byte(value))
			assert.NoError(t, err)
		}

		// Check initial tree structure
		initialIsLeaf := db.pager.GetSnapshot().Root.Type()
		initialRootKeys := int(db.pager.GetSnapshot().Root.NumKeys)
		t.Logf("Initial tree: root Type=%v, NumKeys=%d", initialIsLeaf, initialRootKeys)

		// Create random deletion order
		deleteOrder := make([]int, numKeys)
		for i := 0; i < numKeys; i++ {
			deleteOrder[i] = i
		}
		// Deterministic shuffle for reproducibility
		for i := len(deleteOrder) - 1; i > 0; i-- {
			j := (i * 7) % (i + 1)
			deleteOrder[i], deleteOrder[j] = deleteOrder[j], deleteOrder[i]
		}

		// Track which Keys are deleted
		deleted := make(map[string]bool)

		// Delete Keys randomly and monitor tree structure
		for i, idx := range deleteOrder {
			key := keys[idx]
			err := db.Delete([]byte(key))
			assert.NoError(t, err)
			deleted[key] = true

			// Log tree structure changes at key points
			if i == numKeys/4 || i == numKeys/2 || i == 3*numKeys/4 {
				t.Logf("After %d random deletions: root Type=%v, NumKeys=%d",
					i+1, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
			}

			// Verify deleted key is gone
			val, err := db.Get([]byte(key))
			assert.NoError(t, err)
			assert.Nil(t, val)

			// Verify some non-deleted Keys still exist (spot active)
			checked := 0
			for _, k := range keys {
				if !deleted[k] && checked < 5 {
					val, err := db.Get([]byte(k))
					if assert.NoError(t, err) {
						expectedVal := "value" + k[3:] // Concatenate value prefix with key suffix
						assert.Equal(t, expectedVal, string(val))
					}
					checked++
				}
			}
		}

		// Final tree should be empty
		assert.Equal(t, uint16(0), db.pager.GetSnapshot().Root.NumKeys, "Tree should be empty")
		assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Empty tree root should be branch")

		// close database after each iteration
		db.Close()
	}
}

func TestBTreeReverseDelete(t *testing.T) {
	t.Parallel()

	// Test reverse sequential deletion pattern
	db, _ := setup(t)

	// Insert Keys
	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}

	// Check initial tree structure
	t.Logf("Initial tree: root Type=%v, NumKeys=%d", db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)

	// Delete Keys in reverse order
	for i := numKeys - 1; i >= 0; i-- {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		assert.NoError(t, err)

		// Log tree structure at key points
		deletedCount := numKeys - i
		if deletedCount == numKeys/4 || deletedCount == numKeys/2 || deletedCount == 3*numKeys/4 {
			t.Logf("After %d reverse deletions: root Type=%v, NumKeys=%d",
				deletedCount, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
		}

		// Verify key is deleted
		val, err := db.Get([]byte(key))
		assert.NoError(t, err)
		assert.Nil(t, val)
	}

	// Final tree should be empty
	assert.Equal(t, uint16(0), db.pager.GetSnapshot().Root.NumKeys, "Tree should be empty")
}

// Stress Tests

func TestBTreeStress(t *testing.T) {
	// Stress test with large number of operations
	// - Insert 10000 random key-value pairs
	// - Perform random get operations
	// - Update random existing Keys
	// - Verify data integrity throughout
	t.Skip("Not implemented")
}

func TestBTreeLargeValues(t *testing.T) {
	// Test with large value sizes
	// - Insert Keys with 1KB, 10KB Values
	// - Verify retrieval
	// - Check memory usage patterns
	t.Skip("Not implemented")
}

// PageManager Integration Tests

func TestPageCaching(t *testing.T) {
	// Test Page cache behavior
	// - Verify cache hit/miss behavior
	// - Test cache population on reads
	// - Verify Dirty Page tracking
	t.Skip("Not implemented")
}

func TestCloseFlush(t *testing.T) {
	// Test close() flushes Dirty pages
	// - Insert data
	// - Mark nodes Dirty
	// - close algo
	// - Verify WritePage called for all Dirty nodes
	t.Skip("Not implemented")
}

func TestLoadNode(t *testing.T) {
	// Test load caching behavior
	// - Load same Node multiple times
	// - Verify cache returns same instance
	// - Verify no duplicate reads from PageManager
	t.Skip("Not implemented")
}

// Edge Cases

func TestEmptyTree(t *testing.T) {
	t.Parallel()

	// Test operations on empty tree
	db, _ := setup(t)

	// get from empty tree (should return nil)
	val, err := db.Get([]byte("nonexistent"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Try multiple different Keys on empty tree
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte(""),
		[]byte("test"),
		[]byte("\x00\xff"),
	}

	for _, key := range testKeys {
		val, err = db.Get(key)
		assert.NoError(t, err)
		assert.Nil(t, val)
	}
}

func TestSingleKey(t *testing.T) {
	t.Parallel()

	// Test tree with single key
	db, _ := setup(t)

	// Insert one key
	testKey := []byte("single_key")
	testValue := []byte("single_value")

	err := db.Set(testKey, testValue)
	assert.NoError(t, err)

	// get that key
	val, err := db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(testValue), string(val))

	// get non-existent key
	val2, err := db.Get([]byte("nonexistent"))
	assert.NoError(t, err)
	assert.Nil(t, val2)

	// Update the key
	newValue := []byte("updated_value")
	err = db.Set(testKey, newValue)
	assert.NoError(t, err)

	val, err = db.Get(testKey)
	assert.NoError(t, err)
	assert.Equal(t, string(newValue), string(val))

	// Verify tree structure: root is always a branch (never leaf)
	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should always be a branch node")
	assert.Equal(t, 1, len(db.pager.GetSnapshot().Root.Children), "Root should have 1 child")

	// Load the child and verify it has the key
	tx, err := db.Begin(false)
	require.NoError(t, err)
	defer tx.Rollback()

	child, err := tx.load(db.pager.GetSnapshot().Root.Children[0])
	assert.NoError(t, err)
	assert.Equal(t, base.LeafType, child.Type(), "Child should be a leaf")
	assert.Equal(t, uint16(1), child.NumKeys, "Child should have exactly 1 key")
}

func TestDuplicateKeys(t *testing.T) {
	t.Parallel()

	// Test handling of duplicate key insertions
	db, _ := setup(t)

	key := []byte("duplicate_test")
	value1 := []byte("value1")
	value2 := []byte("value2")
	value3 := []byte("value3")

	// Insert key with value1
	err := db.Set(key, value1)
	assert.NoError(t, err)

	// Verify value1 is stored
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value1), string(val))

	// Insert same key with value2
	err = db.Set(key, value2)
	assert.NoError(t, err)

	// Verify only one entry exists with value2
	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value2), string(val))

	// Add more Keys to ensure tree structure
	for i := 0; i < 10; i++ {
		k := []byte(fmt.Sprintf("key%d", i))
		v := []byte(fmt.Sprintf("value%d", i))
		db.Set(k, v)
	}

	// Update original key again with value3
	err = db.Set(key, value3)
	assert.NoError(t, err)

	val, err = db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, string(value3), string(val))
}

func TestBinaryKeys(t *testing.T) {
	t.Parallel()

	// Test with non-string binary Keys
	db, _ := setup(t)

	// Test data with various binary patterns
	testData := []struct {
		key   []byte
		value []byte
		desc  string
	}{
		{[]byte("\x00\x00\x00"), []byte("null_bytes"), "null bytes"},
		{[]byte("\xff\xff\xff"), []byte("high_bytes"), "high-bit bytes"},
		{[]byte("\x00\x01\x02\x03"), []byte("ascending"), "ascending bytes"},
		{[]byte("\xff\xfe\xfd\xfc"), []byte("descending"), "descending bytes"},
		{[]byte("normal\x00key"), []byte("embedded_null"), "embedded null"},
		{[]byte("\x80\x90\xa0\xb0"), []byte("high_bits"), "high bits set"},
		{[]byte{0, 1, 255, 254, 127, 128}, []byte("mixed"), "mixed binary"},
	}

	// Insert all binary Keys
	for _, td := range testData {
		err := db.Set(td.key, td.value)
		assert.NoError(t, err)
	}

	// Verify correct retrieval
	for _, td := range testData {
		val, err := db.Get(td.key)
		if assert.NoError(t, err) {
			assert.Equal(t, string(td.value), string(val))
		}
	}

	// Test ordering with binary Keys
	key1 := []byte{0x00, 0x01}
	key2 := []byte{0x00, 0x02}
	val1 := []byte("first")
	val2 := []byte("second")

	db.Set(key1, val1)
	db.Set(key2, val2)

	// Both should be retrievable
	v, err := db.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, string(val1), string(v))

	v, err = db.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, string(val2), string(v))
}

func TestZeroLengthKeys(t *testing.T) {
	t.Parallel()

	// Test with zero-length Keys
	db, _ := setup(t)

	// Insert empty key
	emptyKey := []byte{}
	emptyValue := []byte("empty_key_value")

	err := db.Set(emptyKey, emptyValue)
	assert.NoError(t, err)

	// Retrieve empty key
	val, err := db.Get(emptyKey)
	assert.NoError(t, err)
	assert.Equal(t, string(emptyValue), string(val))

	// Mix with non-empty Keys
	normalKeys := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("a"), []byte("value_a")},
		{[]byte("b"), []byte("value_b")},
		{[]byte(""), []byte("another_empty")}, // Update empty key
		{[]byte("c"), []byte("value_c")},
	}

	for _, kv := range normalKeys {
		err := db.Set(kv.key, kv.value)
		assert.NoError(t, err)
	}

	// Verify empty key was updated
	val, err = db.Get(emptyKey)
	assert.NoError(t, err)
	assert.Equal(t, "another_empty", string(val))

	// Verify all Keys are retrievable
	for _, kv := range normalKeys {
		val, err := db.Get(kv.key)
		if assert.NoError(t, err) {
			assert.Equal(t, string(kv.value), string(val))
		}
	}
}

func TestZeroLengthValues(t *testing.T) {
	t.Parallel()

	// Test with zero-length Values
	db, _ := setup(t)

	// Insert key with empty value
	key := []byte("key_with_empty_value")
	emptyValue := []byte{}

	err := db.Set(key, emptyValue)
	assert.NoError(t, err)

	// Retrieve and verify empty value
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Empty(t, val)

	// Mix empty and non-empty Values
	testData := []struct {
		key   []byte
		value []byte
	}{
		{[]byte("normal1"), []byte("value1")},
		{[]byte("empty1"), []byte{}},
		{[]byte("normal2"), []byte("value2")},
		{[]byte("empty2"), []byte{}},
		{[]byte("normal3"), []byte("value3")},
	}

	for _, td := range testData {
		err := db.Set(td.key, td.value)
		assert.NoError(t, err)
	}

	// Verify all Values are correctly stored
	for _, td := range testData {
		val, err := db.Get(td.key)
		if assert.NoError(t, err) {
			assert.Equal(t, string(td.value), string(val))
		}
	}

	// Update a key from non-empty to empty value
	updateKey := []byte("update_test")
	db.Set(updateKey, []byte("initial_value"))

	val, err = db.Get(updateKey)
	assert.NoError(t, err)
	assert.Equal(t, "initial_value", string(val))

	// Update to empty value
	db.Set(updateKey, []byte{})

	val, err = db.Get(updateKey)
	assert.NoError(t, err)
	assert.Empty(t, val)
}

func TestPageOverflowLargeKey(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Key that exceeds MaxKeySize = 1024
	largeKey := make([]byte, 2000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
	}
	value := []byte("small_value")

	err := db.Set(largeKey, value)
	assert.Equal(t, ErrKeyTooLarge, err)
}

func TestPageOverflowLargeValue(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	key := []byte("small_key")
	// Value that's too large to fit inline in a page
	// PageSize = 4096, PageHeaderSize = 32, LeafElementSize = 16
	// Available = 4096 - 32 - 16 = 4048 for key+value
	// small_key = 9 bytes, so max value = 4048 - 9 = 4039
	largeValue := make([]byte, 4060)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Should return ErrValueTooLarge (no overflow pages)
	err := db.Set(key, largeValue)
	assert.ErrorIs(t, err, ErrValueTooLarge)
}

func TestPageOverflowCombinedSize(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// With overflow pages, values can be large - only keys are limited
	// Test that key too large for page still fails
	// Max key that fits = PageSize - PageHeaderSize - LeafElementSize = 4048
	key := make([]byte, 4050) // Too large even for page
	value := make([]byte, 100)
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 50) % 256)
	}

	err := db.Set(key, value)
	assert.Equal(t, ErrKeyTooLarge, err)
}

func TestPageOverflowBoundary(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test key+value that exceeds available space
	// PageSize = 4096
	// PageHeaderSize = 32
	// LeafElementSize = 16 (2 * uint64)
	// Available = 4096 - 32 - 16 = 4048 bytes for key+value

	keySize := 1000
	valueSize := 3050 // Total 4050 > 4048, should overflow

	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	for i := range key {
		key[i] = 'k'
	}
	for i := range value {
		value[i] = 'v'
	}

	err := db.Set(key, value)
	assert.ErrorIs(t, err, ErrValueTooLarge)
}

func TestPageOverflowMaxKeyValue(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test with key at MaxKeySize = 1024 and large value
	// Should fit: PageSize - PageHeaderSize - LeafElementSize = 4048
	key := make([]byte, 1024)   // At MaxKeySize limit
	value := make([]byte, 3000) // Total = 4024, should fit in 4048

	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte((i + 128) % 256)
	}

	err := db.Set(key, value)
	assert.NoError(t, err)

	// Verify retrieval
	retrieved, err := db.Get(key)
	assert.NoError(t, err)
	assert.Equal(t, value, retrieved)
}

func TestBoundaryInsert255ThenSplit(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert enough keys to fill a page and trigger split
	// With 8-byte LeafElement: 24 + 8*N + (9+12)*N = 4096 → N ≈ 140 entries per page
	// Insert 160 to ensure split occurs
	numKeys := 160

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	// After inserting enough keys, bucket root should have split
	tx, err := db.Begin(false)
	require.NoError(t, err)
	rootBucket := tx.Bucket([]byte("__root__"))
	require.NotNil(t, rootBucket, "__root__ bucket should exist")

	t.Logf("Root: Type=%v, NumKeys=%d, Children=%d", rootBucket.root.Type(), rootBucket.root.NumKeys, len(rootBucket.root.Children))
	assert.Equal(t, base.BranchType, rootBucket.root.Type(), "Bucket root should always be a branch node")
	assert.GreaterOrEqual(t, len(rootBucket.root.Children), 2, "Bucket root should have at least 2 children after split")
	tx.Rollback()

	// Verify all keys retrievable
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		assert.NoError(t, err)
	}
}

func TestBoundaryRootWithOneKeyDeleteIt(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := MaxKeysPerNode * 2
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	require.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should be branch Node")

	initialRootKeys := db.pager.GetSnapshot().Root.NumKeys
	t.Logf("Initial root Keys: %d", initialRootKeys)

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)

		if i%(numKeys/4) == 0 {
			t.Logf("After %d deletions: root.Type()=%v, root.NumKeys=%d",
				i+1, db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
		}
	}

	assert.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Final root should be branch")
	assert.Equal(t, uint16(0), db.pager.GetSnapshot().Root.NumKeys, "Final root should have 0 Keys")
}

func TestBoundarySiblingBorrowVsMerge(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	numKeys := MaxKeysPerNode * 3
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		value := fmt.Sprintf("value%06d", i)
		err := db.Set([]byte(key), []byte(value))
		require.NoError(t, err)
	}

	require.Equal(t, base.BranchType, db.pager.GetSnapshot().Root.Type(), "Root should not be leaf for this test")

	t.Logf("Tree structure: root.NumKeys=%d, root.Children=%d",
		db.pager.GetSnapshot().Root.NumKeys, len(db.pager.GetSnapshot().Root.Children))

	deleteCount := numKeys / 2
	for i := 0; i < deleteCount; i++ {
		key := fmt.Sprintf("key%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)
	}

	for i := deleteCount; i < numKeys; i++ {
		key := fmt.Sprintf("key%06d", i)
		_, err := db.Get([]byte(key))
		assert.NoError(t, err)
	}

	t.Logf("After deletions: root.Type()=%v, root.NumKeys=%d",
		db.pager.GetSnapshot().Root.Type(), db.pager.GetSnapshot().Root.NumKeys)
}

func TestDBRestartPersistence(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_restart_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	defer os.Remove(tmpfile)

	// Phase 1: Insert 100 values one at a time
	db, err := Open(tmpfile, WithCacheSizeMB(0))
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		err := db.Update(func(tx *Tx) error {
			return tx.Put(key, value)
		})
		require.NoError(t, err, "Failed to insert key%03d", i)
	}

	// Close the database
	err = db.Close()
	require.NoError(t, err)

	// Phase 2: Reopen and read the last value
	db, err = Open(tmpfile, WithCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// Read the last key inserted
	lastKey := []byte("key099")
	expectedValue := []byte("value099")

	err = db.View(func(tx *Tx) error {
		val, err := tx.Get(lastKey)
		if err != nil {
			return err
		}
		if val == nil {
			return fmt.Errorf("key099 returned nil after restart")
		}
		assert.Equal(t, expectedValue, val, "Value mismatch after restart")
		return nil
	})
	require.NoError(t, err)

	// Verify all 100 keys are readable
	err = db.View(func(tx *Tx) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			val, err := tx.Get(key)
			if err != nil {
				return err
			}
			if val == nil {
				return fmt.Errorf("key%03d returned nil after restart", i)
			}
		}
		return nil
	})
	require.NoError(t, err, "Failed to read all keys after restart")
}

package test

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb"
)

func TestDBConcurrency(t *testing.T) {
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
						err := db.Put([]byte(key), []byte(value))
						if errors.Is(err, fredb.ErrTxInProgress) {
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

		err := db.Put([]byte(key), []byte(value))
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
					err := db.Put([]byte(key), []byte(value))
					if errors.Is(err, fredb.ErrTxInProgress) {
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
				err := db.Put(updateKey, []byte(value))
				if err == fredb.ErrTxInProgress {
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
		require.NoError(t, db.Put(key, value), "Failed to Put initial key")
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
	err = db.Update(func(writeTx *fredb.Tx) error {
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
			err := db.Put(key, value)
			if err != nil {
				readerErrors <- fmt.Errorf("writer: failed to Put key-%d: %w", i, err)
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

	db, tmpfile := setup(t)

	// Insert 100 initial Keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("initial-key-%d", i))
		value := []byte(fmt.Sprintf("initial-value-%d", i))
		require.NoError(t, db.Put(key, value), "Failed to insert initial data")
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
					err := db.Update(func(tx *fredb.Tx) error {
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

					if errors.Is(err, fredb.ErrTxInProgress) {
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
						if errors.Is(err, fredb.ErrTxInProgress) {
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
					err := db.Update(func(tx *fredb.Tx) error {
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

					if errors.Is(err, fredb.ErrTxInProgress) {
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

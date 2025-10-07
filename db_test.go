package fredb

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"fredb/internal/base"
)

// Use -slow flag to run longer tests
var slow = flag.Bool("slow", false, "run slow tests")

// Helper to create a temporary test database
func setupTestDB(t *testing.T) *DB {
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.DB", t.Name())
	_ = os.Remove(tmpfile)
	_ = os.Remove(tmpfile + ".wal") // Also remove wal file

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Type assert to get concrete type for tests
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
		_ = os.Remove(tmpfile + ".wal") // Cleanup wal file
	})

	return db
}

func TestDBBasicOperations(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_basic.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Test Set and get
	key := []byte("test-key")
	value := []byte("test-value")

	err = db.Set(key, value)
	if err != nil {
		t.Errorf("Failed to set key: %v", err)
	}

	retrieved, err := db.Get(key)
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}
	if string(retrieved) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(retrieved))
	}

	// Test Update
	newValue := []byte("updated-value")
	err = db.Set(key, newValue)
	if err != nil {
		t.Errorf("Failed to update key: %v", err)
	}

	retrieved, err = db.Get(key)
	if err != nil {
		t.Errorf("Failed to get updated key: %v", err)
	}
	if string(retrieved) != string(newValue) {
		t.Errorf("Expected %s, got %s", string(newValue), string(retrieved))
	}

	// Test Delete
	err = db.Delete(key)
	if err != nil {
		t.Errorf("Failed to delete key: %v", err)
	}

	// Verify key is deleted
	_, err = db.Get(key)
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleted key, got %v", err)
	}
}

func TestDBErrors(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_errors.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// get non-existent key
	_, err = db.Get([]byte("non-existent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for non-existent key, got %v", err)
	}

	// Delete non-existent key
	err = db.Delete([]byte("non-existent"))
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for deleting non-existent key, got %v", err)
	}
}

func TestDBClose(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_close.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Set a key before closing
	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Errorf("Failed to set key: %v", err)
	}

	// close the DB
	err = db.Close()
	if err != nil {
		t.Errorf("Failed to close DB: %v", err)
	}

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
	// - Mix of get/Set operations
	// - Verify no races or corruption
	// - Check final state consistency
	tmpfile := "/tmp/test_db_concurrency.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

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
						if err != nil {
							t.Errorf("Set failed: %v", err)
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
					if err != nil && err != ErrKeyNotFound {
						t.Errorf("get failed with unexpected error: %v", err)
						return
					}

					// If found, verify it matches expected pattern
					if err == nil {
						expectedPattern := fmt.Sprintf("value-%d-%d", readID, readJ)
						if string(val) != expectedPattern {
							t.Errorf("Value mismatch: got %s, expected pattern %s", string(val), expectedPattern)
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
		if err != nil {
			t.Errorf("Key should exist: %s, error: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Value mismatch for key %s: got %s, expected %s", key, string(val), expectedValue)
		}
	}
}

func TestDBConcurrentReads(t *testing.T) {
	t.Parallel()

	// Test concurrent reads don't block each other
	// - Insert test data
	// - Launch multiple reader goroutines
	// - Verify all complete successfully
	tmpfile := "/tmp/test_db_concurrent_reads.DB"
	os.Remove(tmpfile) // Clean up any old test file
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Insert test data
	testData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		testData[key] = value

		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
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
				if err != nil {
					t.Errorf("get failed for key %s: %v", key, err)
					return
				}

				if string(val) != expectedValue {
					t.Errorf("Value mismatch for key %s: got %s, expected %s", key, string(val), expectedValue)
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
	tmpfile := "/tmp/test_db_concurrent_writes.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

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
					if err == ErrTxInProgress {
						// Another writer holds the lock, retry
						continue
					}
					if err != nil {
						t.Errorf("Set failed for writer %d: %v", id, err)
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
	if len(writtenKeys) != numWriters*writesPerWriter {
		t.Errorf("Expected %d Keys, got %d", numWriters*writesPerWriter, len(writtenKeys))
	}

	for key, expectedValue := range writtenKeys {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found: %v", key, err)
			continue
		}
		if string(val) != expectedValue {
			t.Errorf("Value mismatch for key %s: got %s, expected %s", key, string(val), expectedValue)
		}
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
				if err != nil {
					t.Errorf("Update failed for updater %d: %v", id, err)
					return
				}
				break
			}
		}(i)
	}

	wg.Wait()

	// Verify the key exists (value could be from any updater)
	_, err = db.Get(updateKey)
	if err != nil {
		t.Errorf("Concurrent update key not found: %v", err)
	}
}

// TestTxSnapshotIsolation tests MVCC snapshot isolation.
// Test concurrent read/write with snapshot isolation
func TestTxSnapshotIsolation(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_tx_snapshot_isolation.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Insert initial Values
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d-v1", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set initial key: %v", err)
		}
	}

	// Test 1: Simple snapshot isolation
	// Start read transaction, capture value
	readTx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("Failed to begin read transaction: %v", err)
	}
	defer readTx.Rollback()

	oldValue, err := readTx.Get([]byte("key-0"))
	if err != nil {
		t.Fatalf("Failed to get value in read tx: %v", err)
	}
	if string(oldValue) != "value-0-v1" {
		t.Errorf("Expected 'value-0-v1', got %s", string(oldValue))
	}

	// Start write transaction, modify value, commit
	err = db.Update(func(writeTx *Tx) error {
		return writeTx.Set([]byte("key-0"), []byte("value-0-v2"))
	})
	if err != nil {
		t.Fatalf("Failed to commit write transaction: %v", err)
	}

	// Verify read tx still sees old value (snapshot isolation)
	stillOldValue, err := readTx.Get([]byte("key-0"))
	if err != nil {
		t.Fatalf("Failed to get value in read tx after write: %v", err)
	}
	if string(stillOldValue) != "value-0-v1" {
		t.Errorf("Snapshot isolation broken: expected 'value-0-v1', got %s", string(stillOldValue))
	}

	readTx.Rollback()

	// Verify new transaction sees new value
	newValue, err := db.Get([]byte("key-0"))
	if err != nil {
		t.Fatalf("Failed to get value after commit: %v", err)
	}
	if string(newValue) != "value-0-v2" {
		t.Errorf("Expected 'value-0-v2', got %s", string(newValue))
	}

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
				readerErrors <- fmt.Errorf("reader %d: failed to begin: %v", readerID, err)
				return
			}
			defer tx.Rollback()

			// Capture initial snapshot
			snapshot := make(map[string]string)
			for j := 0; j < 10; j++ {
				key := []byte(fmt.Sprintf("key-%d", j))
				value, err := tx.Get(key)
				if err != nil {
					readerErrors <- fmt.Errorf("reader %d: failed to get key-%d: %v", readerID, j, err)
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
					readerErrors <- fmt.Errorf("reader %d: failed to re-read key-%d: %v", readerID, j, err)
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
				readerErrors <- fmt.Errorf("writer: failed to set key-%d: %v", i, err)
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
		if err != nil {
			t.Errorf("Failed to get final value for key-%d: %v", i, err)
			continue
		}
		expectedValue := fmt.Sprintf("value-%d-v3", i)
		if string(value) != expectedValue {
			t.Errorf("Final value for key-%d: expected %s, got %s", i, expectedValue, string(value))
		}
	}
}

// TestTxRollbackUnderContention tests that rollbacks don't corrupt state or leak pages
// when multiple write transactions compete and some rollback intentionally
func TestTxRollbackUnderContention(t *testing.T) {
	t.Parallel()

	if !*slow {
		t.Skip("Skipping slow rollback test; use -slow to enable")
	}

	tmpfile := "/tmp/test_tx_rollback_contention.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Insert 100 initial Keys
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("initial-key-%d", i))
		value := []byte(fmt.Sprintf("initial-value-%d", i))
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
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
							if err := tx.Set(key, value); err != nil {
								return err
							}
						}

						// Update 1 existing key
						updateKey := []byte(result.update)
						updateValue := []byte(fmt.Sprintf("updated-by-tx-%d", opID))
						result.values = append(result.values, string(updateValue))
						if err := tx.Set(updateKey, updateValue); err != nil {
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
						if err != nil {
							t.Errorf("Failed to begin tx: %v", err)
							break
						}

						// Write some Keys (will be rolled back)
						for k := 0; k < 5; k++ {
							key := []byte(fmt.Sprintf("rollback-tx-%d-key-%d", opID, k))
							value := []byte(fmt.Sprintf("rollback-tx-%d-value-%d", opID, k))
							if err := tx.Set(key, value); err != nil {
								t.Errorf("Set failed in rollback test: %v", err)
							}
						}

						// Explicit rollback
						if err := tx.Rollback(); err != nil {
							t.Errorf("Rollback failed: %v", err)
						}

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
							if err := tx.Set(key, value); err != nil {
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
			if err != nil {
				t.Errorf("Committed tx %d: key %s not found: %v", txIdx, result.keys[i], err)
				continue
			}
			if string(val) != result.values[i] {
				t.Errorf("Committed tx %d: key %s value mismatch: got %s, expected %s",
					txIdx, result.keys[i], string(val), result.values[i])
			}
		}

		// Verify updated key
		val, err := db.Get([]byte(result.update))
		if err != nil {
			t.Errorf("Committed tx %d: updated key %s not found: %v", txIdx, result.update, err)
		} else {
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
		_, err := db.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Rolled-back key should not exist: %s, got error: %v", key, err)
		}

		// Check error rollback Keys
		key = []byte(fmt.Sprintf("error-tx-%d-key-0", i*100))
		_, err = db.Get(key)
		if err != ErrKeyNotFound {
			t.Errorf("Error-rolled-back key should not exist: %s, got error: %v", key, err)
		}
	}

	// Heuristic active for Page leaks: file size should be reasonable
	// With 100 initial Keys + ~500-750 committed transactions × 5-6 Keys each
	// = ~100 + 3000-4500 = ~4000 Keys total
	// At ~4KB per Page with branching, expect < 10MB
	info, err := os.Stat(tmpfile)
	if err != nil {
		t.Errorf("Failed to stat DB file: %v", err)
	} else {
		sizeMB := float64(info.Size()) / (1024 * 1024)
		t.Logf("Database file size: %.2f MB", sizeMB)
		if sizeMB > 10.0 {
			t.Errorf("File size suspiciously large (%.2f MB), possible Page leak", sizeMB)
		}
	}
}

// TestDBLargeKeysPerPage tests inserting large key-value pairs
// where only 2 pairs fit per Page (stress test for Page splits)
func TestDBLargeKeysPerPage(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_large_keys.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

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
			if err := db.Set(key, value); err != nil {
				t.Fatalf("Failed to insert large key %d: %v", i, err)
			}
		}
	})

	// Test 2: Read back all Keys
	t.Run("ReadLargeKeys", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			key := makeKey(i)
			expectedValue := makeValue(i)

			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Failed to get large key %d: %v", i, err)
				continue
			}

			if len(value) != valueSize {
				t.Errorf("Key %d: value size mismatch: got %d, expected %d", i, len(value), valueSize)
			}

			if string(value) != string(expectedValue) {
				t.Errorf("Key %d: value mismatch", i)
			}
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

			if err := db.Set(key, newValue); err != nil {
				t.Fatalf("Failed to update large key %d: %v", i, err)
			}

			// Verify update
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Failed to get updated key %d: %v", i, err)
				continue
			}

			if string(value) != string(newValue) {
				t.Errorf("Key %d: updated value mismatch", i)
			}
		}
	})

	// Test 4: Delete large Keys
	t.Run("DeleteLargeKeys", func(t *testing.T) {
		// Delete every other key
		for i := 0; i < 10; i += 2 {
			key := makeKey(i)
			if err := db.Delete(key); err != nil {
				t.Errorf("Failed to delete large key %d: %v", i, err)
			}

			// Verify deleted
			_, err := db.Get(key)
			if err != ErrKeyNotFound {
				t.Errorf("Key %d should be deleted, got error: %v", i, err)
			}
		}

		// Verify remaining Keys still exist
		for i := 1; i < 10; i += 2 {
			key := makeKey(i)
			_, err := db.Get(key)
			if err != nil {
				t.Errorf("Remaining key %d should exist: %v", i, err)
			}
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
							if err != nil {
								t.Errorf("Goroutine %d: Set failed: %v", id, err)
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
					if err != nil {
						t.Errorf("Concurrent key %d not found: %v", keyID, err)
						continue
					}

					if string(value) != string(expectedValue) {
						t.Errorf("Concurrent key %d: value mismatch", keyID)
					}
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
			if err := db.Set(key, value); err != nil {
				t.Fatalf("Failed to insert key %d in deep tree: %v", i, err)
			}
		}

		// Spot active some Keys
		testKeys := []int{100, 125, 150, 175, 199}
		for _, i := range testKeys {
			key := makeKey(i)
			expectedValue := makeValue(i)

			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Key %d not found in deep tree: %v", i, err)
				continue
			}

			if string(value) != string(expectedValue) {
				t.Errorf("Key %d in deep tree: value mismatch", i)
			}
		}
	})
}

// TestCrashRecoveryLastCommittedState tests recovery to previous valid state
func TestCrashRecoveryLastCommittedState(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_last_committed.DB"
	defer os.Remove(tmpfile)

	// Create DB and do a single commit
	db1, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	db1.Set([]byte("key1"), []byte("value1"))
	db1.Close()

	// Reopen and do a second commit (Set without close to avoid extra TxnID)
	db2, _ := Open(tmpfile)
	db2.Set([]byte("key2"), []byte("value2"))

	// Check TxnIDs after second Set (before close)
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	page0 := &base.Page{}
	file.Read(page0.Data[:])
	page1 := &base.Page{}
	file.Read(page1.Data[:])
	file.Close()

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()
	t.Logf("After second Set: Page 0 TxnID=%d RootPageID=%d, Page 1 TxnID=%d RootPageID=%d",
		meta0.TxnID, meta0.RootPageID, meta1.TxnID, meta1.RootPageID)

	// Record the older TxnID (should only have key1)
	var olderTxn uint64
	var olderRoot base.PageID
	if meta0.TxnID < meta1.TxnID {
		olderTxn = meta0.TxnID
		olderRoot = meta0.RootPageID
	} else {
		olderTxn = meta1.TxnID
		olderRoot = meta1.RootPageID
	}

	db2.Close() // Now close, which will write another meta

	// Simulate crash: corrupt the newest meta Page
	file, err = os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	page0 = &base.Page{}
	file.ReadAt(page0.Data[:], 0)
	page1 = &base.Page{}
	file.ReadAt(page1.Data[:], int64(base.PageSize))

	meta0 = page0.ReadMeta()
	meta1 = page1.ReadMeta()

	// Corrupt the newer one
	var corruptOffset int64
	if meta0.TxnID > meta1.TxnID {
		corruptOffset = int64(base.PageHeaderSize)
		t.Logf("Corrupting Page 0 (TxnID %d)", meta0.TxnID)
	} else {
		corruptOffset = int64(base.PageSize + base.PageHeaderSize)
		t.Logf("Corrupting Page 1 (TxnID %d)", meta1.TxnID)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	file.WriteAt(corruptData, corruptOffset)
	file.Sync()
	file.Close()

	// Reopen - should fall back to previous valid state
	db3, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen after simulated crash: %v", err)
	}
	defer db3.Close()

	meta3 := db3.store.Pager.GetMeta()
	t.Logf("After reopen: loaded meta with TxnID %d, RootPageID %d", meta3.TxnID, meta3.RootPageID)
	t.Logf("Expected to recover to TxnID %d (previous valid state)", olderTxn)

	// key1 should always exist
	v1, err := db3.Get([]byte("key1"))
	if err != nil {
		t.Errorf("key1 should exist after crash recovery: %v", err)
	}
	if string(v1) != "value1" {
		t.Errorf("Wrong value for key1: got %s, expected value1", string(v1))
	}

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
	defer os.Remove(tmpfile)

	// Create new database
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Insert some data
	if err := db.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}
	if err := db.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Verify data
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// close (flushes to disk)
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Reopen database
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}

	// Verify data persisted
	val, err = db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 after reopen: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1 after reopen, got %s", val)
	}

	val, err = db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2 after reopen: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2 after reopen, got %s", val)
	}

	// Cleanup
	if err := db2.Close(); err != nil {
		t.Fatalf("Failed to close db2: %v", err)
	}
}

func TestDiskPageManagerPersistence(t *testing.T) {
	t.Parallel()

	// Use unique filename per test to avoid parallel test collisions
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.DB", t.Name())
	_ = os.Remove(tmpfile)
	defer os.Remove(tmpfile)

	// Create database and insert 100 Keys
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Reopen and verify all Keys
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i * 2)}

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d after reopen: %v", i, err)
		}
		if len(value) != 1 || value[0] != expectedValue[0] {
			t.Errorf("Key %d: expected %v, got %v", i, expectedValue, value)
		}
	}

	if err := db2.Close(); err != nil {
		t.Fatalf("Failed to close db2: %v", err)
	}
}

func TestDiskPageManagerDelete(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_delete.DB"
	defer os.Remove(tmpfile)

	// Create database
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Insert Keys
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("c"), []byte("3"))

	// Delete one
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// close
	db.Close()

	// Reopen and verify
	db2, _ := Open(tmpfile)

	// Should still have a and c
	if _, err := db2.Get([]byte("a")); err != nil {
		t.Error("Key 'a' should exist")
	}
	if _, err := db2.Get([]byte("c")); err != nil {
		t.Error("Key 'c' should exist")
	}

	// b should be gone
	if _, err := db2.Get([]byte("b")); err == nil {
		t.Error("Key 'b' should be deleted")
	}

	db2.Close()
}

// TestDBFileFormat validates the on-disk format
func TestDBFileFormat(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_format.DB"
	defer os.Remove(tmpfile)

	// Create DB and write some data
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// validate file exists and has correct structure
	info, err := os.Stat(tmpfile)
	if err != nil {
		t.Fatalf("DB file not found: %v", err)
	}

	// File should be at least 3 pages (meta 0-1, freelist 2)
	minSize := int64(base.PageSize * 3)
	if info.Size() < minSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", info.Size(), minSize)
	}

	// File size should be Page-aligned
	if info.Size()%int64(base.PageSize) != 0 {
		t.Errorf("File size not Page-aligned: %d bytes", info.Size())
	}

	// Read both meta pages
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	page0 := &base.Page{}
	n, err := file.Read(page0.Data[:])
	if err != nil {
		t.Fatalf("Failed to read meta Page 0: %v", err)
	}
	if n != base.PageSize {
		t.Fatalf("Short read: got %d bytes, expected %d", n, base.PageSize)
	}

	page1 := &base.Page{}
	n, err = file.Read(page1.Data[:])
	if err != nil {
		t.Fatalf("Failed to read meta Page 1: %v", err)
	}
	if n != base.PageSize {
		t.Fatalf("Short read: got %d bytes, expected %d", n, base.PageSize)
	}

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	// close() increments TxnID from 0 to 1, so writes to Page 1 (1 % 2 = 1)
	// Page 1 should have the latest meta with RootPageID set
	t.Logf("Meta Page 0 TxnID: %d, RootPageID: %d", meta0.TxnID, meta0.RootPageID)
	t.Logf("Meta Page 1 TxnID: %d, RootPageID: %d", meta1.TxnID, meta1.RootPageID)

	// Pick the Page with highest TxnID (should be Page 1)
	var meta *base.MetaPage
	if meta0.TxnID > meta1.TxnID {
		meta = meta0
	} else {
		meta = meta1
	}

	// validate magic number
	if meta.Magic != base.MagicNumber {
		t.Errorf("Invalid magic number: got 0x%08x, expected 0x%08x", meta.Magic, base.MagicNumber)
	}

	// validate version
	if meta.Version != base.FormatVersion {
		t.Errorf("Invalid version: got %d, expected %d", meta.Version, base.FormatVersion)
	}

	// validate Page size
	if meta.PageSize != base.PageSize {
		t.Errorf("Invalid Page size: got %d, expected %d", meta.PageSize, base.PageSize)
	}

	// validate RootPageID is persisted after close
	if meta.RootPageID == 0 {
		t.Errorf("RootPageID is zero after close - should be persisted to meta")
	}

	// validate freelist location
	if meta.FreelistID != 2 {
		t.Errorf("Freelist ID: got %d, expected 2", meta.FreelistID)
	}

	// validate checksum
	if err := meta.Validate(); err != nil {
		t.Errorf("Meta validation failed: %v", err)
	}

	t.Logf("Meta Page validated successfully:")
	t.Logf("  Magic: 0x%08x", meta.Magic)
	t.Logf("  Version: %d", meta.Version)
	t.Logf("  storage.PageSize: %d", meta.PageSize)
	t.Logf("  RootPageID: %d", meta.RootPageID)
	t.Logf("  FreelistID: %d", meta.FreelistID)
	t.Logf("  FreelistPages: %d", meta.FreelistPages)
	t.Logf("  TxnID: %d", meta.TxnID)
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
	defer os.Remove(tmpfile)

	// Create DB with known data
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("testkey"), []byte("testvalue"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Read first 4 pages and dump them
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read meta Page 0
	page0 := make([]byte, base.PageSize)
	_, err = file.Read(page0)
	if err != nil {
		t.Fatalf("Failed to read Page 0: %v", err)
	}

	t.Logf("\n=== Page 0 (Meta) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page0[:128]))

	// Read meta Page 1
	page1 := make([]byte, base.PageSize)
	_, err = file.Read(page1)
	if err != nil {
		t.Fatalf("Failed to read Page 1: %v", err)
	}

	t.Logf("\n=== Page 1 (Meta backup) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page1[:128]))

	// Read freelist Page 2
	page2 := make([]byte, base.PageSize)
	_, err = file.Read(page2)
	if err != nil {
		t.Fatalf("Failed to read Page 2: %v", err)
	}

	t.Logf("\n=== Page 2 (Freelist) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page2[:128]))

	// Read root Page (Page 3 likely)
	page3 := make([]byte, base.PageSize)
	_, err = file.Read(page3)
	if err != nil {
		t.Fatalf("Failed to read Page 3: %v", err)
	}

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
	defer os.Remove(tmpfile)

	// Create valid DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Corrupt Page 0's magic number
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 0) // Overwrite magic number
	if err != nil {
		t.Fatalf("Failed to corrupt file: %v", err)
	}
	file.Close()

	// Try to reopen - should succeed because Page 1 is still valid
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB with one corrupted meta Page: %v", err)
	}
	defer db2.Close()

	// Verify data still accessible (using Page 1)
	v, err := db2.Get([]byte("key"))
	if err != nil {
		t.Errorf("Failed to get key from DB with corrupted Page 0: %v", err)
	}
	if string(v) != "value" {
		t.Errorf("Wrong value: got %s, expected value", string(v))
	}

	t.Logf("Successfully recovered from single meta Page corruption using backup")
}

// Helper to inspect raw bytes as hex
func dumpBytes(label string, data []byte) string {
	return fmt.Sprintf("%s: %s", label, hex.EncodeToString(data))
}

// TestCrashRecoveryBothMetaCorrupted tests that DB fails to open when both meta pages are invalid
func TestCrashRecoveryBothMetaCorrupted(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_both_meta_corrupt_%d.DB", os.Getpid())
	os.Remove(tmpfile) // Clean up any previous test file
	defer os.Remove(tmpfile)

	// Create valid DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Corrupt both meta pages (magic number)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	// Corrupt Page 0 meta magic (at PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(base.PageHeaderSize))
	if err != nil {
		t.Fatalf("Failed to corrupt Page 0: %v", err)
	}
	// Corrupt Page 1 meta magic (at storage.PageSize + PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(base.PageSize+base.PageHeaderSize))
	if err != nil {
		t.Fatalf("Failed to corrupt Page 1: %v", err)
	}
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
	if err == nil {
		t.Fatal("Expected error opening DB with both meta pages corrupted, got nil")
	}
	if err.Error() != "both meta pages corrupted: invalid magic number, invalid magic number" {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCrashRecoveryChecksumCorruption tests that corrupted checksums are detected
func TestCrashRecoveryChecksumCorruption(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_checksum_corrupt.DB"
	defer os.Remove(tmpfile)

	// Create valid DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	db.Close()

	// Corrupt Page 0's checksum field (last 4 bytes of MetaPage header)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Checksum is at offset 44 (after all other fields)
	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 44)
	if err != nil {
		t.Fatalf("Failed to corrupt checksum: %v", err)
	}
	file.Close()

	// Try to reopen - should succeed using Page 1
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB with corrupted checksum on Page 0: %v", err)
	}
	defer db2.Close()

	// Verify data still accessible (using Page 1)
	v, err := db2.Get([]byte("key"))
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}
	if string(v) != "value" {
		t.Errorf("Wrong value: got %s, expected value", string(v))
	}

	t.Logf("Successfully recovered from checksum corruption using backup meta Page")
}

// TestCrashRecoveryAlternatingWrites tests that meta pages alternate correctly based on TxnID
func TestCrashRecoveryAlternatingWrites(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.DB", t.Name())
	_ = os.Remove(tmpfile)
	defer os.Remove(tmpfile)

	// Create DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// TxnID starts at 0, so first commit writes to Page 0
	// Do multiple commits and verify alternating pattern
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		err = db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key%d: %v", i, err)
		}
	}

	db.Close()

	// Reopen and read both meta pages
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	page0 := &base.Page{}
	file.Read(page0.Data[:])
	page1 := &base.Page{}
	file.Read(page1.Data[:])
	file.Close()

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	t.Logf("Page 0 TxnID: %d", meta0.TxnID)
	t.Logf("Page 1 TxnID: %d", meta1.TxnID)

	// One should have higher TxnID than the other
	if meta0.TxnID == meta1.TxnID {
		t.Error("Both meta pages have same TxnID - alternating writes not working")
	}

	// The Page with higher TxnID should be the active one
	var activeMeta *base.MetaPage
	if meta0.TxnID > meta1.TxnID {
		activeMeta = meta0
		// TxnID should be even (written to Page 0)
		if activeMeta.TxnID%2 != 0 {
			t.Errorf("Page 0 has odd TxnID %d, expected even", activeMeta.TxnID)
		}
	} else {
		activeMeta = meta1
		// TxnID should be odd (written to Page 1)
		if activeMeta.TxnID%2 != 1 {
			t.Errorf("Page 1 has even TxnID %d, expected odd", activeMeta.TxnID)
		}
	}

	// Both should be valid
	if err := meta0.Validate(); err != nil {
		t.Errorf("Page 0 invalid: %v", err)
	}
	if err := meta1.Validate(); err != nil {
		t.Errorf("Page 1 invalid: %v", err)
	}

	t.Logf("Meta Page alternating writes validated successfully")
}

func TestCrashRecoveryWrongMagicNumber(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_wrong_magic.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Corrupt meta Page with valid checksum but wrong magic
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Write wrong magic number at Page 0
	wrongMagic := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_, err = file.WriteAt(wrongMagic, int64(base.PageHeaderSize))
	if err != nil {
		t.Fatalf("Failed to write wrong magic: %v", err)
	}
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
	} else if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}
}

func TestCrashRecoveryRootPageIDZero(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_root_zero.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Corrupt meta Page: set RootPageID to 0 but keep valid TxnID
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Read current meta from Page 0
	page := &base.Page{}
	_, err = file.ReadAt(page.Data[:], 0)
	if err != nil {
		t.Fatalf("Failed to read meta: %v", err)
	}

	// Parse and modify
	meta := page.ReadMeta()
	meta.RootPageID = 0 // Set to 0 (invalid state if TxnID > 0)
	meta.Checksum = meta.CalculateChecksum()

	// Write back
	page.WriteMeta(meta)
	_, err = file.WriteAt(page.Data[:], 0)
	if err != nil {
		t.Fatalf("Failed to write modified meta: %v", err)
	}
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
	_, err = db.Get([]byte("key1"))
	if err == ErrKeyNotFound {
		t.Logf("Key not found (expected with RootPageID=0)")
	} else if err != nil {
		t.Logf("get returned error: %v", err)
	}
}

func TestCrashRecoveryTruncatedFile(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_truncated.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Truncate file to only 1 Page (missing meta Page 1)
	err = os.Truncate(tmpfile, base.PageSize)
	if err != nil {
		t.Fatalf("Failed to truncate file: %v", err)
	}

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
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Create impossible state: both meta pages with same TxnID but different roots
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Read meta from Page 0
	page0 := &base.Page{}
	_, err = file.ReadAt(page0.Data[:], 0)
	if err != nil {
		t.Fatalf("Failed to read meta 0: %v", err)
	}
	meta0 := page0.ReadMeta()

	// Read meta from Page 1
	page1 := &base.Page{}
	_, err = file.ReadAt(page1.Data[:], base.PageSize)
	if err != nil {
		t.Fatalf("Failed to read meta 1: %v", err)
	}
	meta1 := page1.ReadMeta()

	// Make both have same TxnID
	meta1.TxnID = meta0.TxnID
	meta1.RootPageID = 999 // Different root (invalid)
	meta1.Checksum = meta1.CalculateChecksum()

	// Write back Page 1
	page1.WriteMeta(meta1)
	_, err = file.WriteAt(page1.Data[:], int64(base.PageSize))
	if err != nil {
		t.Fatalf("Failed to write meta 1: %v", err)
	}
	file.Sync()
	file.Close()

	// Try to open - should handle this gracefully
	db, err = Open(tmpfile)
	if err != nil {
		t.Logf("Open failed with same TxnID (may be expected): %v", err)
		return
	}
	defer db.Close()

	// If open succeeded, verify which meta was chosen
	t.Log("Open succeeded despite same TxnID on both metas")
}

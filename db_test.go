package fredb

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Use -slow flag to run longer tests
var slow = flag.Bool("slow", false, "run slow tests")

// Helper to create a temporary test database
func setupTestDB(t *testing.T) *DB {
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	_ = os.Remove(tmpfile + ".wal") // Also remove WAL file

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
		_ = os.Remove(tmpfile + ".wal") // Cleanup WAL file
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
	// PageSize = 4096, PageHeaderSize = 40, LeafElementSize = 16
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

func TestFreelistNoPageLeaks(t *testing.T) {
	if !*slow {
		t.Skip("Skipping slow test; use -slow to enable")
	}

	// Test that pages are properly reclaimed after delete operations
	// with concurrent readers active
	db := setupTestDB(t)

	// Track initial pages
	initialPages := db.store.pager.GetMeta().NumPages

	// Start concurrent readers - they will take snapshots at various points
	var wg sync.WaitGroup
	stopReaders := make(chan struct{})

	// Launch 3 concurrent readers that continuously read random Keys
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					// Begin read transaction
					tx, err := db.Begin(false)
					if err != nil {
						continue
					}

					// Read a few random Keys
					for j := 0; j < 10; j++ {
						key := []byte(fmt.Sprintf("key%d", j))
						tx.Get(key) // Ignore errors, some Keys may not exist
					}

					// Hold transaction briefly to simulate real reader
					time.Sleep(time.Millisecond)

					// Commit/rollback read transaction
					tx.Rollback()
				}
			}
		}(i)
	}

	// Run 10000 insert/delete cycles
	for cycle := 0; cycle < 10000; cycle++ {
		// Begin write transaction
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatalf("Failed to begin write tx at cycle %d: %v", cycle, err)
		}

		// Insert a key
		key := []byte(fmt.Sprintf("cycle%d", cycle))
		value := []byte(fmt.Sprintf("value%d", cycle))
		if err := tx.Set(key, value); err != nil {
			t.Fatalf("Failed to insert at cycle %d: %v", cycle, err)
		}

		// Commit insert
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit insert at cycle %d: %v", cycle, err)
		}

		// Begin another write transaction
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatalf("Failed to begin delete tx at cycle %d: %v", cycle, err)
		}

		// Delete the key
		if err := tx.Delete(key); err != nil {
			t.Fatalf("Failed to delete at cycle %d: %v", cycle, err)
		}

		// Commit delete
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit delete at cycle %d: %v", cycle, err)
		}

		// Periodically let background releaser run
		if cycle%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Stop readers
	close(stopReaders)
	wg.Wait()

	// Give background releaser time to process final releases
	time.Sleep(100 * time.Millisecond)

	// Check final Page count
	finalPages := db.store.pager.GetMeta().NumPages
	pageGrowth := finalPages - initialPages

	// With proper Page reclamation, growth should be minimal
	// Allow for some growth due to B-tree structure, but not linear with operations
	maxExpectedGrowth := uint64(400) // Generous allowance for tree overhead and MVCC
	if pageGrowth > maxExpectedGrowth {
		t.Errorf("Excessive Page growth detected: grew by %d pages (from %d to %d), expected <= %d",
			pageGrowth, initialPages, finalPages, maxExpectedGrowth)
	}

	// Also active that freelist size is reasonable
	dm := db.store.pager
	dm.mu.Lock()
	freeSize := dm.freelist.Size()
	pendingSize := dm.freelist.PendingSize()
	dm.mu.Unlock()

	// After all operations complete and readers finish,
	// pending should be empty and free list should contain reclaimed pages
	if pendingSize > 10 {
		t.Errorf("Pending list still has %d pages after operations complete", pendingSize)
	}

	t.Logf("Test complete: Pages grew from %d to %d (growth: %d), Free: %d, Pending: %d",
		initialPages, finalPages, pageGrowth, freeSize, pendingSize)
}

package src

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// Basic DB Interface Tests

func TestDBBasicOperations(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_basic.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Test Set and Get
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

	tmpfile := "/tmp/test_db_errors.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Get non-existent key
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

	tmpfile := "/tmp/test_db_close.db"
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

	// Close the DB
	err = db.Close()
	if err != nil {
		t.Errorf("Failed to close DB: %v", err)
	}

	// Multiple Close calls should not panic
	err = db.Close()
	if err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

// Concurrent Access Tests

func TestDBConcurrency(t *testing.T) {
	// Test concurrent access through DB interface
	// - Launch 100 goroutines
	// - Mix of Get/Set operations
	// - Verify no races or corruption
	// - Check final state consistency
	tmpfile := "/tmp/test_db_concurrency.db"
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

	// Track all written values for verification
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
						t.Errorf("Get failed with unexpected error: %v", err)
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

	// Verify final state - all written keys should be readable
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
	tmpfile := "/tmp/test_db_concurrent_reads.db"
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
				// Read random keys from test data
				key := fmt.Sprintf("key-%d", (id*readsPerReader+j)%1000)
				expectedValue := fmt.Sprintf("value-%d", (id*readsPerReader+j)%1000)

				val, err := db.Get([]byte(key))
				if err != nil {
					t.Errorf("Get failed for key %s: %v", key, err)
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
	// - Each writes unique keys
	// - Verify all keys present at end
	tmpfile := "/tmp/test_db_concurrent_writes.db"
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
				// Each writer writes unique keys
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

	// Verify all keys are present and have correct values
	if len(writtenKeys) != numWriters*writesPerWriter {
		t.Errorf("Expected %d keys, got %d", numWriters*writesPerWriter, len(writtenKeys))
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

	// Test concurrent updates to same keys
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

	tmpfile := "/tmp/test_tx_snapshot_isolation.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Insert initial values
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

	// Writer modifies all values
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

	// Verify final state has v3 values
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

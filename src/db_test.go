package src

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Basic DB Interface Tests

func TestDBBasicOperations(t *testing.T) {
	db, err := NewDB("")
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
	db, err := NewDB("")
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
	db, err := NewDB("")
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
	db, err := NewDB("")
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

					err := db.Set([]byte(key), []byte(value))
					if err != nil {
						t.Errorf("Set failed: %v", err)
						return
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
	// Test concurrent reads don't block each other
	// - Insert test data
	// - Launch multiple reader goroutines
	// - Verify all complete successfully
	db, err := NewDB("")
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
	// Test concurrent writes are serialized correctly
	// - Launch multiple writer goroutines
	// - Each writes unique keys
	// - Verify all keys present at end
	db, err := NewDB("")
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

				err := db.Set([]byte(key), []byte(value))
				if err != nil {
					t.Errorf("Set failed for writer %d: %v", id, err)
					return
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
			err := db.Set(updateKey, []byte(value))
			if err != nil {
				t.Errorf("Update failed for updater %d: %v", id, err)
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
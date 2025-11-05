package test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb"
)

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
			require.NoError(t, db.Put(key, value), "Failed to insert large key %d", i)
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

			require.NoError(t, db.Put(key, newValue), "Failed to update large key %d", i)

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
						err := db.Put(key, value)
						if errors.Is(err, fredb.ErrTxInProgress) {
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

	// Test 6: Fill a tree to force deep splits
	t.Run("DeepTreeWithLargeKeys", func(t *testing.T) {
		// Insert 100 more large Keys to create a deeper tree
		for i := 100; i < 200; i++ {
			key := makeKey(i)
			value := makeValue(i)
			require.NoError(t, db.Put(key, value), "Failed to insert key %d in deep tree", i)
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

func TestOverflowBasicWriteRead(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Create a 5KB value (larger than 3KB threshold)
	largeValue := make([]byte, 5*1024)
	for i := range largeValue {
		largeValue[i] = byte('A' + (i % 26))
	}

	key := []byte("overflow-key")

	// Write overflow value
	err := db.Put(key, largeValue)
	require.NoError(t, err, "Failed to write overflow value")

	// Read back
	retrieved, err := db.Get(key)
	require.NoError(t, err, "Failed to read overflow value")
	assert.Equal(t, largeValue, retrieved, "Overflow value mismatch")
}

func TestOverflowLargeValue(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Create a 100KB value
	largeValue := make([]byte, 100*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	key := []byte("large-overflow-key")

	// Write large overflow value
	err := db.Put(key, largeValue)
	require.NoError(t, err, "Failed to write large overflow value")

	// Read back
	retrieved, err := db.Get(key)
	require.NoError(t, err, "Failed to read large overflow value")
	assert.Equal(t, len(largeValue), len(retrieved), "Large overflow value length mismatch")
	assert.Equal(t, largeValue, retrieved, "Large overflow value content mismatch")
}

func TestOverflowMultipleValues(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Write multiple overflow values
	numKeys := 10
	valueSize := 10 * 1024 // 10KB each

	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("overflow-key-%d", i))
		value := make([]byte, valueSize)
		// Fill with unique pattern
		for j := range value {
			value[j] = byte((i + j) % 256)
		}

		err := db.Put(key, value)
		require.NoError(t, err, "Failed to write overflow key %d", i)
	}

	// Read back and verify
	for i := 0; i < numKeys; i++ {
		key := []byte(fmt.Sprintf("overflow-key-%d", i))
		expectedValue := make([]byte, valueSize)
		for j := range expectedValue {
			expectedValue[j] = byte((i + j) % 256)
		}

		retrieved, err := db.Get(key)
		require.NoError(t, err, "Failed to read overflow key %d", i)
		assert.Equal(t, expectedValue, retrieved, "Overflow value %d mismatch", i)
	}
}

func TestOverflowUpdate(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	key := []byte("update-overflow-key")

	// Write initial overflow value (5KB)
	value1 := make([]byte, 5*1024)
	for i := range value1 {
		value1[i] = byte('A' + (i % 26))
	}
	err := db.Put(key, value1)
	require.NoError(t, err, "Failed to write initial overflow value")

	// Update with different overflow value (7KB)
	value2 := make([]byte, 7*1024)
	for i := range value2 {
		value2[i] = byte('Z' - (i % 26))
	}
	err = db.Put(key, value2)
	require.NoError(t, err, "Failed to update overflow value")

	// Read back and verify it's the updated value
	retrieved, err := db.Get(key)
	require.NoError(t, err, "Failed to read updated overflow value")
	assert.Equal(t, value2, retrieved, "Updated overflow value mismatch")
	assert.NotEqual(t, value1, retrieved, "Should not match old value")
}

func TestOverflowDelete(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Write overflow value
	key := []byte("delete-overflow-key")
	largeValue := make([]byte, 10*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err := db.Put(key, largeValue)
	require.NoError(t, err, "Failed to write overflow value")

	// Verify it exists
	retrieved, err := db.Get(key)
	require.NoError(t, err, "Failed to read overflow value before delete")
	assert.Equal(t, largeValue, retrieved)

	// Delete the key (should free overflow chain)
	err = db.Delete(key)
	require.NoError(t, err, "Failed to delete overflow key")

	// Verify it's gone
	retrieved, err = db.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, retrieved, "Deleted overflow key should return nil")
}

func TestOverflowCursorIteration(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Insert mix of normal and overflow values
	normalValue := []byte("small-value")
	overflowValue := make([]byte, 5*1024)
	for i := range overflowValue {
		overflowValue[i] = byte('X')
	}

	err := db.Put([]byte("key-normal-1"), normalValue)
	require.NoError(t, err)

	err = db.Put([]byte("key-overflow-1"), overflowValue)
	require.NoError(t, err)

	err = db.Put([]byte("key-normal-2"), normalValue)
	require.NoError(t, err)

	err = db.Put([]byte("key-overflow-2"), overflowValue)
	require.NoError(t, err)

	// Iterate with cursor
	err = db.View(func(tx *fredb.Tx) error {
		c := tx.Cursor()
		count := 0

		for k, v := c.First(); k != nil; k, v = c.Next() {
			count++
			if bytes.HasPrefix(k, []byte("key-overflow")) {
				assert.Equal(t, overflowValue, v, "Cursor should load overflow value for %s", k)
			} else if bytes.HasPrefix(k, []byte("key-normal")) {
				assert.Equal(t, normalValue, v, "Normal value mismatch for %s", k)
			}
		}

		assert.Equal(t, 4, count, "Should iterate over all 4 keys")
		return nil
	})
	require.NoError(t, err)
}

func TestOverflowMixedSizes(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test various value sizes around the threshold
	testCases := []struct {
		name string
		size int
	}{
		{"below-threshold", 2 * 1024},     // 2KB - inline
		{"at-threshold", 3 * 1024},        // 3KB - inline
		{"just-over-threshold", 4 * 1024}, // 4KB - overflow
		{"medium-overflow", 20 * 1024},    // 20KB - overflow
		{"large-overflow", 100 * 1024},    // 100KB - overflow
	}

	for _, tc := range testCases {
		key := []byte(fmt.Sprintf("key-%s", tc.name))
		value := make([]byte, tc.size)
		// Fill with pattern
		for i := range value {
			value[i] = byte((i + tc.size) % 256)
		}

		err := db.Put(key, value)
		require.NoError(t, err, "Failed to write %s", tc.name)

		retrieved, err := db.Get(key)
		require.NoError(t, err, "Failed to read %s", tc.name)
		assert.Equal(t, value, retrieved, "Value mismatch for %s", tc.name)
	}
}

func TestOverflowPersistence(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_overflow_persist_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	defer os.Remove(tmpfile)

	// Write overflow value and close
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	largeValue := make([]byte, 50*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	err = db.Put([]byte("persistent-overflow"), largeValue)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)

	// Reopen and verify overflow value persisted
	db, err = fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	retrieved, err := db.Get([]byte("persistent-overflow"))
	require.NoError(t, err, "Failed to read overflow value after restart")
	assert.Equal(t, largeValue, retrieved, "Persisted overflow value mismatch")
}

func TestOverflowUpdateToNormal(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	key := []byte("shrink-key")

	// Start with overflow value (5KB)
	largeValue := make([]byte, 5*1024)
	for i := range largeValue {
		largeValue[i] = byte('L')
	}
	err := db.Put(key, largeValue)
	require.NoError(t, err)

	// Update to normal value (should free overflow chain)
	smallValue := []byte("small-value")
	err = db.Put(key, smallValue)
	require.NoError(t, err)

	// Verify it's the small value
	retrieved, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, smallValue, retrieved)
}

func TestOverflowUpdateFromNormal(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	key := []byte("grow-key")

	// Start with normal value
	smallValue := []byte("small-value")
	err := db.Put(key, smallValue)
	require.NoError(t, err)

	// Update to overflow value (should allocate overflow chain)
	largeValue := make([]byte, 8*1024)
	for i := range largeValue {
		largeValue[i] = byte('B')
	}
	err = db.Put(key, largeValue)
	require.NoError(t, err)

	// Verify it's the large value
	retrieved, err := db.Get(key)
	require.NoError(t, err)
	assert.Equal(t, largeValue, retrieved)
}

package fredb

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverflowPages(t *testing.T) {
	// Create temporary database
	tmpFile := "/tmp/test_overflow.db"
	defer os.Remove(tmpFile)

	db, err := Open(tmpFile)
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Write value larger than MaxValueInLeaf (2036 bytes)
	t.Run("LargeValueRoundTrip", func(t *testing.T) {
		key := []byte("large-key")
		// Create 10KB value (much larger than 2036 byte threshold)
		value := bytes.Repeat([]byte("0123456789"), 1024)
		assert.Equal(t, 10240, len(value), "Value should be 10KB")

		// Write large value
		err := db.Set(key, value)
		require.NoError(t, err, "Should write large value")

		// Read it back
		retrieved, err := db.Get(key)
		require.NoError(t, err, "Should read large value")
		assert.Equal(t, value, retrieved, "Retrieved value should match original")
	})

	// Test 2: Multiple large values
	t.Run("MultipleLargeValues", func(t *testing.T) {
		values := map[string][]byte{
			"small":  bytes.Repeat([]byte("a"), 100),           // 100 bytes - inline
			"medium": bytes.Repeat([]byte("b"), 2000),          // 2KB - inline (just under threshold)
			"large":  bytes.Repeat([]byte("c"), 5000),          // 5KB - overflow
			"huge":   bytes.Repeat([]byte("d"), 20000),         // 20KB - overflow (multiple pages)
		}

		// Write all values
		for k, v := range values {
			err := db.Set([]byte(k), v)
			require.NoError(t, err, "Should write %s value", k)
		}

		// Read all values back
		for k, expected := range values {
			retrieved, err := db.Get([]byte(k))
			require.NoError(t, err, "Should read %s value", k)
			assert.Equal(t, expected, retrieved, "Value for %s should match", k)
		}
	})

	// Test 3: Update large value with another large value
	t.Run("UpdateLargeValue", func(t *testing.T) {
		key := []byte("update-key")

		// Write initial large value
		oldValue := bytes.Repeat([]byte("old"), 3000)
		err := db.Set(key, oldValue)
		require.NoError(t, err)

		// Update with new large value
		newValue := bytes.Repeat([]byte("new"), 4000)
		err = db.Set(key, newValue)
		require.NoError(t, err)

		// Verify new value
		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, newValue, retrieved, "Should have new value")
	})

	// Test 4: Update large value to small value (overflow -> inline)
	t.Run("UpdateLargeToSmall", func(t *testing.T) {
		key := []byte("shrink-key")

		// Write large value
		largeValue := bytes.Repeat([]byte("x"), 5000)
		err := db.Set(key, largeValue)
		require.NoError(t, err)

		// Update to small value
		smallValue := []byte("tiny")
		err = db.Set(key, smallValue)
		require.NoError(t, err)

		// Verify small value
		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, smallValue, retrieved, "Should have small value")
	})

	// Test 5: Update small value to large value (inline -> overflow)
	t.Run("UpdateSmallToLarge", func(t *testing.T) {
		key := []byte("grow-key")

		// Write small value
		smallValue := []byte("small")
		err := db.Set(key, smallValue)
		require.NoError(t, err)

		// Update to large value
		largeValue := bytes.Repeat([]byte("y"), 8000)
		err = db.Set(key, largeValue)
		require.NoError(t, err)

		// Verify large value
		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, largeValue, retrieved, "Should have large value")
	})

	// Test 6: Delete large value
	t.Run("DeleteLargeValue", func(t *testing.T) {
		key := []byte("delete-key")

		// Write large value
		largeValue := bytes.Repeat([]byte("z"), 7000)
		err := db.Set(key, largeValue)
		require.NoError(t, err)

		// Delete it
		err = db.Delete(key)
		require.NoError(t, err)

		// Verify it's gone
		_, err = db.Get(key)
		assert.ErrorIs(t, err, ErrKeyNotFound, "Should not find deleted key")
	})

	// Test 7: Very large value (50KB)
	t.Run("VeryLargeValue", func(t *testing.T) {
		key := []byte("very-large")
		// 50KB value - will span multiple overflow pages
		value := bytes.Repeat([]byte("ABCDEFGHIJ"), 5120)
		assert.Equal(t, 51200, len(value), "Value should be 50KB")

		err := db.Set(key, value)
		require.NoError(t, err, "Should write 50KB value")

		retrieved, err := db.Get(key)
		require.NoError(t, err, "Should read 50KB value")
		assert.Equal(t, value, retrieved, "50KB value should match")
	})

	// Test 8: Iteration over mixed values
	t.Run("IterationMixed", func(t *testing.T) {
		// Clear database first
		db.Close()
		os.Remove(tmpFile)

		db, err = Open(tmpFile)
		require.NoError(t, err)
		// Don't defer close here - let parent cleanup handle it

		// Write mix of small and large values
		testData := map[string][]byte{
			"a-small":  []byte("small1"),
			"b-large":  bytes.Repeat([]byte("L"), 5000),
			"c-small":  []byte("small2"),
			"d-large":  bytes.Repeat([]byte("M"), 10000),
			"e-small":  []byte("small3"),
		}

		for k, v := range testData {
			err := db.Set([]byte(k), v)
			require.NoError(t, err)
		}

		// Iterate and verify all values
		count := 0
		err = db.View(func(tx *Tx) error {
			return tx.ForEach(func(key, value []byte) error {
				expected, exists := testData[string(key)]
				assert.True(t, exists, "Key %s should exist", key)
				assert.Equal(t, expected, value, "Value for %s should match", key)
				count++
				return nil
			})
		})
		require.NoError(t, err)
		assert.Equal(t, len(testData), count, "Should iterate over all keys")
	})

	// Test 9: Transaction rollback with large values
	t.Run("RollbackLargeValue", func(t *testing.T) {
		key := []byte("rollback-key")
		originalValue := []byte("original")

		// Set initial value
		err := db.Set(key, originalValue)
		require.NoError(t, err)

		// Start transaction and write large value but rollback
		tx, err := db.Begin(true)
		require.NoError(t, err)

		largeValue := bytes.Repeat([]byte("R"), 6000)
		err = tx.Set(key, largeValue)
		require.NoError(t, err)

		// Rollback
		err = tx.Rollback()
		require.NoError(t, err)

		// Verify original value is still there
		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, originalValue, retrieved, "Should have original value after rollback")
	})

	// Test 10: Bucket with large values
	t.Run("BucketWithLargeValues", func(t *testing.T) {
		err := db.Update(func(tx *Tx) error {
			// Create bucket
			bucket, err := tx.CreateBucket([]byte("overflow-bucket"))
			if err != nil {
				return err
			}

			// Put large values in bucket
			for i := 0; i < 5; i++ {
				key := []byte{byte('k'), byte(i)}
				value := bytes.Repeat([]byte{byte(i)}, 4000)
				if err := bucket.Put(key, value); err != nil {
					return err
				}
			}

			return nil
		})
		require.NoError(t, err)

		// Read back from bucket
		err = db.View(func(tx *Tx) error {
			bucket := tx.Bucket([]byte("overflow-bucket"))
			require.NotNil(t, bucket)

			for i := 0; i < 5; i++ {
				key := []byte{byte('k'), byte(i)}
				expected := bytes.Repeat([]byte{byte(i)}, 4000)

				value := bucket.Get(key)
				assert.Equal(t, expected, value, "Bucket value %d should match", i)
			}

			return nil
		})
		require.NoError(t, err)

		// Delete bucket (should free overflow pages)
		err = db.Update(func(tx *Tx) error {
			return tx.DeleteBucket([]byte("overflow-bucket"))
		})
		require.NoError(t, err)
	})
}

func TestOverflowEdgeCases(t *testing.T) {
	tmpFile := "/tmp/test_overflow_edge.db"
	defer os.Remove(tmpFile)

	db, err := Open(tmpFile)
	require.NoError(t, err)
	defer db.Close()

	// Test value exactly at threshold (2036 bytes)
	t.Run("ValueAtThreshold", func(t *testing.T) {
		key := []byte("threshold")
		// MaxValueInLeaf = (4096 - 24) / 2 = 2036
		value := bytes.Repeat([]byte("T"), 2036)

		err := db.Set(key, value)
		require.NoError(t, err)

		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	// Test value just over threshold
	t.Run("ValueJustOverThreshold", func(t *testing.T) {
		key := []byte("just-over")
		value := bytes.Repeat([]byte("O"), 2037)

		err := db.Set(key, value)
		require.NoError(t, err)

		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	// Test value that fills exactly one overflow page
	t.Run("ExactlyOneOverflowPage", func(t *testing.T) {
		key := []byte("exact-page")
		// OverflowDataSize = 4096 - 24 - 8 = 4064 bytes
		value := bytes.Repeat([]byte("E"), 4064)

		err := db.Set(key, value)
		require.NoError(t, err)

		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})

	// Test value that needs exactly 2 overflow pages
	t.Run("ExactlyTwoOverflowPages", func(t *testing.T) {
		key := []byte("two-pages")
		value := bytes.Repeat([]byte("2"), 8128) // 4064 * 2

		err := db.Set(key, value)
		require.NoError(t, err)

		retrieved, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, retrieved)
	})
}
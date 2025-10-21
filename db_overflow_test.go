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

// TestOverflowChainReuse tests that overflow chains are reused during splits and CoW
func TestOverflowChainReuse(t *testing.T) {
	tmpFile := "/tmp/test_overflow_reuse.db"
	defer os.Remove(tmpFile)

	db, err := Open(tmpFile, WithSyncOff())
	require.NoError(t, err)
	defer db.Close()

	// Test 1: Overflow chain reuse during node splits
	t.Run("ReuseChainsDuringSplits", func(t *testing.T) {
		// Create large values that will require overflow pages (3KB each, needs 1 overflow page)
		largeValue := bytes.Repeat([]byte("x"), 3000)

		// Insert enough keys to cause splits
		// With 3KB values, we can fit ~1 key per leaf node before splitting
		numKeys := 50
		for i := 0; i < numKeys; i++ {
			key := []byte{byte('a'), byte(i / 256), byte(i % 256)}
			err := db.Set(key, largeValue)
			require.NoError(t, err, "Insert %d should succeed", i)
		}

		// Verify all values are correct
		for i := 0; i < numKeys; i++ {
			key := []byte{byte('a'), byte(i / 256), byte(i % 256)}
			retrieved, err := db.Get(key)
			require.NoError(t, err, "Get key %d should succeed", i)
			assert.Equal(t, largeValue, retrieved, "Value %d should match", i)
		}

		// Get stats
		stats := db.Stats()
		t.Logf("After %d inserts: Writes=%d, Reads=%d", numKeys, stats.Store.Writes, stats.Store.Reads)

		// Reality check: each insert triggers:
		// - 1 overflow page for the value
		// - Multiple node pages (for CoW and splits up the tree)
		// - Metadata page writes per transaction
		// The key metric is that writes/insert should be reasonable (~30-40 writes/insert)
		// If overflow chains were NOT reused, we'd see significantly more (50+ writes/insert)
		avgWritesPerInsert := float64(stats.Store.Writes) / float64(numKeys)
		t.Logf("Average writes per insert: %.1f", avgWritesPerInsert)

		// Sanity check: should be less than 40 writes per insert on average
		assert.Less(t, avgWritesPerInsert, 40.0,
			"Should reuse overflow chains during splits (writes should be bounded)")
	})

	// Test 2: Verify overflow chains are preserved after split
	t.Run("VerifyChainIntegrityAfterSplit", func(t *testing.T) {
		// Create a new DB for clean test
		tmpFile2 := "/tmp/test_overflow_reuse2.db"
		defer os.Remove(tmpFile2)

		db2, err := Open(tmpFile2, WithSyncOff())
		require.NoError(t, err)
		defer db2.Close()

		// Insert large values with distinct patterns to detect corruption
		type testValue struct {
			key   []byte
			value []byte
		}

		testValues := []testValue{}
		for i := 0; i < 30; i++ {
			key := []byte{byte('b'), byte(i)}
			// Each value has a unique pattern (repeated byte value = i)
			value := bytes.Repeat([]byte{byte(i)}, 4000)
			testValues = append(testValues, testValue{key, value})

			err := db2.Set(key, value)
			require.NoError(t, err, "Insert %d should succeed", i)
		}

		// Close and reopen to ensure persistence
		db2.Close()
		db2, err = Open(tmpFile2, WithSyncOff())
		require.NoError(t, err)
		defer db2.Close()

		// Verify all values retained their unique patterns (no chain corruption)
		for i, tv := range testValues {
			retrieved, err := db2.Get(tv.key)
			require.NoError(t, err, "Get key %d should succeed after reopen", i)
			assert.Equal(t, tv.value, retrieved,
				"Value %d should match after split and reopen (chain integrity)", i)

			// Extra check: verify the pattern is correct
			if len(retrieved) > 0 {
				assert.Equal(t, byte(i), retrieved[0],
					"First byte should match pattern for value %d", i)
				assert.True(t, bytes.Equal(bytes.Repeat([]byte{byte(i)}, 4000), retrieved),
					"Entire value should have consistent pattern for value %d", i)
			}
		}
	})

	// Test 3: Overflow chain reuse with mixed operations
	t.Run("MixedOperationsPreserveChains", func(t *testing.T) {
		tmpFile3 := "/tmp/test_overflow_reuse3.db"
		defer os.Remove(tmpFile3)

		db3, err := Open(tmpFile3, WithSyncOff())
		require.NoError(t, err)
		defer db3.Close()

		// Phase 1: Insert large values
		numInitial := 20
		largeValue := bytes.Repeat([]byte("L"), 5000)
		for i := 0; i < numInitial; i++ {
			key := []byte{byte('c'), byte(i)}
			err := db3.Set(key, largeValue)
			require.NoError(t, err)
		}

		stats1 := db3.Stats()

		// Phase 2: Insert more keys to cause additional splits (should reuse existing chains)
		numAdditional := 20
		for i := numInitial; i < numInitial+numAdditional; i++ {
			key := []byte{byte('c'), byte(i)}
			err := db3.Set(key, largeValue)
			require.NoError(t, err)
		}

		stats2 := db3.Stats()

		// Calculate incremental writes for phase 2
		phase2Writes := stats2.Store.Writes - stats1.Store.Writes

		t.Logf("Phase 1 writes: %d, Phase 2 writes: %d", stats1.Store.Writes, phase2Writes)

		// Phase 2 should have similar or fewer writes per key than phase 1
		// because it benefits from chain reuse during splits
		avgPhase1 := float64(stats1.Store.Writes) / float64(numInitial)
		avgPhase2 := float64(phase2Writes) / float64(numAdditional)

		t.Logf("Avg writes per key: Phase 1=%.1f, Phase 2=%.1f", avgPhase1, avgPhase2)

		// Phase 2 may be worse than phase 1 due to deeper tree (more CoW up the tree)
		// But it should still be reasonable - not orders of magnitude worse
		// Without chain reuse, phase 2 would be 100+ writes/insert
		assert.Less(t, avgPhase2, 100.0,
			"Phase 2 should benefit from overflow chain reuse (bounded writes per insert)")

		// Verify data integrity
		for i := 0; i < numInitial+numAdditional; i++ {
			key := []byte{byte('c'), byte(i)}
			retrieved, err := db3.Get(key)
			require.NoError(t, err)
			assert.Equal(t, largeValue, retrieved, "Value %d should match", i)
		}
	})

	// Test 4: Updating one value doesn't rebuild other values' chains
	t.Run("SelectiveChainRebuild", func(t *testing.T) {
		tmpFile4 := "/tmp/test_overflow_reuse4.db"
		defer os.Remove(tmpFile4)

		db4, err := Open(tmpFile4, WithSyncOff())
		require.NoError(t, err)
		defer db4.Close()

		// Insert 3 large values in same leaf node
		value1 := bytes.Repeat([]byte("1"), 2500)
		value2 := bytes.Repeat([]byte("2"), 2500)
		value3 := bytes.Repeat([]byte("3"), 2500)

		// Use keys that will likely be in same leaf
		err = db4.Set([]byte("key1"), value1)
		require.NoError(t, err)
		err = db4.Set([]byte("key2"), value2)
		require.NoError(t, err)
		err = db4.Set([]byte("key3"), value3)
		require.NoError(t, err)

		stats1 := db4.Stats()

		// Update only key2
		newValue2 := bytes.Repeat([]byte("X"), 2500)
		err = db4.Set([]byte("key2"), newValue2)
		require.NoError(t, err)

		stats2 := db4.Stats()
		updateWrites := stats2.Store.Writes - stats1.Store.Writes

		t.Logf("Writes for single update: %d", updateWrites)

		// Should only rebuild chain for key2, not key1 or key3
		// Expected: ~3-5 writes (1 new overflow page + node CoW + metadata)
		// If it rebuilds all 3 chains: ~9+ writes
		assert.Less(t, updateWrites, uint64(7),
			"Updating one value should not rebuild other overflow chains in same node")

		// Verify all values
		retrieved1, err := db4.Get([]byte("key1"))
		require.NoError(t, err)
		assert.Equal(t, value1, retrieved1, "key1 should be unchanged")

		retrieved2, err := db4.Get([]byte("key2"))
		require.NoError(t, err)
		assert.Equal(t, newValue2, retrieved2, "key2 should have new value")

		retrieved3, err := db4.Get([]byte("key3"))
		require.NoError(t, err)
		assert.Equal(t, value3, retrieved3, "key3 should be unchanged")
	})

	// Test 5: Deleting one value doesn't affect others' chains
	t.Run("DeletionPreservesOtherChains", func(t *testing.T) {
		tmpFile5 := "/tmp/test_overflow_reuse5.db"
		defer os.Remove(tmpFile5)

		db5, err := Open(tmpFile5, WithSyncOff())
		require.NoError(t, err)
		defer db5.Close()

		// Insert multiple large values
		largeValue := bytes.Repeat([]byte("D"), 3000)
		keys := [][]byte{
			[]byte("del1"),
			[]byte("del2"),
			[]byte("del3"),
			[]byte("del4"),
			[]byte("del5"),
		}

		for _, key := range keys {
			err := db5.Set(key, largeValue)
			require.NoError(t, err)
		}

		// Delete middle key
		err = db5.Delete([]byte("del3"))
		require.NoError(t, err)

		// Verify other keys still work
		for _, key := range [][]byte{[]byte("del1"), []byte("del2"), []byte("del4"), []byte("del5")} {
			retrieved, err := db5.Get(key)
			require.NoError(t, err)
			assert.Equal(t, largeValue, retrieved, "Key %s should still have correct value", key)
		}

		// Verify deleted key is gone
		_, err = db5.Get([]byte("del3"))
		assert.ErrorIs(t, err, ErrKeyNotFound, "Deleted key should not be found")
	})
}
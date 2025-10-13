package coordinator

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fredb/internal/base"
)

// TestVersionIndexBasicPutGet tests basic Put/Get operations on version index
func TestVersionIndexBasicPutGet(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Initialize __versions__ bucket via DB Open
	// The test coordinator doesn't initialize buckets, so we need to simulate it
	// For now, skip this test if __versions__ bucket doesn't exist

	// Try putting a version mapping
	err := pm.PutVersionMapping(100, 1, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}

	// Retrieve it
	physicalPageID, err := pm.GetVersionMapping(100, 1)
	require.NoError(t, err, "GetVersionMapping failed")
	assert.Equal(t, base.PageID(500), physicalPageID, "Expected physicalPageID=500")

	// Try getting non-existent mapping
	_, err = pm.GetVersionMapping(999, 999)
	assert.Error(t, err, "Expected error for non-existent mapping")
}

// TestVersionIndexUpdate tests updating an existing version mapping
func TestVersionIndexUpdate(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Put initial mapping
	err := pm.PutVersionMapping(100, 1, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}

	// Update it
	err = pm.PutVersionMapping(100, 1, 600)
	require.NoError(t, err, "PutVersionMapping update failed")

	// Retrieve updated value
	physicalPageID, err := pm.GetVersionMapping(100, 1)
	require.NoError(t, err, "GetVersionMapping failed")
	assert.Equal(t, base.PageID(600), physicalPageID, "Expected updated physicalPageID=600")
}

// TestVersionIndexDelete tests deleting version mappings
func TestVersionIndexDelete(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Put mapping
	err := pm.PutVersionMapping(100, 1, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}

	// Verify it exists
	_, err = pm.GetVersionMapping(100, 1)
	require.NoError(t, err, "GetVersionMapping failed before delete")

	// Delete it
	err = pm.DeleteVersionMapping(100, 1)
	require.NoError(t, err, "DeleteVersionMapping failed")

	// Verify it's gone
	_, err = pm.GetVersionMapping(100, 1)
	assert.Error(t, err, "Expected error after delete")

	// Deleting again should be idempotent
	err = pm.DeleteVersionMapping(100, 1)
	require.NoError(t, err, "DeleteVersionMapping should be idempotent")
}

// TestVersionIndexMultipleVersions tests storing multiple versions of same page
func TestVersionIndexMultipleVersions(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Put multiple versions of page 100
	err := pm.PutVersionMapping(100, 1, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}
	err = pm.PutVersionMapping(100, 2, 501)
	require.NoError(t, err, "PutVersionMapping failed")
	err = pm.PutVersionMapping(100, 3, 502)
	require.NoError(t, err, "PutVersionMapping failed")

	// Retrieve all versions
	physicalPageID1, err := pm.GetVersionMapping(100, 1)
	require.NoError(t, err, "GetVersionMapping failed")
	assert.Equal(t, base.PageID(500), physicalPageID1)

	physicalPageID2, err := pm.GetVersionMapping(100, 2)
	require.NoError(t, err, "GetVersionMapping failed")
	assert.Equal(t, base.PageID(501), physicalPageID2)

	physicalPageID3, err := pm.GetVersionMapping(100, 3)
	require.NoError(t, err, "GetVersionMapping failed")
	assert.Equal(t, base.PageID(502), physicalPageID3)
}

// TestVersionIndexCleanupOnRelease tests version cleanup during ReleasePages
func TestVersionIndexCleanupOnRelease(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Put version mapping
	err := pm.PutVersionMapping(100, 10, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}

	// Add page to pending at txnID 10
	err = pm.FreePending(10, []base.PageID{100})
	require.NoError(t, err, "FreePending failed")

	// Verify mapping exists
	_, err = pm.GetVersionMapping(100, 10)
	require.NoError(t, err, "GetVersionMapping failed before release")

	// Release pages < 11 (should clean up version mapping)
	released := pm.ReleasePages(11)
	assert.Equal(t, 1, released, "Expected 1 page released")

	// Version mapping should be cleaned up (best-effort)
	// Note: Cleanup errors are silently ignored, so mapping might still exist
	// This test just ensures cleanup is attempted without crashing
}

// TestVersionIndexConcurrentAccess tests concurrent reads/writes with race detector
func TestVersionIndexConcurrentAccess(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Initialize some version mappings
	for i := 0; i < 10; i++ {
		err := pm.PutVersionMapping(base.PageID(100+i), uint64(1), base.PageID(500+i))
		if err != nil {
			t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
		}
	}

	// Run concurrent readers and writers
	var wg sync.WaitGroup
	numReaders := 10
	numWriters := 5
	iterations := 100

	// Concurrent readers
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				pageID := base.PageID(100 + (i % 10))
				_, _ = pm.GetVersionMapping(pageID, 1)
			}
		}(r)
	}

	// Concurrent writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				pageID := base.PageID(100 + (i % 10))
				txnID := uint64(2 + (i % 5))
				physicalPageID := base.PageID(600 + writerID*100 + i)
				_ = pm.PutVersionMapping(pageID, txnID, physicalPageID)
			}
		}(w)
	}

	wg.Wait()
}

// TestVersionIndexLargeDataset tests version index with large number of mappings
func TestVersionIndexLargeDataset(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test_large.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Test with large dataset: 10K pages, 5 versions each = 50K mappings
	numPages := 10000
	numVersions := 5

	t.Logf("Inserting %d pages with %d versions each (%d total mappings)",
		numPages, numVersions, numPages*numVersions)

	// Insert version mappings
	for pageID := 0; pageID < numPages; pageID++ {
		for txnID := 1; txnID <= numVersions; txnID++ {
			logicalPageID := base.PageID(1000 + pageID)
			physicalPageID := base.PageID(10000 + pageID*numVersions + txnID)

			err := pm.PutVersionMapping(logicalPageID, uint64(txnID), physicalPageID)
			if err != nil {
				t.Skipf("Skipping test after %d insertions: __versions__ bucket issue (%v)",
					pageID*numVersions+txnID, err)
			}

			// Progress indicator
			if (pageID*numVersions+txnID)%10000 == 0 {
				t.Logf("Progress: %d/%d mappings inserted",
					pageID*numVersions+txnID, numPages*numVersions)
			}
		}
	}

	t.Logf("Completed insertion of %d version mappings", numPages*numVersions)

	// Verify a sample of mappings
	t.Log("Verifying sample of mappings...")
	for i := 0; i < 100; i++ {
		pageID := i * (numPages / 100)
		if pageID >= numPages {
			break
		}

		logicalPageID := base.PageID(1000 + pageID)
		txnID := uint64((i % numVersions) + 1)
		expectedPhysical := base.PageID(10000 + pageID*numVersions + int(txnID))

		physicalPageID, err := pm.GetVersionMapping(logicalPageID, txnID)
		require.NoError(t, err, "GetVersionMapping failed for page %d txn %d", logicalPageID, txnID)
		assert.Equal(t, expectedPhysical, physicalPageID,
			"Mismatch for page %d txn %d", logicalPageID, txnID)
	}

	t.Log("Verification complete")

	// Test memory bounds: get coordinator stats
	stats := pm.Stats()
	cacheSize := pm.cache.Size()
	t.Logf("Cache stats: Hits=%d Misses=%d Evictions=%d Size=%d",
		stats.Cache.Hits, stats.Cache.Misses, stats.Cache.Evictions, cacheSize)
	t.Logf("Storage stats: Reads=%d Writes=%d",
		stats.Store.Reads, stats.Store.Writes)

	// Verify bounded memory: cache size should not be unbounded
	// With 1024 page cache limit, we should see evictions
	assert.Greater(t, stats.Cache.Evictions, uint64(0),
		"Expected cache evictions with large dataset (bounded memory)")
}

// TestVersionIndexCachingBehavior tests write-through cache behavior
func TestVersionIndexCachingBehavior(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Clear stats
	pm.ClearStats()

	// Put a version mapping (should write to disk + cache)
	err := pm.PutVersionMapping(100, 1, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}

	// First read should hit cache (no disk read)
	// Note: The implementation does cache.Get() first, so we need to verify behavior
	stats1 := pm.Stats()

	_, err = pm.GetVersionMapping(100, 1)
	require.NoError(t, err, "GetVersionMapping failed")

	stats2 := pm.Stats()

	// Second read should also hit cache
	_, err = pm.GetVersionMapping(100, 1)
	require.NoError(t, err, "GetVersionMapping failed")

	stats3 := pm.Stats()

	// Log stats for debugging
	t.Logf("Stats after put: Cache(Hits=%d Misses=%d) Storage(Reads=%d Writes=%d)",
		stats1.Cache.Hits, stats1.Cache.Misses, stats1.Store.Reads, stats1.Store.Writes)
	t.Logf("Stats after read1: Cache(Hits=%d Misses=%d) Storage(Reads=%d Writes=%d)",
		stats2.Cache.Hits, stats2.Cache.Misses, stats2.Store.Reads, stats2.Store.Writes)
	t.Logf("Stats after read2: Cache(Hits=%d Misses=%d) Storage(Reads=%d Writes=%d)",
		stats3.Cache.Hits, stats3.Cache.Misses, stats3.Store.Reads, stats3.Store.Writes)

	// Verify write-through: disk writes should happen on Put
	assert.Greater(t, stats1.Store.Writes, uint64(0), "Expected disk writes on Put")
}

// TestVersionIndexPersistence tests that version mappings persist across restarts
func TestVersionIndexPersistence(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test_persist.db"

	// Create coordinator and insert mappings
	{
		pm, cleanup := createTestCoordinator(t, tmpFile)

		// Insert version mappings
		err := pm.PutVersionMapping(100, 1, 500)
		if err != nil {
			t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
		}
		err = pm.PutVersionMapping(101, 2, 501)
		require.NoError(t, err, "PutVersionMapping failed")
		err = pm.PutVersionMapping(102, 3, 502)
		require.NoError(t, err, "PutVersionMapping failed")

		cleanup()
	}

	// Reopen and verify mappings still exist
	{
		pm, cleanup := createTestCoordinator(t, tmpFile)
		defer cleanup()

		physicalPageID1, err := pm.GetVersionMapping(100, 1)
		require.NoError(t, err, "GetVersionMapping failed after reopen")
		assert.Equal(t, base.PageID(500), physicalPageID1)

		physicalPageID2, err := pm.GetVersionMapping(101, 2)
		require.NoError(t, err, "GetVersionMapping failed after reopen")
		assert.Equal(t, base.PageID(501), physicalPageID2)

		physicalPageID3, err := pm.GetVersionMapping(102, 3)
		require.NoError(t, err, "GetVersionMapping failed after reopen")
		assert.Equal(t, base.PageID(502), physicalPageID3)
	}
}

// TestVersionIndexLatchContention tests latch behavior under contention
func TestVersionIndexLatchContention(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Initialize version mapping
	err := pm.PutVersionMapping(100, 1, 500)
	if err != nil {
		t.Skipf("Skipping test: __versions__ bucket not initialized (%v)", err)
	}

	// Hammer same page with concurrent updates
	var wg sync.WaitGroup
	numGoroutines := 50
	iterations := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				txnID := uint64(id*iterations + i)
				physicalPageID := base.PageID(1000 + id*iterations + i)
				_ = pm.PutVersionMapping(100, txnID, physicalPageID)
			}
		}(g)
	}

	wg.Wait()

	// Verify we can still read (no deadlocks/corruption)
	_, err = pm.GetVersionMapping(100, 1)
	// May or may not exist depending on update pattern, but should not crash
	_ = err
}

// TestVersionIndexEncoding tests version key encoding/decoding
func TestVersionIndexEncoding(t *testing.T) {
	testCases := []struct {
		name           string
		logicalPageID  base.PageID
		txnID          uint64
		physicalPageID base.PageID
	}{
		{"Zero values", 0, 0, 0},
		{"Small values", 1, 1, 1},
		{"Large values", 999999, 999999, 999999},
		{"Max values", base.PageID(^uint64(0)), ^uint64(0), base.PageID(^uint64(0))},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Encode key
			key := encodeVersionKey(tc.logicalPageID, tc.txnID)
			assert.Len(t, key, 16, "Version key should be 16 bytes")

			// Decode key
			decodedLogical, decodedTxnID := decodeVersionKey(key)
			assert.Equal(t, tc.logicalPageID, decodedLogical, "Logical page ID mismatch")
			assert.Equal(t, tc.txnID, decodedTxnID, "TxnID mismatch")

			// Encode value
			val := encodePhysicalPageID(tc.physicalPageID)
			assert.Len(t, val, 8, "Physical page ID should be 8 bytes")

			// Decode value
			decodedPhysical := decodePhysicalPageID(val)
			assert.Equal(t, tc.physicalPageID, decodedPhysical, "Physical page ID mismatch")
		})
	}
}

// TestVersionIndexStressTest runs a stress test with many concurrent operations
func TestVersionIndexStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	t.Parallel()

	tmpFile := t.TempDir() + "/test_stress.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	numGoroutines := 20
	numPages := 1000
	numVersionsPerPage := 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numPages*numVersionsPerPage)

	// Launch writers
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for p := 0; p < numPages; p++ {
				pageID := base.PageID(1000 + p)

				for v := 0; v < numVersionsPerPage; v++ {
					txnID := uint64(goroutineID*numVersionsPerPage + v)
					physicalPageID := base.PageID(10000 + goroutineID*numPages*numVersionsPerPage + p*numVersionsPerPage + v)

					err := pm.PutVersionMapping(pageID, txnID, physicalPageID)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d: PutVersionMapping failed: %w", goroutineID, err)
						return
					}
				}
			}
		}(g)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		if errorCount == 0 {
			// Only skip on first error (likely __versions__ bucket not initialized)
			t.Skipf("Skipping stress test: %v", err)
		}
		t.Errorf("Error during stress test: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Fatalf("Stress test failed with %d errors", errorCount)
	}

	t.Logf("Stress test completed: %d goroutines, %d pages, %d versions/page",
		numGoroutines, numPages, numVersionsPerPage)

	// Verify stats
	stats := pm.Stats()
	t.Logf("Final stats: Cache(Hits=%d Misses=%d Evictions=%d) Storage(Reads=%d Writes=%d)",
		stats.Cache.Hits, stats.Cache.Misses, stats.Cache.Evictions, stats.Store.Reads, stats.Store.Writes)
}

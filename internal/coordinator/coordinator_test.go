package coordinator

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/storage"
)

var _ = flag.Bool("slow", false, "run slow tests")

// Helper to create a coordinator with dependencies for testing
func createTestCoordinator(t *testing.T, tmpFile string) (*Coordinator, func()) {
	stor, err := storage.NewStorage(tmpFile, storage.DirectIO)
	require.NoError(t, err, "Failed to create store")

	cacheInstance := cache.NewCache(1024, storage.DirectIO)

	pm, err := NewCoordinator(stor, cacheInstance)
	require.NoError(t, err, "Failed to create Coordinator")

	cleanup := func() {
		_ = pm.Close()
	}

	return pm, cleanup
}

func TestPageManagerFreeListPending(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Add some pages to pending at different transactions
	require.NoError(t, pm.FreePending(10, []base.PageID{100, 101, 102}), "FreePending failed")
	require.NoError(t, pm.FreePending(11, []base.PageID{200, 201}), "FreePending failed")
	require.NoError(t, pm.FreePending(12, []base.PageID{300}), "FreePending failed")

	// Release pages from transactions < 11 (i.e., txnID 10)
	released := pm.ReleasePages(11)
	assert.Equal(t, 3, released, "Expected 3 pages released")

	// Release everything
	released = pm.ReleasePages(100)
	assert.Equal(t, 3, released, "Expected 3 more pages released")

	// Verify we can allocate the freed pages
	allocated := make(map[base.PageID]bool)
	for i := 0; i < 6; i++ {
		id, err := pm.AssignPageID()
		require.NoError(t, err, "AssignPageID failed")
		// Pages 100, 101, 102, 200, 201, 300 should be reused
		if id == 100 || id == 101 || id == 102 || id == 200 || id == 201 || id == 300 {
			allocated[id] = true
		}
	}

	// Should have allocated at least some of the freed pages
	assert.NotEmpty(t, allocated, "Expected to reuse some freed pages, but none were allocated")
}

func TestPageManagerFreeListReleaseOrder(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Add pages at various transaction IDs
	pm.FreePending(50, []base.PageID{500})
	pm.FreePending(10, []base.PageID{100})
	pm.FreePending(30, []base.PageID{300})
	pm.FreePending(20, []base.PageID{200})

	// Release up to 25 should release txns 10 and 20
	released := pm.ReleasePages(25)
	assert.Equal(t, 2, released, "Expected 2 pages released (txn 10, 20)")

	// Release pages from transactions < 31 (i.e., txnID 30)
	released = pm.ReleasePages(31)
	assert.Equal(t, 1, released, "Expected 1 page released (txn 30)")

	// txn 50 still pending - releasing up to 51 should release 1 more
	released = pm.ReleasePages(51)
	assert.Equal(t, 1, released, "Expected 1 page released (txn 50)")
}

func TestPageManagerFreeListEmptyRelease(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Release on empty pending should do nothing
	released := pm.ReleasePages(100)
	assert.Equal(t, 0, released, "Expected 0 pages released from empty pending")

	// Add empty slice shouldn't break anything
	require.NoError(t, pm.FreePending(10, []base.PageID{}), "FreePending with empty slice failed")

	// Should still be 0
	released = pm.ReleasePages(100)
	assert.Equal(t, 0, released, "Expected 0 pages released after empty FreePending")
}

func TestPageManagerFreeListPersistence(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"

	// Create Coordinator and add data
	{
		pm, cleanup := createTestCoordinator(t, tmpFile)

		// Add some free pages
		require.NoError(t, pm.FreePage(10), "FreePage failed")
		require.NoError(t, pm.FreePage(20), "FreePage failed")
		require.NoError(t, pm.FreePage(30), "FreePage failed")

		// Add pending pages
		pm.FreePending(100, []base.PageID{1000, 1001, 1002})
		pm.FreePending(101, []base.PageID{2000, 2001})
		pm.FreePending(105, []base.PageID{3000})

		cleanup()
	}

	// Reopen and verify
	{
		pm, cleanup := createTestCoordinator(t, tmpFile)
		defer cleanup()

		// Try to allocate - should get freed pages first
		allocated := make(map[base.PageID]bool)
		for i := 0; i < 3; i++ {
			id, err := pm.AssignPageID()
			require.NoError(t, err, "AssignPageID failed")
			allocated[id] = true
		}

		// Should have allocated the freed pages 10, 20, 30
		assert.True(t, allocated[10] || allocated[20] || allocated[30], "Expected to allocate freed pages after reopening")
	}
}

func TestPageManagerAllocateAndFree(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Allocate some pages
	id1, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	id2, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	id3, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")

	// Free them
	require.NoError(t, pm.FreePage(id1), "FreePage failed")
	require.NoError(t, pm.FreePage(id2), "FreePage failed")
	require.NoError(t, pm.FreePage(id3), "FreePage failed")

	// Allocate again - should reuse freed pages
	reused1, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	reused2, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	reused3, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")

	// Verify reused pages match freed pages
	reused := map[base.PageID]bool{reused1: true, reused2: true, reused3: true}
	freed := map[base.PageID]bool{id1: true, id2: true, id3: true}

	for id := range freed {
		assert.True(t, reused[id], "Expected freed page %d to be reused", id)
	}
}

func TestPageManagerPreventAllocation(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestCoordinator(t, tmpFile)
	defer cleanup()

	// Allocate all initial free pages to empty the freelist
	id1, _ := pm.AssignPageID() // page 3
	id2, _ := pm.AssignPageID() // page 4
	id3, _ := pm.AssignPageID() // page 5

	// Add pending pages
	pm.FreePending(10, []base.PageID{100, 101})
	pm.FreePending(20, []base.PageID{200, 201})

	// Prevent allocation up to txn 15
	pm.PreventAllocationUpTo(15)

	// Try to allocate - freelist is empty, pending has pages from txn <= 15
	// Should allocate NEW page instead of releasing pending
	id, err := pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	assert.False(t, id == 100 || id == 101, "Expected new page due to prevention, got pending page %d", id)

	// Clear prevention
	pm.AllowAllAllocations()

	// Now pending pages can be used - but freelist is still empty
	// So next allocation should return 0 (no free pages)
	id, err = pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	assert.NotEqual(t, 0, id, "Expected to allocate new page after clearing prevention")

	// Release the pending pages
	released := pm.ReleasePages(25)
	assert.Equal(t, 4, released, "Expected 4 pages released")

	// Now should be able to allocate freed pages
	id, err = pm.AssignPageID()
	require.NoError(t, err, "AssignPageID failed")
	freed := map[base.PageID]bool{100: true, 101: true, 200: true, 201: true}
	assert.True(t, freed[id], "Expected to allocate freed page (100/101/200/201), got %d", id)

	// Return allocated pages for cleanup
	pm.FreePage(id1)
	pm.FreePage(id2)
	pm.FreePage(id3)
}

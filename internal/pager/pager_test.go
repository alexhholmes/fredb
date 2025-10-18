package pager

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

// Helper to create a pager with dependencies for testing
func createTestPager(t *testing.T, tmpFile string) (*Pager, func()) {
	stor, err := storage.NewDirectIO(tmpFile)
	require.NoError(t, err, "Failed to create store")

	cacheInstance := cache.NewCache(1024)

	pm, err := NewPager(stor, cacheInstance)
	require.NoError(t, err, "Failed to create Pager")

	cleanup := func() {
		_ = pm.Close()
	}

	return pm, cleanup
}

func TestPageManagerFreeListEmptyRelease(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestPager(t, tmpFile)
	defer cleanup()

	// Release on empty pending should do nothing
	released := pm.ReleasePages(100)
	assert.Equal(t, 0, released, "Expected 0 pages released from empty pending")

	// Should still be 0
	released = pm.ReleasePages(100)
	assert.Equal(t, 0, released, "Expected 0 pages released after empty FreePending")
}

func TestPageManagerFreeListPersistence(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"

	// Create Pager and add data
	{
		pm, cleanup := createTestPager(t, tmpFile)

		// Add some free pages
		require.NoError(t, pm.FreePage(10), "FreePage failed")
		require.NoError(t, pm.FreePage(20), "FreePage failed")
		require.NoError(t, pm.FreePage(30), "FreePage failed")

		cleanup()
	}

	// Reopen and verify
	{
		pm, cleanup := createTestPager(t, tmpFile)
		defer cleanup()

		// Try to allocate - should get freed pages first
		allocated := make(map[base.PageID]bool)
		for i := 0; i < 3; i++ {
			id := pm.AssignPage()
			allocated[id] = true
		}

		// Should have allocated the freed pages 10, 20, 30
		assert.True(t, allocated[10] || allocated[20] || allocated[30], "Expected to allocate freed pages after reopening")
	}
}

func TestPageManagerAllocateAndFree(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestPager(t, tmpFile)
	defer cleanup()

	// Allocate some pages
	id1 := pm.AssignPage()
	id2 := pm.AssignPage()
	id3 := pm.AssignPage()

	// Free them
	require.NoError(t, pm.FreePage(id1), "FreePage failed")
	require.NoError(t, pm.FreePage(id2), "FreePage failed")
	require.NoError(t, pm.FreePage(id3), "FreePage failed")

	// Allocate again - should reuse freed pages
	reused1 := pm.AssignPage()
	reused2 := pm.AssignPage()
	reused3 := pm.AssignPage()

	// Verify reused pages match freed pages
	reused := map[base.PageID]bool{reused1: true, reused2: true, reused3: true}
	freed := map[base.PageID]bool{id1: true, id2: true, id3: true}

	for id := range freed {
		assert.True(t, reused[id], "Expected freed page %d to be reused", id)
	}
}

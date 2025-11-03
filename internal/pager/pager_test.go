package pager

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/cache"
	"github.com/alexhholmes/fredb/internal/storage"
)

var _ = flag.Bool("slow", false, "run slow tests")

// Helper to create a pager with dependencies for testing
func createTestPager(t *testing.T, tmpFile string) (*Pager, func()) {
	store, err := storage.New(tmpFile)
	require.NoError(t, err, "Failed to create store")

	cacheInstance := cache.NewCache(1024, nil)

	pm, err := NewPager(SyncEveryCommit, store, cacheInstance)
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
	released := pm.Release(100)
	assert.Equal(t, 0, released, "Expected 0 pages released from empty pending")

	// Should still be 0
	released = pm.Release(100)
	assert.Equal(t, 0, released, "Expected 0 pages released after empty FreePending")
}

func TestPageManagerAllocateAndFree(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, cleanup := createTestPager(t, tmpFile)
	defer cleanup()

	// Allocate some pages
	id1 := pm.Allocate(1)
	id2 := pm.Allocate(1)
	id3 := pm.Allocate(1)

	// Free them
	pm.Free(id1)
	pm.Free(id2)
	pm.Free(id3)

	// Allocate again - should reuse freed pages
	reused1 := pm.Allocate(1)
	reused2 := pm.Allocate(1)
	reused3 := pm.Allocate(1)

	// Verify reused pages match freed pages
	reused := map[base.PageID]bool{reused1: true, reused2: true, reused3: true}
	freed := map[base.PageID]bool{id1: true, id2: true, id3: true}

	for id := range freed {
		assert.True(t, reused[id], "Expected freed page %d to be reused", id)
	}
}

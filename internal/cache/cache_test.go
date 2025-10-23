package cache

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/alexhholmes/fredb/internal/base"
)

var _ = flag.Bool("slow", false, "run slow tests")

// Helper to create a test node (LeafPage)
func makeTestNode(pageID base.PageID) base.PageData {
	leaf := base.NewLeafPage()
	leaf.SetPageID(pageID)
	leaf.Header.NumKeys = 0
	leaf.Keys = make([][]byte, 0)
	leaf.Values = make([][]byte, 0)
	return leaf
}

func TestPageCacheBasics(t *testing.T) {
	t.Parallel()

	cache := NewCache(10, nil)

	// Test cache miss
	_, hit := cache.Get(base.PageID(1))
	assert.False(t, hit, "Expected cache miss for Page 1")

	// Add Node to cache
	node1 := makeTestNode(base.PageID(1))
	cache.Put(base.PageID(1), node1)

	// Should now hit
	retrieved, hit := cache.Get(base.PageID(1))
	assert.True(t, hit, "Expected cache hit for Page 1")
	assert.Equal(t, node1.GetPageID(), retrieved.GetPageID(), "Retrieved wrong Node")

	// Check size
	assert.Equal(t, 1, cache.Size())

	// Check stats
	stats := cache.Stats()
	hits, misses := stats.Hits, stats.Misses
	assert.Equal(t, uint64(1), hits)
	assert.Equal(t, uint64(1), misses)
}

func TestPageCacheReplacement(t *testing.T) {
	t.Parallel()

	cache := NewCache(10, nil)

	// Add Page 1
	node1 := makeTestNode(base.PageID(1))
	leaf1 := node1.(*base.LeafPage)
	leaf1.Header.NumKeys = 0
	cache.Put(base.PageID(1), node1)

	// Should retrieve the first version
	retrieved, hit := cache.Get(base.PageID(1))
	assert.True(t, hit, "Expected cache hit")
	assert.Equal(t, uint16(0), retrieved.GetNumKeys(), "Expected NumKeys=0")

	// Replace Page 1 with new version
	node2 := makeTestNode(base.PageID(1))
	leaf2 := node2.(*base.LeafPage)
	leaf2.Header.NumKeys = 5 // Different data
	cache.Put(base.PageID(1), node2)

	// Should now see the new version
	retrieved, hit = cache.Get(base.PageID(1))
	assert.True(t, hit, "Expected cache hit")
	assert.Equal(t, uint16(5), retrieved.GetNumKeys(), "Expected NumKeys=5")

	// Size should still be 1 (replaced, not added)
	assert.Equal(t, 1, cache.Size())
}

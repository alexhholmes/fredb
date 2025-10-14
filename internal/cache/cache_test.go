package cache

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"

	"fredb/internal/base"
	"fredb/internal/storage"
)

var _ = flag.Bool("slow", false, "run slow tests")

// Helper to create a test Node
func makeTestNode(pageID base.PageID) *base.Node {
	return &base.Node{
		PageID:   pageID,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: make([]base.PageID, 0),
	}
}

func TestPageCacheBasics(t *testing.T) {
	t.Parallel()

	cache := NewCache(10, storage.DirectIO)

	// Test cache miss
	_, hit := cache.Get(base.PageID(1))
	assert.False(t, hit, "Expected cache miss for Page 1")

	// Add Node to cache
	node1 := makeTestNode(base.PageID(1))
	cache.Put(base.PageID(1), node1)

	// Should now hit
	retrieved, hit := cache.Get(base.PageID(1))
	assert.True(t, hit, "Expected cache hit for Page 1")
	assert.Equal(t, node1.PageID, retrieved.PageID, "Retrieved wrong Node")

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

	cache := NewCache(10, storage.DirectIO)

	// Add Page 1
	node1 := makeTestNode(base.PageID(1))
	node1.NumKeys = 0
	cache.Put(base.PageID(1), node1)

	// Should retrieve the first version
	retrieved, hit := cache.Get(base.PageID(1))
	assert.True(t, hit, "Expected cache hit")
	assert.Equal(t, uint16(0), retrieved.NumKeys, "Expected NumKeys=0")

	// Replace Page 1 with new version
	node2 := makeTestNode(base.PageID(1))
	node2.NumKeys = 5 // Different data
	cache.Put(base.PageID(1), node2)

	// Should now see the new version
	retrieved, hit = cache.Get(base.PageID(1))
	assert.True(t, hit, "Expected cache hit")
	assert.Equal(t, uint16(5), retrieved.NumKeys, "Expected NumKeys=5")

	// Size should still be 1 (replaced, not added)
	assert.Equal(t, 1, cache.Size())
}

func TestPageCacheMinSize(t *testing.T) {
	t.Parallel()

	// Request size too small
	cache := NewCache(5, storage.DirectIO)
	assert.Equal(t, MinCacheSize, cache.maxSize)
}

func TestPageCacheEviction(t *testing.T) {
	t.Parallel()

	cache := NewCache(20, storage.DirectIO) // max=20, lowWater=16

	// Add 19 pages
	for i := 1; i <= 19; i++ {
		cache.Put(base.PageID(i), makeTestNode(base.PageID(i)))
	}

	assert.Equal(t, 19, cache.Size())

	// Add 20th Page - triggers eviction
	cache.Put(base.PageID(20), makeTestNode(base.PageID(20)))

	// Should be at 16 now
	size := cache.Size()
	assert.Equal(t, 16, size, "Expected size 16 after eviction")

	// Check eviction stats (evicted 4: pages 1-4)
	assert.Equal(t, uint64(4), cache.Stats().Evictions)

	// Pages 1-4 (LRU) should be evicted
	for i := 1; i <= 4; i++ {
		_, hit := cache.Get(base.PageID(i))
		assert.False(t, hit, "Page %d should have been evicted", i)
	}

	// Page 20 (newest) should still be there
	_, hit := cache.Get(base.PageID(20))
	assert.True(t, hit, "Page 20 should still be in cache")
}

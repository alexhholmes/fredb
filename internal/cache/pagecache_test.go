package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fredb/internal/base"
)

// Helper to create a test Node
func makeTestNode(pageID base.PageID) *base.Node {
	return &base.Node{
		PageID:   pageID,
		IsLeaf:   true,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: make([]base.PageID, 0),
	}
}

func TestPageCacheBasics(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10, nil)

	// Test cache miss
	_, hit := cache.get(base.PageID(1), 0)
	assert.False(t, hit, "Expected cache miss for Page 1")

	// Add Node to cache with txnID=1
	node1 := makeTestNode(base.PageID(1))
	cache.Put(base.PageID(1), 1, node1)

	// Should now hit (for txnID >= 1)
	retrieved, hit := cache.get(base.PageID(1), 1)
	assert.True(t, hit, "Expected cache hit for Page 1")
	assert.Equal(t, node1.PageID, retrieved.PageID, "Retrieved wrong Node")

	// Check size
	assert.Equal(t, 1, cache.Size())

	// Check stats
	hits, misses, _ := cache.Stats()
	assert.Equal(t, uint64(1), hits)
	assert.Equal(t, uint64(1), misses)
}

func TestPageCacheMVCC(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10, nil)

	// Add version 1 of Page 1
	node1 := makeTestNode(base.PageID(1))
	cache.Put(base.PageID(1), 1, node1)

	// Add version 2 of Page 1
	node2 := makeTestNode(base.PageID(1))
	node2.NumKeys = 5 // Different data
	cache.Put(base.PageID(1), 2, node2)

	// Reader at txnID=1 should see version 1
	retrieved, hit := cache.get(base.PageID(1), 1)
	assert.True(t, hit, "Expected cache hit for txnID=1")
	assert.Equal(t, uint16(0), retrieved.NumKeys, "Expected version 1 (NumKeys=0)")

	// Reader at txnID=2 should see version 2
	retrieved, hit = cache.get(base.PageID(1), 2)
	assert.True(t, hit, "Expected cache hit for txnID=2")
	assert.Equal(t, uint16(5), retrieved.NumKeys, "Expected version 2 (NumKeys=5)")

	// Reader at txnID=3 should see version 2 (latest committed)
	retrieved, hit = cache.get(base.PageID(1), 3)
	assert.True(t, hit, "Expected cache hit for txnID=3")
	assert.Equal(t, uint16(5), retrieved.NumKeys, "Expected version 2 (NumKeys=5)")

	// size should be 2 (two versions)
	assert.Equal(t, 2, cache.Size())
}

func TestPageCacheMinSize(t *testing.T) {
	t.Parallel()

	// Request size too small
	cache := NewPageCache(5, nil)
	assert.Equal(t, MinCacheSize, cache.maxSize)
}

func TestPageCacheEviction(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(20, nil) // max=20, lowWater=16

	// Add 19 pages
	for i := 1; i <= 19; i++ {
		cache.Put(base.PageID(i), uint64(i), makeTestNode(base.PageID(i)))
	}

	assert.Equal(t, 19, cache.Size())

	// Add 20th Page - triggers eviction
	cache.Put(base.PageID(20), 20, makeTestNode(base.PageID(20)))

	// Should be at 16 now
	size := cache.Size()
	assert.Equal(t, 16, size, "Expected size 16 after eviction")

	// Check eviction stats (evicted 4: pages 1-4)
	_, _, evictions := cache.Stats()
	assert.Equal(t, uint64(4), evictions)

	// Pages 1-4 (LRU) should be evicted
	for i := 1; i <= 4; i++ {
		_, hit := cache.get(base.PageID(i), uint64(i))
		assert.False(t, hit, "Page %d should have been evicted", i)
	}

	// Page 20 (newest) should still be there
	_, hit := cache.get(base.PageID(20), 20)
	assert.True(t, hit, "Page 20 should still be in cache")
}

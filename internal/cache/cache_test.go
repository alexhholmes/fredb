package cache

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"

	"fredb/internal/base"
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

	cache := NewCache(10)

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

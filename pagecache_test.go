package fredb

import (
	"testing"

	"fredb/internal"
)

// Helper to create a test Node
func makeTestNode(pageID internal.PageID) *internal.Node {
	return &internal.Node{
		PageID:   pageID,
		IsLeaf:   true,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: make([]internal.PageID, 0),
	}
}

func TestPageCacheBasics(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10, nil)

	// Test cache miss
	_, hit := cache.get(internal.PageID(1), 0)
	if hit {
		t.Error("Expected cache miss for Page 1")
	}

	// Add Node to cache with txnID=1
	node1 := makeTestNode(internal.PageID(1))
	cache.Put(internal.PageID(1), 1, node1)

	// Should now hit (for txnID >= 1)
	retrieved, hit := cache.get(internal.PageID(1), 1)
	if !hit {
		t.Error("Expected cache hit for Page 1")
	}
	if retrieved.PageID != node1.PageID {
		t.Error("Retrieved wrong Node")
	}

	// Check size
	if cache.size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.size())
	}

	// Check stats
	hits, misses, _ := cache.stats()
	if hits != 1 {
		t.Errorf("Expected 1 hit, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}
}

func TestPageCacheMVCC(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10, nil)

	// Add version 1 of Page 1
	node1 := makeTestNode(internal.PageID(1))
	cache.Put(internal.PageID(1), 1, node1)

	// Add version 2 of Page 1
	node2 := makeTestNode(internal.PageID(1))
	node2.NumKeys = 5 // Different data
	cache.Put(internal.PageID(1), 2, node2)

	// Reader at txnID=1 should see version 1
	retrieved, hit := cache.get(internal.PageID(1), 1)
	if !hit {
		t.Error("Expected cache hit for txnID=1")
	}
	if retrieved.NumKeys != 0 {
		t.Error("Expected version 1 (NumKeys=0)")
	}

	// Reader at txnID=2 should see version 2
	retrieved, hit = cache.get(internal.PageID(1), 2)
	if !hit {
		t.Error("Expected cache hit for txnID=2")
	}
	if retrieved.NumKeys != 5 {
		t.Error("Expected version 2 (NumKeys=5)")
	}

	// Reader at txnID=3 should see version 2 (latest committed)
	retrieved, hit = cache.get(internal.PageID(1), 3)
	if !hit {
		t.Error("Expected cache hit for txnID=3")
	}
	if retrieved.NumKeys != 5 {
		t.Error("Expected version 2 (NumKeys=5)")
	}

	// size should be 2 (two versions)
	if cache.size() != 2 {
		t.Errorf("Expected size 2, got %d", cache.size())
	}
}

func TestPageCacheMinSize(t *testing.T) {
	t.Parallel()

	// Request size too small
	cache := NewPageCache(5, nil)
	if cache.maxSize != MinCacheSize {
		t.Errorf("Expected min size %d, got %d", MinCacheSize, cache.maxSize)
	}
}

func TestPageCacheMaxSize(t *testing.T) {
	t.Parallel()

	// Request size too large
	cache := NewPageCache(500000000, nil)
	if cache.maxSize != MaxCacheSize {
		t.Errorf("Expected max size %d, got %d", MaxCacheSize, cache.maxSize)
	}
}

func TestPageCacheEviction(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(20, nil) // max=20, lowWater=16

	// Add 19 pages
	for i := 1; i <= 19; i++ {
		cache.Put(internal.PageID(i), uint64(i), makeTestNode(internal.PageID(i)))
	}

	if cache.size() != 19 {
		t.Errorf("Expected size 19, got %d", cache.size())
	}

	// Add 20th Page - triggers eviction
	cache.Put(internal.PageID(20), 20, makeTestNode(internal.PageID(20)))

	// Should be at 16 now
	size := cache.size()
	if size != 16 {
		t.Errorf("Expected size 16 after eviction, got %d", size)
	}

	// Check eviction stats (evicted 4: pages 1-4)
	_, _, evictions := cache.stats()
	if evictions != 4 {
		t.Errorf("Expected 4 evictions, got %d", evictions)
	}

	// Pages 1-4 (LRU) should be evicted
	for i := 1; i <= 4; i++ {
		if _, hit := cache.get(internal.PageID(i), uint64(i)); hit {
			t.Errorf("Page %d should have been evicted", i)
		}
	}

	// Page 20 (newest) should still be there
	if _, hit := cache.get(internal.PageID(20), 20); !hit {
		t.Error("Page 20 should still be in cache")
	}
}

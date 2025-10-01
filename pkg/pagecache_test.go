package pkg

import (
	"testing"
)

// Helper to create a test node
func makeTestNode(pageID PageID) *Node {
	return &Node{
		pageID:   pageID,
		page:     &Page{},
		isLeaf:   true,
		keys:     make([][]byte, 0),
		values:   make([][]byte, 0),
		children: make([]PageID, 0),
	}
}

func TestPageCacheBasics(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10, nil)

	// Test cache miss
	_, hit := cache.Get(PageID(1), 0)
	if hit {
		t.Error("Expected cache miss for page 1")
	}

	// Add node to cache with txnID=1
	node1 := makeTestNode(PageID(1))
	cache.Put(PageID(1), 1, node1)

	// Should now hit (for txnID >= 1)
	retrieved, hit := cache.Get(PageID(1), 1)
	if !hit {
		t.Error("Expected cache hit for page 1")
	}
	if retrieved.pageID != node1.pageID {
		t.Error("Retrieved wrong node")
	}

	// Check size
	if cache.Size() != 1 {
		t.Errorf("Expected size 1, got %d", cache.Size())
	}

	// Check stats
	hits, misses, _ := cache.Stats()
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

	// Add version 1 of page 1
	node1 := makeTestNode(PageID(1))
	cache.Put(PageID(1), 1, node1)

	// Add version 2 of page 1
	node2 := makeTestNode(PageID(1))
	node2.numKeys = 5 // Different data
	cache.Put(PageID(1), 2, node2)

	// Reader at txnID=1 should see version 1
	retrieved, hit := cache.Get(PageID(1), 1)
	if !hit {
		t.Error("Expected cache hit for txnID=1")
	}
	if retrieved.numKeys != 0 {
		t.Error("Expected version 1 (numKeys=0)")
	}

	// Reader at txnID=2 should see version 2
	retrieved, hit = cache.Get(PageID(1), 2)
	if !hit {
		t.Error("Expected cache hit for txnID=2")
	}
	if retrieved.numKeys != 5 {
		t.Error("Expected version 2 (numKeys=5)")
	}

	// Reader at txnID=3 should see version 2 (latest committed)
	retrieved, hit = cache.Get(PageID(1), 3)
	if !hit {
		t.Error("Expected cache hit for txnID=3")
	}
	if retrieved.numKeys != 5 {
		t.Error("Expected version 2 (numKeys=5)")
	}

	// Size should be 2 (two versions)
	if cache.Size() != 2 {
		t.Errorf("Expected size 2, got %d", cache.Size())
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
	cache := NewPageCache(100000, nil)
	if cache.maxSize != MaxCacheSize {
		t.Errorf("Expected max size %d, got %d", MaxCacheSize, cache.maxSize)
	}
}

func TestPageCacheEviction(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(20, nil) // max=20, lowWater=16

	// Add 19 pages
	for i := 1; i <= 19; i++ {
		cache.Put(PageID(i), uint64(i), makeTestNode(PageID(i)))
	}

	if cache.Size() != 19 {
		t.Errorf("Expected size 19, got %d", cache.Size())
	}

	// Add 20th page - triggers eviction
	cache.Put(PageID(20), 20, makeTestNode(PageID(20)))

	// Should be at 16 now
	size := cache.Size()
	if size != 16 {
		t.Errorf("Expected size 16 after eviction, got %d", size)
	}

	// Check eviction stats (evicted 4: pages 1-4)
	_, _, evictions := cache.Stats()
	if evictions != 4 {
		t.Errorf("Expected 4 evictions, got %d", evictions)
	}

	// Pages 1-4 (LRU) should be evicted
	for i := 1; i <= 4; i++ {
		if _, hit := cache.Get(PageID(i), uint64(i)); hit {
			t.Errorf("Page %d should have been evicted", i)
		}
	}

	// Page 20 (newest) should still be there
	if _, hit := cache.Get(PageID(20), 20); !hit {
		t.Error("Page 20 should still be in cache")
	}
}
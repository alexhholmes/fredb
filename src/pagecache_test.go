package src

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

	cache := NewPageCache(10)

	// Test cache miss
	_, hit := cache.Get(PageID(1))
	if hit {
		t.Error("Expected cache miss for page 1")
	}

	// Add node to cache (puts it pinned)
	node1 := makeTestNode(PageID(1))
	cache.Put(PageID(1), node1)

	// Should now hit
	retrieved, hit := cache.Get(PageID(1))
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

func TestPageCachePinUnpin(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10)

	node1 := makeTestNode(PageID(1))
	cache.Put(PageID(1), node1) // pinCount = 1

	// Get increments pin
	cache.Get(PageID(1)) // pinCount = 2

	// Unpin twice (should not panic)
	cache.Unpin(PageID(1)) // pinCount = 1
	cache.Unpin(PageID(1)) // pinCount = 0

	// Unpin non-existent page (should not panic)
	cache.Unpin(PageID(999))
}

func TestPageCacheDoublePinPanic(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10)

	node1 := makeTestNode(PageID(1))
	cache.Put(PageID(1), node1) // pinCount = 1

	// Unpin once
	cache.Unpin(PageID(1)) // pinCount = 0

	// Second unpin should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on double unpin")
		}
	}()
	cache.Unpin(PageID(1)) // Should panic
}

func TestPageCachePutExisting(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(10)

	node1 := makeTestNode(PageID(1))
	cache.Put(PageID(1), node1) // pinCount = 1

	// Put again (should increment pin, not add duplicate)
	cache.Put(PageID(1), node1) // pinCount = 2

	if cache.Size() != 1 {
		t.Errorf("Expected size 1 after duplicate Put, got %d", cache.Size())
	}

	// Unpin twice
	cache.Unpin(PageID(1))
	cache.Unpin(PageID(1))
}

func TestPageCacheLRUOrder(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(5)

	// Add pages 1, 2, 3
	cache.Put(PageID(1), makeTestNode(PageID(1)))
	cache.Unpin(PageID(1)) // Unpin so it's evictable

	cache.Put(PageID(2), makeTestNode(PageID(2)))
	cache.Unpin(PageID(2))

	cache.Put(PageID(3), makeTestNode(PageID(3)))
	cache.Unpin(PageID(3))

	// Access page 1 (should move to front of LRU)
	cache.Get(PageID(1))
	cache.Unpin(PageID(1))

	// Now LRU order: 1 (MRU), 3, 2 (LRU)
	// Verify all present
	if cache.Size() != 3 {
		t.Errorf("Expected size 3, got %d", cache.Size())
	}

	// Verify we can get all of them
	if _, hit := cache.Get(PageID(1)); !hit {
		t.Error("Page 1 should be in cache")
	}
	cache.Unpin(PageID(1))

	if _, hit := cache.Get(PageID(2)); !hit {
		t.Error("Page 2 should be in cache")
	}
	cache.Unpin(PageID(2))

	if _, hit := cache.Get(PageID(3)); !hit {
		t.Error("Page 3 should be in cache")
	}
	cache.Unpin(PageID(3))
}

func TestPageCacheMinSize(t *testing.T) {
	t.Parallel()

	// Request size too small
	cache := NewPageCache(5)
	if cache.maxSize != MinCacheSize {
		t.Errorf("Expected min size %d, got %d", MinCacheSize, cache.maxSize)
	}
}

func TestPageCacheMaxSize(t *testing.T) {
	t.Parallel()

	// Request size too large
	cache := NewPageCache(100000)
	if cache.maxSize != MaxCacheSize {
		t.Errorf("Expected max size %d, got %d", MaxCacheSize, cache.maxSize)
	}
}

func TestPageCacheConcurrentAccess(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(100)

	// Add some nodes
	for i := 1; i <= 10; i++ {
		cache.Put(PageID(i), makeTestNode(PageID(i)))
		cache.Unpin(PageID(i))
	}

	// Concurrent gets (should not race)
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 1; j <= 10; j++ {
				node, hit := cache.Get(PageID(j))
				if hit {
					cache.Unpin(PageID(j))
					_ = node
				}
			}
			done <- true
		}(i)
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestPageCacheEviction(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(20) // max=20, lowWater=16

	// Add 19 pages
	for i := 1; i <= 19; i++ {
		cache.Put(PageID(i), makeTestNode(PageID(i)))
		cache.Unpin(PageID(i)) // Unpin so they're evictable
	}

	if cache.Size() != 19 {
		t.Errorf("Expected size 19, got %d", cache.Size())
	}

	// Add 20th page - triggers eviction (size goes to 20 which >= maxSize)
	// During eviction, page 20 is pinned, so can't evict it
	// Evicts oldest unpinned pages (1-4) to get from 20 down to 16
	cache.Put(PageID(20), makeTestNode(PageID(20)))
	cache.Unpin(PageID(20))

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
		if _, hit := cache.Get(PageID(i)); hit {
			t.Errorf("Page %d should have been evicted", i)
		}
	}

	// Page 20 (newest) should still be there
	if _, hit := cache.Get(PageID(20)); !hit {
		t.Error("Page 20 should still be in cache")
	}
	cache.Unpin(PageID(20))
}

func TestPageCacheEvictPinnedSkipped(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(20) // max=20, lowWater=16

	// Add 18 pages, unpin all
	for i := 1; i <= 18; i++ {
		cache.Put(PageID(i), makeTestNode(PageID(i)))
		cache.Unpin(PageID(i))
	}

	// Add page 19, keep it pinned
	cache.Put(PageID(19), makeTestNode(PageID(19)))
	// Don't unpin page 19 - it stays pinned

	if cache.Size() != 19 {
		t.Errorf("Expected size 19, got %d", cache.Size())
	}

	// Add 20th page - triggers eviction (size=20 >= maxSize)
	// Page 19 is pinned, page 20 becomes pinned during Put
	// Can evict pages 1-18
	cache.Put(PageID(20), makeTestNode(PageID(20)))
	cache.Unpin(PageID(20))

	// Evicts 4 pages (1-4) to reach 16
	// But page 19 still pinned, so actual size = 16
	size := cache.Size()
	if size != 16 {
		t.Errorf("Expected size 16 after eviction, got %d", size)
	}

	// Pinned page 19 should still be there
	if _, hit := cache.Get(PageID(19)); !hit {
		t.Error("Pinned page 19 should not be evicted")
	}
	cache.Unpin(PageID(19)) // Unpin our Get()
	cache.Unpin(PageID(19)) // Unpin original pin
}

func TestPageCacheEvictDirtySkipped(t *testing.T) {
	t.Parallel()

	cache := NewPageCache(20) // max=20, lowWater=16

	// Add 19 pages, mark first 3 dirty
	for i := 1; i <= 19; i++ {
		node := makeTestNode(PageID(i))
		if i <= 3 {
			node.dirty = true // Pages 1-3 dirty
		}
		cache.Put(PageID(i), node)
		cache.Unpin(PageID(i))
	}

	// Add 20th page - triggers eviction (size=20 >= maxSize)
	// During eviction: page 20 pinned, pages 1-3 dirty
	// Can evict clean pages 4-19
	// Evicts pages 4-7 (4 pages) to reach lowWater=16
	cache.Put(PageID(20), makeTestNode(PageID(20)))
	cache.Unpin(PageID(20))

	// Should evict 4 clean pages, keeping dirty pages 1-3
	size := cache.Size()
	if size != 16 {
		t.Errorf("Expected size 16 after eviction, got %d", size)
	}

	// Dirty pages 1-3 should still be there
	for i := 1; i <= 3; i++ {
		if _, hit := cache.Get(PageID(i)); !hit {
			t.Errorf("Dirty page %d should not be evicted", i)
		}
		cache.Unpin(PageID(i))
	}
}

package storage

import (
	"testing"

	"fredb/internal/base"
)

func TestPageManagerFreeListPending(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, err := NewPageManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}
	defer pm.Close()

	// Add some pages to pending at different transactions
	if err := pm.FreePending(10, []base.PageID{100, 101, 102}); err != nil {
		t.Fatalf("FreePending failed: %v", err)
	}
	if err := pm.FreePending(11, []base.PageID{200, 201}); err != nil {
		t.Fatalf("FreePending failed: %v", err)
	}
	if err := pm.FreePending(12, []base.PageID{300}); err != nil {
		t.Fatalf("FreePending failed: %v", err)
	}

	// Release pages from transactions < 11 (i.e., txnID 10)
	released := pm.ReleasePages(11)
	if released != 3 {
		t.Errorf("Expected 3 pages released, got %d", released)
	}

	// Release everything
	released = pm.ReleasePages(100)
	if released != 3 {
		t.Errorf("Expected 3 more pages released, got %d", released)
	}

	// Verify we can allocate the freed pages
	allocated := make(map[base.PageID]bool)
	for i := 0; i < 6; i++ {
		id, err := pm.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}
		// Pages 100, 101, 102, 200, 201, 300 should be reused
		if id == 100 || id == 101 || id == 102 || id == 200 || id == 201 || id == 300 {
			allocated[id] = true
		}
	}

	// Should have allocated at least some of the freed pages
	if len(allocated) == 0 {
		t.Error("Expected to reuse some freed pages, but none were allocated")
	}
}

func TestPageManagerFreeListReleaseOrder(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, err := NewPageManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}
	defer pm.Close()

	// Add pages at various transaction IDs
	pm.FreePending(50, []base.PageID{500})
	pm.FreePending(10, []base.PageID{100})
	pm.FreePending(30, []base.PageID{300})
	pm.FreePending(20, []base.PageID{200})

	// Release up to 25 should release txns 10 and 20
	released := pm.ReleasePages(25)
	if released != 2 {
		t.Errorf("Expected 2 pages released (txn 10, 20), got %d", released)
	}

	// Release pages from transactions < 31 (i.e., txnID 30)
	released = pm.ReleasePages(31)
	if released != 1 {
		t.Errorf("Expected 1 page released (txn 30), got %d", released)
	}

	// txn 50 still pending - releasing up to 51 should release 1 more
	released = pm.ReleasePages(51)
	if released != 1 {
		t.Errorf("Expected 1 page released (txn 50), got %d", released)
	}
}

func TestPageManagerFreeListEmptyRelease(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, err := NewPageManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}
	defer pm.Close()

	// Release on empty pending should do nothing
	released := pm.ReleasePages(100)
	if released != 0 {
		t.Errorf("Expected 0 pages released from empty pending, got %d", released)
	}

	// Add empty slice shouldn't break anything
	if err := pm.FreePending(10, []base.PageID{}); err != nil {
		t.Fatalf("FreePending with empty slice failed: %v", err)
	}

	// Should still be 0
	released = pm.ReleasePages(100)
	if released != 0 {
		t.Errorf("Expected 0 pages released after empty FreePending, got %d", released)
	}
}

func TestPageManagerFreeListPersistence(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"

	// Create PageManager and add data
	{
		pm, err := NewPageManager(tmpFile)
		if err != nil {
			t.Fatalf("Failed to create PageManager: %v", err)
		}

		// Add some free pages
		if err := pm.FreePage(10); err != nil {
			t.Fatalf("FreePage failed: %v", err)
		}
		if err := pm.FreePage(20); err != nil {
			t.Fatalf("FreePage failed: %v", err)
		}
		if err := pm.FreePage(30); err != nil {
			t.Fatalf("FreePage failed: %v", err)
		}

		// Add pending pages
		pm.FreePending(100, []base.PageID{1000, 1001, 1002})
		pm.FreePending(101, []base.PageID{2000, 2001})
		pm.FreePending(105, []base.PageID{3000})

		if err := pm.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	// Reopen and verify
	{
		pm, err := NewPageManager(tmpFile)
		if err != nil {
			t.Fatalf("Failed to reopen PageManager: %v", err)
		}
		defer pm.Close()

		// Try to allocate - should get freed pages first
		allocated := make(map[base.PageID]bool)
		for i := 0; i < 3; i++ {
			id, err := pm.AllocatePage()
			if err != nil {
				t.Fatalf("AllocatePage failed: %v", err)
			}
			allocated[id] = true
		}

		// Should have allocated the freed pages 10, 20, 30
		if !allocated[10] && !allocated[20] && !allocated[30] {
			t.Error("Expected to allocate freed pages after reopening")
		}
	}
}

func TestPageManagerAllocateAndFree(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, err := NewPageManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}
	defer pm.Close()

	// Allocate some pages
	id1, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	id2, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	id3, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	// Free them
	if err := pm.FreePage(id1); err != nil {
		t.Fatalf("FreePage failed: %v", err)
	}
	if err := pm.FreePage(id2); err != nil {
		t.Fatalf("FreePage failed: %v", err)
	}
	if err := pm.FreePage(id3); err != nil {
		t.Fatalf("FreePage failed: %v", err)
	}

	// Allocate again - should reuse freed pages
	reused1, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	reused2, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	reused3, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}

	// Verify reused pages match freed pages
	reused := map[base.PageID]bool{reused1: true, reused2: true, reused3: true}
	freed := map[base.PageID]bool{id1: true, id2: true, id3: true}

	for id := range freed {
		if !reused[id] {
			t.Errorf("Expected freed page %d to be reused", id)
		}
	}
}

func TestPageManagerPreventAllocation(t *testing.T) {
	t.Parallel()

	tmpFile := t.TempDir() + "/test.db"
	pm, err := NewPageManager(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create PageManager: %v", err)
	}
	defer pm.Close()

	// Allocate all initial free pages to empty the freelist
	id1, _ := pm.AllocatePage() // page 3
	id2, _ := pm.AllocatePage() // page 4
	id3, _ := pm.AllocatePage() // page 5

	// Add pending pages
	pm.FreePending(10, []base.PageID{100, 101})
	pm.FreePending(20, []base.PageID{200, 201})

	// Prevent allocation up to txn 15
	pm.PreventAllocationUpTo(15)

	// Try to allocate - freelist is empty, pending has pages from txn <= 15
	// Should allocate NEW page instead of releasing pending
	id, err := pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if id == 100 || id == 101 {
		t.Errorf("Expected new page due to prevention, got pending page %d", id)
	}

	// Clear prevention
	pm.AllowAllAllocations()

	// Now pending pages can be used - but freelist is still empty
	// So next allocation should return 0 (no free pages)
	id, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	if id == 0 {
		t.Error("Expected to allocate new page after clearing prevention")
	}

	// Release the pending pages
	released := pm.ReleasePages(25)
	if released != 4 {
		t.Errorf("Expected 4 pages released, got %d", released)
	}

	// Now should be able to allocate freed pages
	id, err = pm.AllocatePage()
	if err != nil {
		t.Fatalf("AllocatePage failed: %v", err)
	}
	freed := map[base.PageID]bool{100: true, 101: true, 200: true, 201: true}
	if !freed[id] {
		t.Errorf("Expected to allocate freed page (100/101/200/201), got %d", id)
	}

	// Return allocated pages for cleanup
	pm.FreePage(id1)
	pm.FreePage(id2)
	pm.FreePage(id3)
}

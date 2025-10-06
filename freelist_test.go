package fredb

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestFreeListPending(t *testing.T) {
	t.Parallel()

	fl := NewFreeList()

	// Add some pages to pending at different transactions
	fl.FreePending(10, []pageID{100, 101, 102})
	fl.FreePending(11, []pageID{200, 201})
	fl.FreePending(12, []pageID{300})

	// Verify pending size
	if fl.PendingSize() != 6 {
		t.Errorf("Expected 6 pages in pending, got %d", fl.PendingSize())
	}

	// Nothing in free list yet
	if fl.Size() != 0 {
		t.Errorf("Expected 0 pages in free list, got %d", fl.Size())
	}

	// Release pages from transactions < 11 (i.e., txnID 10)
	released := fl.Release(11)
	if released != 3 {
		t.Errorf("Expected 3 pages released, got %d", released)
	}

	// Check free list now has 3 pages
	if fl.Size() != 3 {
		t.Errorf("Expected 3 pages in free list after release, got %d", fl.Size())
	}

	// Pending should still have 3
	if fl.PendingSize() != 3 {
		t.Errorf("Expected 3 pages still pending, got %d", fl.PendingSize())
	}

	// Release everything
	released = fl.Release(100)
	if released != 3 {
		t.Errorf("Expected 3 more pages released, got %d", released)
	}

	// All pages should be free now
	if fl.Size() != 6 {
		t.Errorf("Expected 6 pages in free list after full release, got %d", fl.Size())
	}

	if fl.PendingSize() != 0 {
		t.Errorf("Expected 0 pages pending after full release, got %d", fl.PendingSize())
	}

	// Verify we can allocate the freed pages
	for i := 0; i < 6; i++ {
		id := fl.Allocate()
		if id == 0 {
			t.Errorf("Expected to allocate Page, got 0")
		}
	}

	// Should be empty now
	if fl.Allocate() != 0 {
		t.Error("Expected no more pages to allocate")
	}
}

func TestFreeListReleaseOrder(t *testing.T) {
	t.Parallel()

	fl := NewFreeList()

	// Add pages at various transaction IDs
	fl.FreePending(50, []pageID{500})
	fl.FreePending(10, []pageID{100})
	fl.FreePending(30, []pageID{300})
	fl.FreePending(20, []pageID{200})

	// Release up to 25 should release txns 10 and 20
	released := fl.Release(25)
	if released != 2 {
		t.Errorf("Expected 2 pages released (txn 10, 20), got %d", released)
	}

	// Release pages from transactions < 31 (i.e., txnID 30)
	released = fl.Release(31)
	if released != 1 {
		t.Errorf("Expected 1 Page released (txn 30), got %d", released)
	}

	// txn 50 still pending
	if fl.PendingSize() != 1 {
		t.Errorf("Expected 1 Page still pending (txn 50), got %d", fl.PendingSize())
	}
}

func TestFreeListEmptyRelease(t *testing.T) {
	t.Parallel()

	fl := NewFreeList()

	// Release on empty pending should do nothing
	released := fl.Release(100)
	if released != 0 {
		t.Errorf("Expected 0 pages released from empty pending, got %d", released)
	}

	// Add empty slice shouldn't break anything
	fl.FreePending(10, []pageID{})
	if fl.PendingSize() != 0 {
		t.Errorf("Expected 0 pages after adding empty slice, got %d", fl.PendingSize())
	}
}

func TestFreeListPendingSerialization(t *testing.T) {
	t.Parallel()

	fl := NewFreeList()

	// Add both free and pending pages
	fl.Free(10)
	fl.Free(20)
	fl.Free(30)

	fl.FreePending(100, []pageID{1000, 1001, 1002})
	fl.FreePending(101, []pageID{2000, 2001})
	fl.FreePending(105, []pageID{3000})

	// serialize
	pagesNeeded := fl.PagesNeeded()
	pages := make([]*Page, pagesNeeded)
	for i := 0; i < pagesNeeded; i++ {
		pages[i] = &Page{}
	}
	fl.Serialize(pages)

	// deserialize into new freelist
	fl2 := NewFreeList()
	fl2.Deserialize(pages)

	// Verify free IDs match
	if len(fl2.ids) != len(fl.ids) {
		t.Fatalf("Free IDs count mismatch: got %d, want %d", len(fl2.ids), len(fl.ids))
	}
	for i, id := range fl.ids {
		if fl2.ids[i] != id {
			t.Errorf("Free ID mismatch at %d: got %d, want %d", i, fl2.ids[i], id)
		}
	}

	// Verify pending entries match
	if len(fl2.pending) != len(fl.pending) {
		t.Fatalf("Pending count mismatch: got %d, want %d", len(fl2.pending), len(fl.pending))
	}

	for txnID, pageIDs := range fl.pending {
		pageIDs2, exists := fl2.pending[txnID]
		if !exists {
			t.Errorf("Missing pending entry for txnID %d", txnID)
			continue
		}
		if len(pageIDs2) != len(pageIDs) {
			t.Errorf("Pending pages count mismatch for txnID %d: got %d, want %d",
				txnID, len(pageIDs2), len(pageIDs))
			continue
		}
		for i, id := range pageIDs {
			if pageIDs2[i] != id {
				t.Errorf("Pending Page ID mismatch for txnID %d at %d: got %d, want %d",
					txnID, i, pageIDs2[i], id)
			}
		}
	}

	// Verify sizes match
	if fl2.Size() != fl.Size() {
		t.Errorf("Size mismatch: got %d, want %d", fl2.Size(), fl.Size())
	}
	if fl2.PendingSize() != fl.PendingSize() {
		t.Errorf("PendingSize mismatch: got %d, want %d", fl2.PendingSize(), fl.PendingSize())
	}
}

func TestFreelistNoPageLeaks(t *testing.T) {
	if !*slow {
		t.Skip("Skipping slow test; use -slow to enable")
	}

	// Test that pages are properly reclaimed after delete operations
	// with concurrent readers active
	db := setupTestDB(t)

	// Track initial pages
	initialPages := db.store.pager.GetMeta().NumPages

	// Start concurrent readers - they will take snapshots at various points
	var wg sync.WaitGroup
	stopReaders := make(chan struct{})

	// Launch 3 concurrent readers that continuously read random keys
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					// Begin read transaction
					tx, err := db.Begin(false)
					if err != nil {
						continue
					}

					// Read a few random keys
					for j := 0; j < 10; j++ {
						key := []byte(fmt.Sprintf("key%d", j))
						tx.Get(key) // Ignore errors, some keys may not exist
					}

					// Hold transaction briefly to simulate real reader
					time.Sleep(time.Millisecond)

					// Commit/rollback read transaction
					tx.Rollback()
				}
			}
		}(i)
	}

	// Run 10000 insert/delete cycles
	for cycle := 0; cycle < 10000; cycle++ {
		// Begin write transaction
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatalf("Failed to begin write tx at cycle %d: %v", cycle, err)
		}

		// Insert a key
		key := []byte(fmt.Sprintf("cycle%d", cycle))
		value := []byte(fmt.Sprintf("value%d", cycle))
		if err := tx.Set(key, value); err != nil {
			t.Fatalf("Failed to insert at cycle %d: %v", cycle, err)
		}

		// Commit insert
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit insert at cycle %d: %v", cycle, err)
		}

		// Begin another write transaction
		tx, err = db.Begin(true)
		if err != nil {
			t.Fatalf("Failed to begin delete tx at cycle %d: %v", cycle, err)
		}

		// Delete the key
		if err := tx.Delete(key); err != nil {
			t.Fatalf("Failed to delete at cycle %d: %v", cycle, err)
		}

		// Commit delete
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit delete at cycle %d: %v", cycle, err)
		}

		// Periodically let background releaser run
		if cycle%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Stop readers
	close(stopReaders)
	wg.Wait()

	// Give background releaser time to process final releases
	time.Sleep(100 * time.Millisecond)

	// Check final Page count
	finalPages := db.store.pager.GetMeta().NumPages
	pageGrowth := finalPages - initialPages

	// With proper Page reclamation, growth should be minimal
	// Allow for some growth due to B-tree structure, but not linear with operations
	maxExpectedGrowth := uint64(400) // Generous allowance for tree overhead and MVCC
	if pageGrowth > maxExpectedGrowth {
		t.Errorf("Excessive Page growth detected: grew by %d pages (from %d to %d), expected <= %d",
			pageGrowth, initialPages, finalPages, maxExpectedGrowth)
	}

	// Also active that freelist size is reasonable
	if dm, ok := db.store.pager.(*DiskPageManager); ok {
		dm.mu.Lock()
		freeSize := dm.freelist.Size()
		pendingSize := dm.freelist.PendingSize()
		dm.mu.Unlock()

		// After all operations complete and readers finish,
		// pending should be empty and free list should contain reclaimed pages
		if pendingSize > 10 {
			t.Errorf("Pending list still has %d pages after operations complete", pendingSize)
		}

		t.Logf("Test complete: Pages grew from %d to %d (growth: %d), Free: %d, Pending: %d",
			initialPages, finalPages, pageGrowth, freeSize, pendingSize)
	}
}

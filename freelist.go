package fredb

import (
	"fmt"
	"unsafe"
)

const (
	// FreeListPageCapacity is max number of PageIDs per freelist page
	// (PageSize - 8 bytes for count) / 8 bytes per PageID
	FreeListPageCapacity = (PageSize - 8) / 8

	// PendingMarker indicates transition from free IDs to pending entries
	PendingMarker = PageID(0xFFFFFFFFFFFFFFFF)
)

// FreeList tracks free pages for reuse
type FreeList struct {
	ids              []PageID            // sorted array of free page IDs
	pending          map[uint64][]PageID // txnID -> pages freed at that transaction
	preventUpToTxnID uint64              // temporarily prevent allocation of pages freed up to this txnID (0 = no prevention)
}

// NewFreeList creates an empty freelist
func NewFreeList() *FreeList {
	return &FreeList{
		ids:     make([]PageID, 0),
		pending: make(map[uint64][]PageID),
	}
}

// Allocate returns a free page ID, or 0 if none available
func (f *FreeList) Allocate() PageID {
	// When prevention is active, check if there are pending pages that could be released
	// If so, we should not allocate anything to avoid the race condition
	if f.preventUpToTxnID > 0 {
		// Check if there are any pending pages from transactions <= preventUpToTxnID
		// These pages would normally be released and made available, but we're preventing that
		for txnID := range f.pending {
			if txnID <= f.preventUpToTxnID && len(f.pending[txnID]) > 0 {
				// There are pending pages that could be released
				// Don't allocate anything to avoid the race
				return 0
			}
		}
	}

	if len(f.ids) == 0 {
		return 0
	}
	// Pop from end
	id := f.ids[len(f.ids)-1]
	f.ids = f.ids[:len(f.ids)-1]

	// CRITICAL: Remove from pending to prevent double-allocation
	// If this page is in pending from a previous transaction, remove it
	// to prevent the background releaser from adding it back to free
	removedCount := 0
	for txnID, pageIDs := range f.pending {
		// Remove ALL occurrences of id from this txnID's pending list
		// Iterate backwards to handle removal correctly
		for i := len(pageIDs) - 1; i >= 0; i-- {
			if pageIDs[i] == id {
				// Remove from pending
				f.pending[txnID] = append(pageIDs[:i], pageIDs[i+1:]...)
				pageIDs = f.pending[txnID] // Update reference after modification
				removedCount++
			}
		}
		// Clean up empty entries
		if len(f.pending[txnID]) == 0 {
			delete(f.pending, txnID)
		}
	}

	// DEBUG: Check if page still exists in pending after removal
	for txnID, pageIDs := range f.pending {
		for _, pid := range pageIDs {
			if pid == id {
				panic(fmt.Sprintf("BUG: Allocated pageID %d still in pending[%d] after removal (removed %d occurrences)",
					id, txnID, removedCount))
			}
		}
	}

	return id
}

// Free adds a page ID to the free list
func (f *FreeList) Free(id PageID) {
	// Check if already in free list to prevent duplicates
	for _, existingID := range f.ids {
		if existingID == id {
			return // Already free, don't add duplicate
		}
	}

	f.ids = append(f.ids, id)
	// Keep sorted for deterministic behavior
	for i := len(f.ids) - 1; i > 0; i-- {
		if f.ids[i] < f.ids[i-1] {
			f.ids[i], f.ids[i-1] = f.ids[i-1], f.ids[i]
		} else {
			break
		}
	}
}

// Size returns number of free pages
func (f *FreeList) Size() int {
	return len(f.ids)
}

// FreePending adds pages to the pending map at the given transaction ID.
// These pages are not immediately reusable - they'll be moved to free
// when all readers with txnID < this have finished.
func (f *FreeList) FreePending(txnID uint64, pageIDs []PageID) {
	if len(pageIDs) == 0 {
		return
	}
	f.pending[txnID] = append(f.pending[txnID], pageIDs...)
}

// Release moves pages from pending to free for all transactions < minTxnID.
// Returns the number of pages released.
func (f *FreeList) Release(minTxnID uint64) int {
	released := 0

	// Find all pending entries that can be released
	for txnID, pageIDs := range f.pending {
		if txnID < minTxnID {
			// Move these pages to free list
			for _, id := range pageIDs {
				f.Free(id)
				released++
			}
			// Remove from pending
			delete(f.pending, txnID)
		}
	}

	return released
}

// PendingSize returns the total number of pages in pending state
func (f *FreeList) PendingSize() int {
	total := 0
	for _, pageIDs := range f.pending {
		total += len(pageIDs)
	}
	return total
}

// PagesNeeded returns number of pages needed to serialize this freelist
func (f *FreeList) PagesNeeded() int {
	// Calculate bytes for free IDs
	freeBytes := 8 + len(f.ids)*8 // count + IDs

	// Calculate bytes for pending entries
	pendingBytes := 0
	if len(f.pending) > 0 {
		pendingBytes = 8 + 8 // marker + pending_count
		for _, pageIDs := range f.pending {
			pendingBytes += 8 + 8 + len(pageIDs)*8 // txnID + count + pageIDs
		}
	}

	totalBytes := freeBytes + pendingBytes

	// Always need at least 1 page
	if totalBytes == 0 {
		return 1
	}

	// Calculate pages needed (accounting for count per page)
	// Each page has 8 bytes overhead for count
	pagesNeeded := 0
	remainingBytes := totalBytes
	for remainingBytes > 0 {
		pagesNeeded++
		// First page can hold PageSize bytes
		// But we use 8 bytes for count, so PageSize - 8 for data
		if remainingBytes <= PageSize {
			remainingBytes = 0
		} else {
			remainingBytes -= PageSize
		}
	}

	return pagesNeeded
}

// Serialize writes freelist to pages starting at given slice
func (f *FreeList) Serialize(pages []*Page) {
	// Build linear buffer with all data
	buf := make([]byte, 0, PageSize*len(pages))

	// Write free count
	countBytes := make([]byte, 8)
	*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(f.ids))
	buf = append(buf, countBytes...)

	// Write free IDs
	for _, id := range f.ids {
		idBytes := make([]byte, 8)
		*(*PageID)(unsafe.Pointer(&idBytes[0])) = id
		buf = append(buf, idBytes...)
	}

	// Write pending data if present
	if len(f.pending) > 0 {
		// Write marker
		markerBytes := make([]byte, 8)
		*(*PageID)(unsafe.Pointer(&markerBytes[0])) = PendingMarker
		buf = append(buf, markerBytes...)

		// Write pending count
		pendingCountBytes := make([]byte, 8)
		*(*uint64)(unsafe.Pointer(&pendingCountBytes[0])) = uint64(len(f.pending))
		buf = append(buf, pendingCountBytes...)

		// Sort txnIDs for deterministic serialization
		txnIDs := make([]uint64, 0, len(f.pending))
		for txnID := range f.pending {
			txnIDs = append(txnIDs, txnID)
		}
		// Simple insertion sort for small maps
		for i := 1; i < len(txnIDs); i++ {
			for j := i; j > 0 && txnIDs[j] < txnIDs[j-1]; j-- {
				txnIDs[j], txnIDs[j-1] = txnIDs[j-1], txnIDs[j]
			}
		}

		// Write each pending entry
		for _, txnID := range txnIDs {
			pageIDs := f.pending[txnID]

			// Write txnID
			txnBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&txnBytes[0])) = txnID
			buf = append(buf, txnBytes...)

			// Write page count
			countBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(pageIDs))
			buf = append(buf, countBytes...)

			// Write page IDs
			for _, pageID := range pageIDs {
				pidBytes := make([]byte, 8)
				*(*PageID)(unsafe.Pointer(&pidBytes[0])) = pageID
				buf = append(buf, pidBytes...)
			}
		}
	}

	// Copy buffer to pages
	offset := 0
	for i := 0; i < len(pages); i++ {
		n := copy(pages[i].data[:], buf[offset:])
		offset += n
	}
}

// Deserialize reads freelist from pages
func (f *FreeList) Deserialize(pages []*Page) {
	f.ids = make([]PageID, 0)
	f.pending = make(map[uint64][]PageID)

	// Build linear buffer from pages
	buf := make([]byte, 0, PageSize*len(pages))
	for _, page := range pages {
		buf = append(buf, page.data[:]...)
	}

	offset := 0

	// Read free count
	if len(buf) < 8 {
		return
	}
	freeCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
	offset += 8

	// Read free IDs
	for i := uint64(0); i < freeCount; i++ {
		if offset+8 > len(buf) {
			break
		}
		id := *(*PageID)(unsafe.Pointer(&buf[offset]))
		f.ids = append(f.ids, id)
		offset += 8
	}

	// Check for pending marker
	if offset+8 > len(buf) {
		return
	}
	marker := *(*PageID)(unsafe.Pointer(&buf[offset]))
	if marker != PendingMarker {
		return // No pending data
	}
	offset += 8

	// Read pending count
	if offset+8 > len(buf) {
		return
	}
	pendingCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
	offset += 8

	// Read pending entries
	for i := uint64(0); i < pendingCount; i++ {
		// Read txnID
		if offset+8 > len(buf) {
			break
		}
		txnID := *(*uint64)(unsafe.Pointer(&buf[offset]))
		offset += 8

		// Read page count
		if offset+8 > len(buf) {
			break
		}
		pageCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
		offset += 8

		// Read page IDs
		pageIDs := make([]PageID, 0, pageCount)
		for j := uint64(0); j < pageCount; j++ {
			if offset+8 > len(buf) {
				break
			}
			pageID := *(*PageID)(unsafe.Pointer(&buf[offset]))
			pageIDs = append(pageIDs, pageID)
			offset += 8
		}

		if len(pageIDs) > 0 {
			f.pending[txnID] = pageIDs
		}
	}
}

// PreventAllocationUpTo prevents allocation of pages freed up to and including the specified txnID.
// This is used during checkpoint to prevent allocating pages that were freed by transactions
// being checkpointed, as those pages might still be needed by readers at older transaction IDs.
func (f *FreeList) PreventAllocationUpTo(txnID uint64) {
	f.preventUpToTxnID = txnID
}

// AllowAllAllocations clears the allocation prevention, allowing all free pages to be allocated again.
func (f *FreeList) AllowAllAllocations() {
	f.preventUpToTxnID = 0
}

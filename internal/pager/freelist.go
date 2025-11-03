package pager

import (
	"sort"
	"sync"
	"unsafe"

	"github.com/alexhholmes/fredb/internal/base"
)

// Freelist manages Free and pending pages for MVCC transaction isolation.
// Pages are freed in two stages:
// 1. Pending: Pages freed at epoch cannot be reused until all readers < epoch complete
// 2. Free: Pages released from pending are available for immediate reuse
type Freelist struct {
	mu      sync.RWMutex
	freed   map[base.PageID]struct{} // Pages available for reuse
	pending map[uint64][]base.PageID // epoch -> pages freed at that epoch
}

// Allocate returns a Free Page ID, or 0 if none available.
func (f *Freelist) Allocate() base.PageID {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.freed) == 0 {
		// No Free pages available
		return 0
	}

	// Pop arbitrary element from map
	var id base.PageID
	for id = range f.freed {
		break
	}
	delete(f.freed, id)

	return id
}

// Free adds a Page ID to the Free list
func (f *Freelist) Free(id base.PageID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Map automatically prevents duplicates
	f.freed[id] = struct{}{}
}

// Pending adds pages to the pending map at the given epoch.
// Pages remain pending until Release() moves them to Free list.
func (f *Freelist) Pending(epoch uint64, pageIDs []base.PageID) {
	if len(pageIDs) == 0 {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.pending[epoch] = append(f.pending[epoch], pageIDs...)
}

// Release moves pages from pending to Free for all epochs <= minEpoch.
// Calls onRelease for each freed page (while holding lock) for atomic cache invalidation.
func (f *Freelist) Release(minEpoch uint64, onRelease func(base.PageID)) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	released := 0
	for epoch, pages := range f.pending {
		if epoch <= minEpoch {
			for _, pageID := range pages {
				if onRelease != nil {
					onRelease(pageID) // Cache invalidation happens atomically under lock
				}
				f.freed[pageID] = struct{}{}
				released++
			}
			delete(f.pending, epoch)
		}
	}

	return released
}

// Serialize writes freelist to raw pages for persistence by the pager. Pager
// allocates actual pages. Meta page tracks the starting page ID at the end
// of the file.
func (f *Freelist) Serialize() []*base.Page {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Collect freed page IDs
	allPages := make([]base.PageID, 0, len(f.freed))
	for id := range f.freed {
		allPages = append(allPages, id)
	}

	// Collect pending page IDs from all epochs
	for _, pages := range f.pending {
		allPages = append(allPages, pages...)
	}

	// Sort all page IDs
	sort.Slice(allPages, func(i, j int) bool {
		return allPages[i] < allPages[j]
	})

	// Calculate number of pages needed
	totalBytes := 8 + len(allPages)*8
	pagesNeeded := (totalBytes + base.PageSize - 1) / base.PageSize
	if pagesNeeded == 0 {
		pagesNeeded = 1
	}

	// Allocate pages
	pages := make([]*base.Page, pagesNeeded)
	for i := range pages {
		pages[i] = &base.Page{}
	}

	// Write count at offset 0
	*(*uint64)(unsafe.Pointer(&pages[0].Data[0])) = uint64(len(allPages))

	// Write page IDs sequentially
	offset := 8
	pageIdx := 0
	for _, id := range allPages {
		// Check if we need to move to next page
		if offset+8 > base.PageSize {
			pageIdx++
			offset = 0
		}

		// Write page ID
		*(*base.PageID)(unsafe.Pointer(&pages[pageIdx].Data[offset])) = id
		offset += 8
	}

	return pages
}

// Deserialize reads freelist from pages
func (f *Freelist) Deserialize(pages []*base.Page) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.freed = make(map[base.PageID]struct{})
	f.pending = make(map[uint64][]base.PageID)

	// Build linear buffer from pages
	buf := make([]byte, 0, base.PageSize*len(pages))
	for _, page := range pages {
		buf = append(buf, page.Data[:]...)
	}

	offset := 0

	// Read Free count
	if len(buf) < 8 {
		return
	}
	freeCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
	offset += 8

	// Read Free IDs
	for i := uint64(0); i < freeCount; i++ {
		if offset+8 > len(buf) {
			break
		}
		id := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
		f.freed[id] = struct{}{}
		offset += 8
	}
}

// Stats returns freelist statistics for observability
func (f *Freelist) Stats() (freedCount int) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.freed)
}

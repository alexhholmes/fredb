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

// PagesNeeded returns number of pages needed to serialize this freelist
func (f *Freelist) PagesNeeded() int {
	f.mu.RLock()
	defer f.mu.RUnlock()

	totalBytes := 8 + len(f.freed)*8
	if totalBytes == 0 {
		return 1
	}

	pagesNeeded := 0
	remainingBytes := totalBytes
	for remainingBytes > 0 {
		pagesNeeded++
		if remainingBytes <= base.PageSize {
			remainingBytes = 0
		} else {
			remainingBytes -= base.PageSize
		}
	}

	return pagesNeeded
}

// Serialize writes freelist to pages starting at given slice
func (f *Freelist) Serialize(pages []*base.Page) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	buf := make([]byte, 0, base.PageSize*len(pages))

	// Write Free count
	countBytes := make([]byte, 8)
	*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(f.freed))
	buf = append(buf, countBytes...)

	// Sort freed IDs for deterministic serialization
	freedSlice := make([]base.PageID, 0, len(f.freed))
	for id := range f.freed {
		freedSlice = append(freedSlice, id)
	}
	sort.Slice(freedSlice, func(i, j int) bool {
		return freedSlice[i] < freedSlice[j]
	})

	// Write Free IDs
	for _, id := range freedSlice {
		idBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&idBytes[0])) = id
		buf = append(buf, idBytes...)
	}

	// Copy buffer to pages
	offset := 0
	for i := 0; i < len(pages); i++ {
		n := copy(pages[i].Data[:], buf[offset:])
		offset += n
	}
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

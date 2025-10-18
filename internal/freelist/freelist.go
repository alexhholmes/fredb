package freelist

import (
	"sort"
	"unsafe"

	"github.com/alexhholmes/fredb/internal/base"
)

const (
	// PendingMarker indicates transition from Free IDs to pending entries in serialization
	PendingMarker = base.PageID(0xFFFFFFFFFFFFFFFF)
)

// Freelist manages Free and pending pages for MVCC transaction isolation.
// Pages are freed in two stages:
// 1. Pending: Pages freed at txnID cannot be reused until all readers < txnID complete
// 2. Free: Pages released from pending are available for immediate reuse
type Freelist struct {
	freed          map[base.PageID]struct{} // Pages available for reuse
	pending        map[uint64][]base.PageID // txnID -> pages freed at that transaction
	pendingReverse map[base.PageID]uint64   // pageID -> txnID for O(1) lookup
}

// New creates a new Freelist with empty state
func New() *Freelist {
	return &Freelist{
		freed:          make(map[base.PageID]struct{}),
		pending:        make(map[uint64][]base.PageID),
		pendingReverse: make(map[base.PageID]uint64),
	}
}

// Allocate returns a Free Page ID, or 0 if none available.
// Uses reverse index for O(1) pending lookup instead of O(n*m) scan.
func (f *Freelist) Allocate() base.PageID {
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

	// CRITICAL: Remove from pending to prevent double-allocation
	// Use reverse index for O(1) lookup instead of scanning all pending entries
	if txnID, exists := f.pendingReverse[id]; exists {
		pages := f.pending[txnID]
		for i := len(pages) - 1; i >= 0; i-- {
			if pages[i] == id {
				f.pending[txnID] = append(pages[:i], pages[i+1:]...)
				break
			}
		}
		if len(f.pending[txnID]) == 0 {
			delete(f.pending, txnID)
		}
		delete(f.pendingReverse, id)
	}

	return id
}

// Free adds a Page ID to the Free list (internal, caller must hold lock)
func (f *Freelist) Free(id base.PageID) {
	// Map automatically prevents duplicates
	f.freed[id] = struct{}{}
}

// Pending adds pages to the pending map at the given transaction ID.
// Pages remain pending until Release() moves them to Free list.
// Maintains reverse index for O(1) lookup in Allocate().
func (f *Freelist) Pending(txnID uint64, pageIDs []base.PageID) {
	if len(pageIDs) == 0 {
		return
	}

	f.pending[txnID] = append(f.pending[txnID], pageIDs...)

	// Maintain reverse index
	for _, pageID := range pageIDs {
		f.pendingReverse[pageID] = txnID
	}
}

// Release moves pages from pending to Free for all transactions < minTxnID.
// Returns number of pages released.
// Cleans up reverse index entries.
func (f *Freelist) Release(minTxnID uint64) int {
	released := 0
	for txnID, pages := range f.pending {
		if txnID < minTxnID {
			for _, pageID := range pages {
				f.Free(pageID)
				delete(f.pendingReverse, pageID)
				released++
			}
			delete(f.pending, txnID)
		}
	}
	return released
}

// PagesNeeded returns number of pages needed to serialize this freelist
func (f *Freelist) PagesNeeded() int {
	freeBytes := 8 + len(f.freed)*8

	pendingBytes := 0
	if len(f.pending) > 0 {
		pendingBytes = 8 + 8 // marker + count
		for _, pages := range f.pending {
			pendingBytes += 8 + 8 + len(pages)*8 // txnID + count + page IDs
		}
	}

	totalBytes := freeBytes + pendingBytes
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

	// Write pending data if present
	if len(f.pending) > 0 {
		// Write marker
		markerBytes := make([]byte, 8)
		*(*base.PageID)(unsafe.Pointer(&markerBytes[0])) = PendingMarker
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
		sort.Slice(txnIDs, func(i, j int) bool {
			return txnIDs[i] < txnIDs[j]
		})

		// Write each pending entry
		for _, txnID := range txnIDs {
			pages := f.pending[txnID]

			// Write txnID
			txnBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&txnBytes[0])) = txnID
			buf = append(buf, txnBytes...)

			// Write Page count
			countBytes := make([]byte, 8)
			*(*uint64)(unsafe.Pointer(&countBytes[0])) = uint64(len(pages))
			buf = append(buf, countBytes...)

			// Write page IDs
			for _, pageID := range pages {
				pidBytes := make([]byte, 8)
				*(*base.PageID)(unsafe.Pointer(&pidBytes[0])) = pageID
				buf = append(buf, pidBytes...)
			}
		}
	}

	// Copy buffer to pages
	offset := 0
	for i := 0; i < len(pages); i++ {
		n := copy(pages[i].Data[:], buf[offset:])
		offset += n
	}
}

// Deserialize reads freelist from pages and rebuilds reverse index
func (f *Freelist) Deserialize(pages []*base.Page) {
	f.freed = make(map[base.PageID]struct{})
	f.pending = make(map[uint64][]base.PageID)
	f.pendingReverse = make(map[base.PageID]uint64)

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

	// Check for pending marker
	if offset+8 > len(buf) {
		return
	}
	marker := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
	if marker != PendingMarker {
		return
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

		// Read Page count
		if offset+8 > len(buf) {
			break
		}
		pageCount := *(*uint64)(unsafe.Pointer(&buf[offset]))
		offset += 8

		// Read Page IDs
		pageIDs := make([]base.PageID, 0, pageCount)
		for j := uint64(0); j < pageCount; j++ {
			if offset+8 > len(buf) {
				break
			}
			pageID := *(*base.PageID)(unsafe.Pointer(&buf[offset]))
			pageIDs = append(pageIDs, pageID)
			offset += 8
		}

		if len(pageIDs) > 0 {
			f.pending[txnID] = pageIDs
			// Rebuild reverse index
			for _, pageID := range pageIDs {
				f.pendingReverse[pageID] = txnID
			}
		}
	}
}

// Stats returns freelist statistics for observability
func (f *Freelist) Stats() (freedCount int) {
	return len(f.freed)
}

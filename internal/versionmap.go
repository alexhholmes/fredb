package internal

import (
	"sync"
)

// VersionMap tracks where old Page versions have been relocated
// when evicted from cache. This allows long-running readers to access
// old versions that have been overwritten on disk by checkpoints.
//
// This is a purely in-memory structure - it doesn't need persistence because:
// - Readers don't survive crashes
// - Relocated pages are immediately added to freelist.pending
// - On restart, pending pages are freed (no readers exist)
//
// Example:
//   - Page 100 has version@3 (cached)
//   - Page 100 updated to version@8 (checkpointed to disk at PageID=100)
//   - Cache needs space, evicts version@3
//   - Relocate version@3 to free Page 500
//   - Track: versionMap[100][3] = 500
//   - Add Page 500 to freelist.pending[3]
//   - Reader@5 can still load version@3 from Page 500
type VersionMap struct {
	// Map: originalPageID -> (txnID -> relocatedPageID)
	relocations map[PageID]map[uint64]PageID
	mu          sync.RWMutex
}

// NewVersionMap creates a new version relocation tracker
func NewVersionMap() *VersionMap {
	return &VersionMap{
		relocations: make(map[PageID]map[uint64]PageID),
	}
}

// Track records that a Page version has been relocated to a new location
func (vm *VersionMap) Track(originalPageID PageID, txnID uint64, relocatedPageID PageID) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if vm.relocations[originalPageID] == nil {
		vm.relocations[originalPageID] = make(map[uint64]PageID)
	}
	vm.relocations[originalPageID][txnID] = relocatedPageID
}

// Get returns the relocated Page ID for a specific version, or 0 if not relocated
func (vm *VersionMap) Get(originalPageID PageID, txnID uint64) PageID {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	if versions := vm.relocations[originalPageID]; versions != nil {
		return versions[txnID]
	}
	return 0
}

// GetLatestVisible returns the relocated Page ID for the latest version visible to txnID,
// or 0 if no visible version is relocated. Used for MVCC snapshot isolation.
func (vm *VersionMap) GetLatestVisible(originalPageID PageID, maxTxnID uint64) (PageID, uint64) {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	versions := vm.relocations[originalPageID]
	if versions == nil {
		return 0, 0
	}

	// Find latest version where versionTxnID <= maxTxnID
	var latestTxnID uint64
	var latestPageID PageID

	for versionTxnID, relocatedPageID := range versions {
		if versionTxnID <= maxTxnID && versionTxnID > latestTxnID {
			latestTxnID = versionTxnID
			latestPageID = relocatedPageID
		}
	}

	return latestPageID, latestTxnID
}

// Remove removes a relocation entry (called when version is no longer needed)
func (vm *VersionMap) Remove(originalPageID PageID, txnID uint64) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	if versions := vm.relocations[originalPageID]; versions != nil {
		delete(versions, txnID)
		if len(versions) == 0 {
			delete(vm.relocations, originalPageID)
		}
	}
}

// Cleanup removes all relocations for versions older than minReaderTxn
// Returns the relocated Page IDs that can now be freed
func (vm *VersionMap) Cleanup(minReaderTxn uint64) []PageID {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	var toFree []PageID

	for originalPageID, versions := range vm.relocations {
		for txnID, relocatedPageID := range versions {
			if txnID < minReaderTxn {
				// No readers need this version anymore
				toFree = append(toFree, relocatedPageID)
				delete(versions, txnID)
			}
		}

		// Remove empty version maps
		if len(versions) == 0 {
			delete(vm.relocations, originalPageID)
		}
	}

	return toFree
}

// Size returns the total number of tracked relocations
func (vm *VersionMap) Size() int {
	vm.mu.RLock()
	defer vm.mu.RUnlock()

	total := 0
	for _, versions := range vm.relocations {
		total += len(versions)
	}
	return total
}

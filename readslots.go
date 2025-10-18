package fredb

import (
	"math"
	"sync/atomic"
)

// ReaderSlots provides fixed-size slot-based reader tracking for bounded concurrency
// Each slot is an atomic pointer, giving O(1) register/unregister with no allocation
type ReaderSlots struct {
	slots       []atomic.Pointer[Tx] // Fixed-size array of reader slots
	maxSize     int                  // Maximum number of concurrent readers
	activeCount atomic.Int32         // Count of active readers
	minTxID     atomic.Uint64        // Cached minimum txID (MaxUint64 when no readers)
}

// NewReaderSlots creates a fixed-size slot array for reader tracking
func NewReaderSlots(maxReaders int) *ReaderSlots {
	rs := &ReaderSlots{
		slots:   make([]atomic.Pointer[Tx], maxReaders),
		maxSize: maxReaders,
	}
	rs.minTxID.Store(math.MaxUint64) // Initialize to max (no readers)
	return rs
}

// Register finds an empty slot and atomically assigns it to the reader
// Returns the slot index on success, error if all slots are full
func (rs *ReaderSlots) Register(tx *Tx) (int, error) {
	for i := 0; i < rs.maxSize; i++ {
		// Try to claim this slot atomically
		if rs.slots[i].CompareAndSwap(nil, tx) {
			rs.activeCount.Add(1)

			// Update cached min if this is smaller
			for {
				current := rs.minTxID.Load()
				if tx.txID >= current {
					break // Current min is smaller or equal
				}
				if rs.minTxID.CompareAndSwap(current, tx.txID) {
					break // Updated min
				}
			}

			return i, nil
		}
	}
	return -1, ErrTooManyReaders
}

// Unregister atomically clears the slot and handles cache invalidation
func (rs *ReaderSlots) Unregister(slot int) {
	tx := rs.slots[slot].Load()
	rs.slots[slot].Store(nil)

	if rs.activeCount.Add(-1) == 0 {
		// Last reader out, reset min
		rs.minTxID.Store(math.MaxUint64)
	} else if tx != nil && tx.txID == rs.minTxID.Load() {
		// We removed the min reader, need rescan
		rs.rescanMin()
	}
}

// rescanMin rescans all slots to find the new minimum txID
func (rs *ReaderSlots) rescanMin() {
	minTxID := uint64(math.MaxUint64)
	for i := 0; i < rs.maxSize; i++ {
		if tx := rs.slots[i].Load(); tx != nil {
			if tx.txID < minTxID {
				minTxID = tx.txID
			}
		}
	}
	rs.minTxID.Store(minTxID)
}

// GetMinTxID returns the cached minimum transaction ID (O(1) lookup)
func (rs *ReaderSlots) GetMinTxID() uint64 {
	if rs.activeCount.Load() == 0 {
		return math.MaxUint64 // Fast path: no readers
	}
	return rs.minTxID.Load() // O(1) cached value
}

package lifecycle

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

var ErrTooManyReaders = errors.New("too many concurrent readers (increase maxReaders)")

// ReaderSlots provides fixed-size slot-based reader tracking for bounded concurrency
// Each slot stores a txID directly, giving O(1) register/unregister with no allocation
type ReaderSlots struct {
	slots   []atomic.Uint64 // Fixed-size array of txIDs (0 = empty slot)
	maxSize int             // Maximum number of concurrent readers
	active  atomic.Int32    // Count of active readers
	minTxID atomic.Uint64   // Cached minimum txID (MaxUint64 when no readers)
}

// NewReaderSlots creates a fixed-size slot array for reader tracking
func NewReaderSlots(maxReaders int) *ReaderSlots {
	rs := &ReaderSlots{
		slots:   make([]atomic.Uint64, maxReaders),
		maxSize: maxReaders,
	}
	rs.minTxID.Store(math.MaxUint64) // Initialize to max (no readers)
	return rs
}

// Register finds an empty slot and atomically assigns it to the reader.
// Returns an unregister function to free the slot when done.
func (rs *ReaderSlots) Register(txID uint64) (func(), error) {
	for i := 0; i < rs.maxSize; i++ {
		// Try to claim this slot atomically (0 = empty)
		if rs.slots[i].CompareAndSwap(0, txID) {
			rs.active.Add(1)

			// Update cached min if this is smaller
			for {
				current := rs.minTxID.Load()
				if txID >= current {
					break // Current min is smaller or equal
				}
				if rs.minTxID.CompareAndSwap(current, txID) {
					break // Updated min
				}
			}

			var once sync.Once
			return func() {
				once.Do(func() {
					rs.unregister(i)
				})
			}, nil
		}
	}
	return nil, ErrTooManyReaders
}

// unregister atomically clears the slot and handles cache invalidation
func (rs *ReaderSlots) unregister(slot int) {
	txID := rs.slots[slot].Swap(0) // Clear slot, get old value

	if rs.active.Add(-1) == 0 {
		// Last reader out, reset min
		rs.minTxID.Store(math.MaxUint64)
	} else if txID == rs.minTxID.Load() {
		// We removed the min reader, need rescan
		minTxID := uint64(math.MaxUint64)
		for i := 0; i < rs.maxSize; i++ {
			if txID := rs.slots[i].Load(); txID != 0 && txID < minTxID {
				minTxID = txID
			}
		}
		rs.minTxID.Store(minTxID)
	}
}

// MinTxID returns the cached minimum transaction ID (O(1) lookup)
func (rs *ReaderSlots) MinTxID() uint64 {
	if rs.active.Load() == 0 {
		return math.MaxUint64 // Fast path: no readers
	}
	return rs.minTxID.Load() // O(1) cached value
}

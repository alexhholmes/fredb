package fredb

import (
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrPageOverflow       = errors.New("page overflow: serialized data exceeds page size")
	ErrInvalidOffset      = errors.New("invalid offset: out of bounds")
	ErrInvalidMagicNumber = errors.New("invalid magic number")
	ErrInvalidVersion     = errors.New("invalid format version")
	ErrInvalidPageSize    = errors.New("invalid page size")
	ErrInvalidChecksum    = errors.New("invalid checksum")
	ErrDatabaseClosed     = errors.New("database is closed")
	ErrCorruption         = errors.New("data corruption detected")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrValueTooLarge      = errors.New("value too large")
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	// Set conservatively to ensure branch nodes can hold multiple keys.
	// With 4KB pages, limiting keys to 1KB allows ~3 keys per branch node.
	MaxKeySize = 1024

	// MaxValueSize is the maximum length of a value, in bytes.
	// Following bbolt's limit of (1 << 31) - 2, but in practice
	// limited by page size since we don't support overflow pages yet.
	MaxValueSize = (1 << 31) - 2
)

type db struct {
	mu     sync.RWMutex
	store  *BTree
	closed bool // Database closed flag

	// Transaction state
	writerTx  *Tx    // Current write transaction (nil if none)
	readerTxs []*Tx  // Active read transactions
	nextTxnID uint64 // Monotonic transaction ID counter

	// Background releaser
	releaseC chan uint64    // Trigger release (unbuffered)
	stopC    chan struct{}  // Shutdown signal
	wg       sync.WaitGroup // Clean shutdown
}

func Open(path string, options ...DBOption) (*db, error) {
	// Apply options
	opts := defaultDBOptions()
	for _, opt := range options {
		opt(&opts)
	}

	pager, err := NewDiskPageManager(path, opts)
	if err != nil {
		return nil, err
	}

	btree, err := NewBTree(pager)
	if err != nil {
		return nil, err
	}

	// Initialize nextTxnID from disk to ensure monotonic IDs across sessions
	meta := pager.GetMeta()

	d := &db{
		store:     btree,
		nextTxnID: meta.TxnID, // Resume from last committed TxnID
		releaseC:  make(chan uint64),
		stopC:     make(chan struct{}),
	}

	// Start background releaser goroutine
	d.wg.Add(1)
	go d.backgroundReleaser()

	// Start background checkpointer goroutine
	d.wg.Add(1)
	go d.backgroundCheckpointer()

	return d, nil
}

func (d *db) Set(key, value []byte) error {
	return d.Update(func(tx *Tx) error {
		return tx.Set(key, value)
	})
}

func (d *db) Delete(key []byte) error {
	return d.Update(func(tx *Tx) error {
		return tx.Delete(key)
	})
}

func (d *db) Begin(writable bool) (*Tx, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if database is closed
	if d.closed {
		return nil, ErrDatabaseClosed
	}

	// Enforce single writer rule
	if writable && d.writerTx != nil {
		return nil, ErrTxInProgress
	}

	// Assign monotonic transaction ID
	d.nextTxnID++
	txnID := d.nextTxnID

	// Create transaction
	// Read transactions keep this snapshot, write transactions replace it on first modification
	tx := &Tx{
		db:       d,
		txnID:    txnID,
		writable: writable,
		meta:     nil,          // Reserved for future BTree metadata tracking
		root:     d.store.root, // Capture snapshot for MVCC isolation
		pending:  make([]PageID, 0),
		freed:    make([]PageID, 0),
		done:     false,
	}

	// Initialize TX-local cache for write transactions
	if writable {
		tx.pages = make(map[PageID]*node)
	}

	// Track active transaction
	if writable {
		d.writerTx = tx
	} else {
		d.readerTxs = append(d.readerTxs, tx)
	}

	return tx, nil
}

// View executes a function within a read-only transaction.
// If the function returns an error, the transaction is rolled back.
// If the function returns nil, the transaction is rolled back (read-only).
func (d *db) View(fn func(*Tx) error) error {
	tx, err := d.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	return fn(tx)
}

// Update executes a function within a read-write transaction.
// If the function returns an error, the transaction is rolled back.
// If the function returns nil, the transaction is committed.
func (d *db) Update(fn func(*Tx) error) error {
	tx, err := d.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *db) Close() error {
	// Stop background goroutines
	select {
	case <-d.stopC:
		// Already closed
	default:
		close(d.stopC)
		d.wg.Wait()
	}

	// Force sync WAL before checkpoint to ensure all data is durable
	if dm, ok := d.store.pager.(*DiskPageManager); ok {
		if err := dm.ForceSyncWAL(); err != nil {
			return err
		}
	}

	// Final checkpoint to flush WAL
	if err := d.checkpoint(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Mark database as closed
	d.closed = true

	return d.store.close()
}

// backgroundReleaser periodically releases pending pages when safe
func (d *db) backgroundReleaser() {
	defer d.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Periodic active
			minTxn := d.minReaderTxn()
			d.releasePages(minTxn)

		case <-d.releaseC:
			// Reader-triggered release - recalculate minimum
			minTxn := d.minReaderTxn()
			d.releasePages(minTxn)

		case <-d.stopC:
			// Shutdown - release everything
			d.releasePages(math.MaxUint64)
			return
		}
	}
}

// backgroundCheckpointer periodically checkpoints the WAL to disk
func (d *db) backgroundCheckpointer() {
	defer d.wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Ignore errors in background checkpoint
			_ = d.checkpoint()

		case <-d.stopC:
			return
		}
	}
}

// checkpoint writes WAL transactions to main file and truncates WAL
func (d *db) checkpoint() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Skip checkpoint if there's an active write transaction
	// This prevents corruption from checkpoint allocating pages that are being freed
	if d.writerTx != nil {
		return nil
	}

	dm, ok := d.store.pager.(*DiskPageManager)
	if !ok {
		return nil // In-memory database, skip checkpoint
	}

	// Get current transaction boundary
	meta := dm.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	currentTxn := meta.TxnID

	// Nothing to checkpoint
	if currentTxn <= checkpointTxn {
		return nil
	}

	// Get minimum reader txnID to determine which disk versions need preservation
	// IMPORTANT: If no readers, use currentTxn (last committed) not nextTxnID
	// This prevents checkpoint from allocating pages freed by the just-committed transaction
	minReaderTxn := d.minReaderTxnForCheckpoint(currentTxn)

	// Before replaying WAL, temporarily prevent allocation of recently freed pages
	// This ensures checkpoint won't allocate pages freed by transactions being checkpointed
	dm.mu.Lock()
	dm.freelist.PreventAllocationUpTo(currentTxn)
	dm.mu.Unlock()
	defer func() {
		dm.mu.Lock()
		dm.freelist.AllowAllAllocations()
		dm.mu.Unlock()
	}()

	// Replay WAL from last checkpoint to current
	// IMPORTANT: Before overwriting each page, check if old disk version needs preservation
	err := dm.ReplayWAL(checkpointTxn, func(pageID PageID, newPage *Page) error {
		// Read current disk version BEFORE overwriting
		// Note: We need to bypass the WAL latch check for this read
		oldPage, readErr := dm.readPageAtUnsafe(pageID)

		if readErr == nil && oldPage != nil {
			// Parse old version's TxnID
			oldHeader := oldPage.Header()
			oldTxnID := oldHeader.TxnID

			// If any active reader might need this old version, relocate it
			// Preserve if oldTxnID < minReaderTxn (old version still potentially visible to readers)
			if oldTxnID < minReaderTxn && oldHeader.NumKeys > 0 {
				// Allocate free page for relocation
				relocPageID, allocErr := dm.AllocatePage()
				if allocErr != nil {
					return allocErr
				}

				// Write old disk version to relocated page
				if writeErr := dm.WritePage(relocPageID, oldPage); writeErr != nil {
					return writeErr
				}

				// Add to VersionMap so readers can find it
				d.store.cache.versionMap.Track(pageID, oldTxnID, relocPageID)

				// NOTE: Do NOT call FreePending here! The relocated page will be freed
				// later by CleanupRelocatedVersions when no readers need it anymore.
				// Calling FreePending here would cause a double-free bug.
			}
		}

		// Now overwrite disk with new checkpointed version
		if err := dm.WritePage(pageID, newPage); err != nil {
			return err
		}

		// Remove WAL latch immediately after writing to disk
		// This allows readers to access the page from disk instead of being blocked
		dm.walPages.Delete(pageID)

		return nil
	})
	if err != nil {
		return err
	}

	// Fsync main B-tree file
	dm.mu.Lock()
	if err := dm.file.Sync(); err != nil {
		dm.mu.Unlock()
		return err
	}
	dm.mu.Unlock()

	// Update meta with new checkpoint marker
	// IMPORTANT: Make a copy of meta to avoid overwriting concurrent updates
	freshMeta := *dm.GetMeta() // Dereference to copy
	freshMeta.CheckpointTxnID = currentTxn
	if err := dm.PutMeta(&freshMeta); err != nil {
		return err
	}

	// Fsync meta page (PutMeta already writes, but need explicit fsync)
	dm.mu.Lock()
	err = dm.file.Sync()
	dm.mu.Unlock()
	if err != nil {
		return err
	}

	// Truncate WAL up to checkpoint
	if err := dm.TruncateWAL(currentTxn); err != nil {
		return err
	}

	// Evict checkpointed versions from cache
	// Only evict versions older than all active readers to preserve MVCC
	_ = d.store.cache.EvictCheckpointed(minReaderTxn)

	// Cleanup WAL page latches for checkpointed pages visible to all readers
	dm.CleanupLatchOnWAL(minReaderTxn)

	return nil
}

// minReaderTxn returns the minimum transaction ID across all active transactions (readers + writer).
// This determines which pending pages can be safely released to the freelist.
func (d *db) minReaderTxn() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Start with minimum possible value
	min := d.nextTxnID

	// Consider active write transaction
	if d.writerTx != nil {
		min = d.writerTx.txnID
	}

	// Consider all active read transactions
	for _, tx := range d.readerTxs {
		if tx.txnID < min {
			min = tx.txnID
		}
	}

	return min
}

// minReaderTxnForCheckpoint returns minimum transaction ID for checkpoint operations.
// Unlike minReaderTxn, this uses lastCommittedTxn as the floor when no readers exist.
// This prevents checkpoint from allocating pages that were just freed.
func (d *db) minReaderTxnForCheckpoint(lastCommittedTxn uint64) uint64 {
	// Start with last committed transaction as minimum
	// This ensures pages freed by that transaction won't be reallocated during checkpoint
	min := lastCommittedTxn

	// Consider active write transaction (should not exist during checkpoint, but be safe)
	if d.writerTx != nil {
		if d.writerTx.txnID < min {
			min = d.writerTx.txnID
		}
	}

	// Consider all active read transactions
	for _, tx := range d.readerTxs {
		if tx.txnID < min {
			min = tx.txnID
		}
	}

	return min
}

// releasePages calls Release on the freelist with the given minimum transaction ID
func (d *db) releasePages(minTxn uint64) {
	// Need to access the DiskPageManager's freelist
	if dm, ok := d.store.pager.(*DiskPageManager); ok {
		dm.mu.Lock()
		dm.freelist.Release(minTxn)
		dm.mu.Unlock()

		// Cleanup WAL page latches for checkpointed pages visible to all readers
		dm.CleanupLatchOnWAL(minTxn)
	}

	// Cleanup relocated page versions that are no longer needed
	d.store.cache.CleanupRelocatedVersions(minTxn)
}

func (d *db) Get(key []byte) ([]byte, error) {
	var result []byte
	err := d.View(func(tx *Tx) error {
		val, err := tx.Get(key)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

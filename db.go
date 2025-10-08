package fredb

import (
	"errors"
	"math"
	"sync"
	"time"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/storage"
	"fredb/internal/wal"
)

//goland:noinspection GoUnusedGlobalVariable
var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrDatabaseClosed     = errors.New("database is closed")
	ErrCorruption         = errors.New("data corruption detected")
	ErrKeyTooLarge        = errors.New("key too large")
	ErrValueTooLarge      = errors.New("value too large")
	ErrPageOverflow       = base.ErrPageOverflow
	ErrInvalidOffset      = base.ErrInvalidOffset
	ErrInvalidMagicNumber = base.ErrInvalidMagicNumber
	ErrInvalidVersion     = base.ErrInvalidVersion
	ErrInvalidPageSize    = base.ErrInvalidPageSize
	ErrInvalidChecksum    = base.ErrInvalidChecksum
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	// Set conservatively to ensure branch nodes can hold multiple Keys.
	// With 4KB pages, limiting Keys to 1KB allows ~3 Keys per branch Node.
	MaxKeySize = 1024

	// MaxValueSize is the maximum length of a value, in bytes.
	// Following bbolt's limit of (1 << 31) - 2, but in practice
	// limited by Page size since we don't support overflow pages yet.
	MaxValueSize = (1 << 31) - 2
)

type DB struct {
	mu     sync.RWMutex
	store  *btree
	wal    *wal.WAL // Write-ahead log for durability
	closed bool     // Database closed flag

	// Transaction state
	writerTx  *Tx    // Current write transaction (nil if none)
	readerTxs []*Tx  // Active read transactions
	nextTxnID uint64 // Monotonic transaction ID counter

	// Background releaser
	releaseC chan uint64    // Trigger release (unbuffered)
	stopC    chan struct{}  // Shutdown signal
	wg       sync.WaitGroup // Clean shutdown
}

func Open(path string, options ...DBOption) (*DB, error) {
	// Apply options
	opts := defaultDBOptions()
	for _, opt := range options {
		opt(&opts)
	}

	// Open newWAL (DB owns newWAL lifecycle)
	walPath := path + ".newWAL"
	newWAL, err := wal.NewWAL(walPath, opts.walSyncMode, opts.walBytesPerSync)
	if err != nil {
		return nil, err
	}

	pager, err := storage.NewPageManager(path)
	if err != nil {
		_ = newWAL.Close()
		return nil, err
	}

	c := cache.NewPageCache(opts.maxCacheSizeMB*256, pager)

	bt, err := newTree(pager, c)
	if err != nil {
		_ = newWAL.Close()
		_ = pager.Close()
		return nil, err
	}

	// Initialize nextTxnID from disk to ensure monotonic IDs across sessions
	meta := pager.GetMeta()

	db := &DB{
		store:     bt,
		wal:       newWAL,
		nextTxnID: meta.TxnID, // Resume from last committed TxnID
		releaseC:  make(chan uint64),
		stopC:     make(chan struct{}),
	}

	// Recover uncommitted newWAL entries into cache
	if err := db.recoverFromWAL(bt.cache); err != nil {
		return nil, err
	}

	// Start background releaser goroutine
	db.wg.Add(1)
	go db.backgroundReleaser()

	// Start background checkpoint goroutine
	db.wg.Add(1)
	go db.backgroundCheckpointer()

	return db, nil
}

// recoverFromWAL replays uncommitted wal entries into cache after startup.
// This recovers transactions that were committed to wal but not yet checkpointed to disk.
func (d *DB) recoverFromWAL(cache *cache.PageCache) error {
	meta := d.store.pager.GetMeta()
	checkpointTxn := meta.CheckpointTxnID

	return d.wal.Replay(checkpointTxn, func(pageID base.PageID, page *base.Page) error {
		// Create empty node and Deserialize pages data into it
		node := &base.Node{}
		if err := node.Deserialize(page); err != nil {
			return err
		}

		header := page.Header()

		// Load into cache as hot version (ready for next reader)
		cache.Put(pageID, header.TxnID, node)

		// Rebuild wal latch to prevent stale disk reads
		d.wal.Pages.Store(pageID, header.TxnID)

		return nil
	})
}

func (d *DB) Get(key []byte) ([]byte, error) {
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

func (d *DB) Set(key, value []byte) error {
	return d.Update(func(tx *Tx) error {
		return tx.Set(key, value)
	})
}

func (d *DB) Delete(key []byte) error {
	return d.Update(func(tx *Tx) error {
		return tx.Delete(key)
	})
}

func (d *DB) Begin(writable bool) (*Tx, error) {
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
		root:     d.store.root, // Capture snapshot for MVCC isolation
		pending:  make([]base.PageID, 0),
		freed:    make([]base.PageID, 0),
		done:     false,
	}

	// Initialize TX-local cache for write transactions
	if writable {
		tx.pages = make(map[base.PageID]*base.Node)
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
func (d *DB) View(fn func(*Tx) error) error {
	tx, err := d.Begin(false)
	if err != nil {
		return err
	}
	defer func(tx *Tx) {
		_ = tx.Rollback()
	}(tx)

	return fn(tx)
}

// Update executes a function within a read-write transaction.
// If the function returns an error, the transaction is rolled back.
// If the function returns nil, the transaction is committed.
func (d *DB) Update(fn func(*Tx) error) error {
	tx, err := d.Begin(true)
	if err != nil {
		return err
	}
	defer func(tx *Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit()
}

func (d *DB) Close() error {
	// Stop background goroutines
	select {
	case <-d.stopC:
		// Already closed
	default:
		close(d.stopC)
		d.wg.Wait()
	}

	// Force sync wal before checkpoint to ensure all data is durable
	if err := d.wal.ForceSync(); err != nil {
		return err
	}

	// Final checkpoint to flush wal
	if err := d.checkpoint(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Mark database as closed
	d.closed = true

	// Close wal
	if err := d.wal.Close(); err != nil {
		_ = d.store.close()
		return err
	}

	return d.store.close()
}

// backgroundReleaser periodically releases pending pages when safe
func (d *DB) backgroundReleaser() {
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

// backgroundCheckpointer periodically checkpoints the wal to disk
func (d *DB) backgroundCheckpointer() {
	defer d.wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Ignore errors in background checkpoint
			_ = d.checkpoint()

		case <-d.releaseC:
			// Also checkpoint on reader-triggered release
			err := d.checkpoint()
			if err != nil {
				// TODO
			}

		case <-d.stopC:
			return
		}
	}
}

// checkpoint writes wal transactions to main file and truncates wal
func (d *DB) checkpoint() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Skip checkpoint if there's an active write transaction
	// This prevents corruption from checkpoint allocating pages that are being freed
	if d.writerTx != nil {
		return nil
	}

	dm := d.store.pager

	// get current transaction boundary
	meta := dm.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	currentTxn := meta.TxnID

	// Nothing to checkpoint
	if currentTxn <= checkpointTxn {
		return nil
	}

	// get minimum reader txnID to determine which disk versions need preservation
	// IMPORTANT: If no readers, use currentTxn (last committed) not nextTxnID
	// This prevents checkpoint from allocating pages freed by the just-committed transaction
	minReaderTxn := d.minReaderTxnForCheckpoint(currentTxn)

	// Before replaying wal, temporarily prevent allocation of recently freed pages
	// This ensures checkpoint won't allocate pages freed by transactions being checkpointed
	dm.PreventAllocationUpTo(currentTxn)
	defer dm.AllowAllAllocations()

	// Replay wal from last checkpoint to current
	// IMPORTANT: Idempotent replay - skip pages already at correct version
	err := d.wal.Replay(checkpointTxn, func(pageID base.PageID,
		newPage *base.Page) error {
		// Read current disk version BEFORE overwriting
		// Note: We need to bypass the wal latch check for this read
		oldPage, readErr := dm.ReadPage(pageID)

		// Get new pages version for idempotency check
		newHeader := newPage.Header()
		newTxnID := newHeader.TxnID

		if readErr == nil && oldPage != nil {
			// Parse old version's TxnID
			oldHeader := oldPage.Header()
			oldTxnID := oldHeader.TxnID

			// IDEMPOTENCY: Skip if already applied (prevents double-apply corruption)
			// This handles crash between file.Sync() and PutMeta()
			if oldTxnID >= newTxnID {
				// Already has this version or newer, skip write
				d.wal.Pages.Delete(pageID) // Clear latch even if skipping
				return nil
			}

			// If any active reader might need this old version, relocate it
			// Preserve if oldTxnID < minReaderTxn (old version still potentially visible to readers)
			if oldTxnID < minReaderTxn && oldHeader.NumKeys > 0 {
				// Allocate free Page for relocation
				relocPageID, allocErr := dm.AllocatePage()
				if allocErr != nil {
					return allocErr
				}

				// Write old disk version to relocated Page
				if writeErr := dm.WritePage(relocPageID, oldPage); writeErr != nil {
					return writeErr
				}

				// Add to VersionMap so readers can find it
				d.store.pager.TrackRelocation(pageID, oldTxnID, relocPageID)

				// NOTE: Do NOT call FreePending here! The relocated Page will be freed
				// later by CleanupRelocatedVersions when no readers need it anymore.
				// Calling FreePending here would cause a double-free bug.
			}
		}

		// Now overwrite disk with new checkpointed version
		if err := dm.WritePage(pageID, newPage); err != nil {
			return err
		}

		// Remove wal latch immediately after writing to disk
		// This allows readers to access the Page from disk instead of being blocked
		d.wal.Pages.Delete(pageID)

		return nil
	})
	if err != nil {
		return err
	}

	// Fsync main B-tree file
	if err := dm.Sync(); err != nil {
		return err
	}

	// Update meta with new checkpoint marker
	// IMPORTANT: Make a copy of meta to avoid overwriting concurrent updates
	freshMeta := dm.GetMeta() // Dereference to copy
	freshMeta.CheckpointTxnID = currentTxn
	if err := dm.PutMeta(freshMeta); err != nil {
		return err
	}

	// Fsync meta Page (PutMeta already writes, but need explicit fsync)
	if err := dm.Sync(); err != nil {
		return err
	}

	// Truncate wal up to checkpoint
	if err := d.wal.Truncate(currentTxn); err != nil {
		return err
	}

	// Evict checkpointed versions from cache
	// Only evict versions older than all active readers to preserve MVCC
	_ = d.store.cache.EvictCheckpointed(minReaderTxn)

	// Cleanup wal Page latches for checkpointed pages visible to all readers
	d.wal.CleanupLatch(checkpointTxn, minReaderTxn)

	return nil
}

// minReaderTxn returns the minimum transaction ID across all active transactions (readers + writer).
// This determines which pending pages can be safely released to the freelist.
func (d *DB) minReaderTxn() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Start with minimum possible value
	minTxnID := d.nextTxnID

	// Consider active write transaction
	if d.writerTx != nil {
		minTxnID = d.writerTx.txnID
	}

	// Consider all active read transactions
	for _, tx := range d.readerTxs {
		if tx.txnID < minTxnID {
			minTxnID = tx.txnID
		}
	}

	return minTxnID
}

// minReaderTxnForCheckpoint returns minimum transaction ID for checkpoint operations.
// Unlike minReaderTxn, this uses lastCommittedTxn as the floor when no readers exist.
// This prevents checkpoint from allocating pages that were just freed.
func (d *DB) minReaderTxnForCheckpoint(lastCommittedTxn uint64) uint64 {
	// Start with last committed transaction as minimum
	// This ensures pages freed by that transaction won't be reallocated during checkpoint
	minTxnID := lastCommittedTxn

	// Consider active write transaction (should not exist during checkpoint, but be safe)
	if d.writerTx != nil {
		if d.writerTx.txnID < minTxnID {
			minTxnID = d.writerTx.txnID
		}
	}

	// Consider all active read transactions
	for _, tx := range d.readerTxs {
		if tx.txnID < minTxnID {
			minTxnID = tx.txnID
		}
	}

	return minTxnID
}

// releasePages calls Release on the freelist with the given minimum transaction ID
func (d *DB) releasePages(minTxn uint64) {
	// Need to access the PageManager's freelist
	dm := d.store.pager
	dm.ReleasePages(minTxn)

	// Cleanup wal Page latches for checkpointed pages visible to all readers
	meta := dm.GetMeta()
	d.wal.CleanupLatch(meta.CheckpointTxnID, minTxn)

	// Cleanup relocated Page versions that are no longer needed
	d.store.cache.CleanupRelocatedVersions(minTxn)
}

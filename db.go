package fredb

import (
	"math"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/storage"
	"fredb/internal/wal"
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
	mu     sync.Mutex // Lock only for writers
	pager  *storage.PageManager
	cache  *cache.PageCache
	root   atomic.Pointer[base.Node] // Atomic root pointer for lock-free readers
	wal    *wal.WAL                  // Write-ahead log for durability
	closed atomic.Bool               // Database closed flag

	// Transaction state
	writerTx  atomic.Pointer[Tx] // Current write transaction (nil if none)
	readerTxs sync.Map           // Active read transactions (map[*Tx]struct{})
	nextTxnID atomic.Uint64      // Monotonic transaction ID counter (incremented for each write Tx)

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

	// Open WAL (DB owns WAL lifecycle)
	walPath := path + ".wal"
	newWAL, err := wal.NewWAL(walPath, opts.walSyncMode, opts.walBytesPerSync)
	if err != nil {
		return nil, err
	}

	pager, err := storage.NewPageManager(path)
	if err != nil {
		_ = newWAL.Close()
		return nil, err
	}

	// Page size is 4096 bytes, so 256 pages per MB
	c := cache.NewPageCache(opts.maxCacheSizeMB*256, pager)

	// Initialize root (was newTree logic)
	var root *base.Node
	meta := pager.GetMeta()

	if meta.RootPageID != 0 {
		// Load existing root
		rootPage, err := pager.ReadPage(meta.RootPageID)
		if err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}

		root = &base.Node{
			PageID: meta.RootPageID,
			Dirty:  false,
		}

		if err := root.Deserialize(rootPage); err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}
	} else {
		// Create new root - always a branch node, even when empty
		rootPageID, err := pager.AllocatePage()
		if err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}

		// Create initial empty leaf
		leafPageID, err := pager.AllocatePage()
		if err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}

		leaf := &base.Node{
			PageID:   leafPageID,
			Dirty:    true,
			IsLeaf:   true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}

		// Serialize and write the leaf
		leafPage, err := leaf.Serialize(0)
		if err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}
		if err := pager.WritePage(leafPageID, leafPage); err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}

		// Root is always a branch, pointing to the single leaf
		root = &base.Node{
			PageID:   rootPageID,
			Dirty:    true,
			IsLeaf:   false,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   nil,
			Children: []base.PageID{leafPageID},
		}

		// Serialize and write the root
		rootPage, err := root.Serialize(0)
		if err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}
		if err := pager.WritePage(rootPageID, rootPage); err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}

		meta.RootPageID = rootPageID
		if err := pager.PutMeta(meta); err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}

		// CRITICAL: Sync pager to persist the initial allocations
		// This ensures pages 3 and 4 (root and leaf) are not returned by future AllocatePage() calls
		if err := pager.Sync(); err != nil {
			_ = newWAL.Close()
			_ = pager.Close()
			return nil, err
		}
	}

	db := &DB{
		pager:    pager,
		cache:    c,
		wal:      newWAL,
		releaseC: make(chan uint64),
		stopC:    make(chan struct{}),
	}
	db.root.Store(root)            // Atomic store of initial root
	db.nextTxnID.Store(meta.TxnID) // Resume from last committed TxnID

	// Recover uncommitted newWAL entries into cache
	if err := db.recoverFromWAL(); err != nil {
		return nil, err
	}

	// Start background releaser goroutine
	db.wg.Add(1)
	go db.backgroundReleaser()

	return db, nil
}

// recoverFromWAL replays uncommitted wal entries into cache after startup.
// This recovers transactions that were committed to wal but not yet checkpointed to disk.
func (db *DB) recoverFromWAL() error {
	meta := db.pager.GetMeta()
	checkpointTxn := meta.CheckpointTxnID

	return db.wal.Replay(checkpointTxn, func(pageID base.PageID, page *base.Page) error {
		// Create empty node and Deserialize pages data into it
		node := &base.Node{}
		if err := node.Deserialize(page); err != nil {
			return err
		}

		header := page.Header()

		// Load into cache as hot version (ready for next reader)
		db.cache.Put(pageID, header.TxnID, node)

		// Rebuild wal latch to prevent stale disk reads
		db.wal.Pages.Store(pageID, header.TxnID)

		return nil
	})
}

func (db *DB) Get(key []byte) ([]byte, error) {
	var result []byte
	err := db.View(func(tx *Tx) error {
		val, err := tx.Get(key)
		if err != nil {
			return err
		}
		result = val
		return nil
	})
	return result, err
}

func (db *DB) Set(key, value []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.Set(key, value)
	})
}

func (db *DB) Delete(key []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.Delete(key)
	})
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	// Check if database is closed (lock-free atomic check)
	if db.closed.Load() {
		return nil, ErrDatabaseClosed
	}

	if writable {
		// Writers need exclusive lock
		db.mu.Lock()
		defer db.mu.Unlock()

		// Recheck closed flag after acquiring lock
		if db.closed.Load() {
			return nil, ErrDatabaseClosed
		}

		// Enforce single writer rule
		if db.writerTx.Load() != nil {
			return nil, ErrTxInProgress
		}

		// Writers get a new unique txnID (atomic increment)
		txnID := db.nextTxnID.Add(1)

		// Create write transaction
		tx := &Tx{
			db:       db,
			txnID:    txnID,
			writable: true,
			root:     db.root.Load(), // Atomic load of current root
			pages:    make(map[base.PageID]*base.Node),
			pending:  make(map[base.PageID]struct{}),
			freed:    make(map[base.PageID]struct{}),
			done:     false,
		}

		// Register writer (atomic store)
		db.writerTx.Store(tx)

		return tx, nil
	}

	// READERS: NO LOCK - completely lock-free path
	// Capture the current committed txnID from metadata
	meta := db.pager.GetMeta()
	committedTxnID := meta.TxnID

	// Create read transaction
	tx := &Tx{
		db:       db,
		txnID:    committedTxnID, // Readers use committed txnID as snapshot
		writable: false,
		root:     db.root.Load(), // Atomic load of current root
		pending:  make(map[base.PageID]struct{}),
		freed:    make(map[base.PageID]struct{}),
		done:     false,
	}

	// Register reader (lock-free via sync.Map)
	db.readerTxs.Store(tx, struct{}{})

	return tx, nil
}

// View executes a function within a read-only transaction.
// If the function returns an error, the transaction is rolled back.
// If the function returns nil, the transaction is rolled back (read-only).
func (db *DB) View(fn func(*Tx) error) error {
	tx, err := db.Begin(false)
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
func (db *DB) Update(fn func(*Tx) error) error {
	tx, err := db.Begin(true)
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

func (db *DB) Close() error {
	// Stop background goroutines
	select {
	case <-db.stopC:
		// Already closed
	default:
		close(db.stopC)
		db.wg.Wait()
	}

	// Force sync wal before checkpoint to ensure all data is durable
	if err := db.wal.ForceSync(); err != nil {
		return err
	}

	// Final checkpoint to flush wal
	if err := db.checkpoint(); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Mark database as closed
	db.closed.Store(true)

	// Flush root if dirty (was algo.close logic)
	root := db.root.Load()
	if root != nil && root.Dirty {
		meta := db.pager.GetMeta()
		page, err := root.Serialize(meta.TxnID)
		if err != nil {
			return err
		}
		if err := db.pager.WritePage(root.PageID, page); err != nil {
			return err
		}
		root.Dirty = false
	}

	// Flush cache
	if db.cache != nil {
		if err := db.cache.FlushDirty(db.pager); err != nil {
			return err
		}
	}

	// Close WAL
	if err := db.wal.Close(); err != nil {
		return err
	}

	// Close pager
	return db.pager.Close()
}

// backgroundReleaser periodically releases pending pages when safe
func (db *DB) backgroundReleaser() {
	defer db.wg.Done()

	for {
		select {
		case <-db.releaseC:
			// Reader-triggered release - recalculate minimum
			minTxn := db.minReaderTxn()
			db.releasePages(minTxn)

		case <-db.stopC:
			// Shutdown - release everything
			db.releasePages(math.MaxUint64)
			return
		}
	}
}

// checkpoint writes wal transactions to main file and truncates wal
func (db *DB) checkpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Skip checkpoint if there's an active write transaction
	// This prevents corruption from checkpoint allocating pages that are being freed
	if db.writerTx.Load() != nil {
		return nil
	}

	pm := db.pager

	// get current transaction boundary
	meta := pm.GetMeta()
	checkpointTxn := meta.CheckpointTxnID
	currentTxn := meta.TxnID

	// Nothing to checkpoint
	if currentTxn <= checkpointTxn {
		return nil
	}

	// get minimum reader txnID to determine which disk versions need preservation
	// IMPORTANT: If no readers, use currentTxn (last committed) not nextTxnID
	// This prevents checkpoint from allocating pages freed by the just-committed transaction
	minReaderTxn := db.minReaderTxnForCheckpoint(currentTxn)

	// Before replaying wal, temporarily prevent allocation of recently freed pages
	// This ensures checkpoint won't allocate pages freed by transactions being checkpointed
	pm.PreventAllocationUpTo(currentTxn)
	defer pm.AllowAllAllocations()

	// Replay wal from last checkpoint to current
	// IMPORTANT: Idempotent replay - skip pages already at correct version
	err := db.wal.Replay(checkpointTxn, func(pageID base.PageID,
		newPage *base.Page) error {
		// Read current disk version BEFORE overwriting
		// Note: We need to bypass the wal latch check for this read
		oldPage, readErr := pm.ReadPage(pageID)

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
				db.wal.Pages.Delete(pageID) // Clear latch even if skipping
				return nil
			}

			// If any active reader might need this old version, relocate it
			// Preserve if oldTxnID < minReaderTxn (old version still potentially visible to readers)
			if oldTxnID < minReaderTxn && oldHeader.NumKeys > 0 {
				// Allocate free Page for relocation
				relocPageID, allocErr := pm.AllocatePage()
				if allocErr != nil {
					return allocErr
				}

				// Write old disk version to relocated Page
				if writeErr := pm.WritePage(relocPageID, oldPage); writeErr != nil {
					return writeErr
				}

				// Add to VersionMap so readers can find it
				db.pager.TrackRelocation(pageID, oldTxnID, relocPageID)

				// NOTE: Do NOT call FreePending here! The relocated Page will be freed
				// later by CleanupRelocatedVersions when no readers need it anymore.
				// Calling FreePending here would cause a double-free bug.
			}
		}

		// Now overwrite disk with new checkpointed version
		if err := pm.WritePage(pageID, newPage); err != nil {
			return err
		}

		// Remove wal latch immediately after writing to disk
		// This allows readers to access the Page from disk instead of being blocked
		db.wal.Pages.Delete(pageID)

		return nil
	})
	if err != nil {
		return err
	}

	// Fsync main B-tree file
	if err := pm.Sync(); err != nil {
		return err
	}

	// Update meta with new checkpoint marker
	// IMPORTANT: Make a copy of meta to avoid overwriting concurrent updates
	freshMeta := pm.GetMeta() // Dereference to copy
	freshMeta.CheckpointTxnID = currentTxn
	if err := pm.PutMeta(freshMeta); err != nil {
		return err
	}

	// Fsync meta Page (PutMeta already writes, but need explicit fsync)
	if err := pm.Sync(); err != nil {
		return err
	}

	// Truncate wal up to checkpoint
	if err := db.wal.Truncate(currentTxn); err != nil {
		return err
	}

	// Evict superseded checkpointed versions from cache
	// Only evicts old versions that are superseded by newer versions visible to all readers
	_ = db.cache.EvictCheckpointed(minReaderTxn)

	// Cleanup wal Page latches for checkpointed pages visible to all readers
	db.wal.CleanupLatch(checkpointTxn, minReaderTxn)

	return nil
}

// minReaderTxn returns the minimum transaction ID across all active transactions (readers + writer).
// This determines which pending pages can be safely released to the freelist.
func (db *DB) minReaderTxn() uint64 {
	// Start with current next transaction ID
	minTxnID := db.nextTxnID.Load()

	// Consider active write transaction (atomic load)
	if writerTx := db.writerTx.Load(); writerTx != nil {
		if writerTx.txnID < minTxnID {
			minTxnID = writerTx.txnID
		}
	}

	// Consider all active read transactions (lock-free via sync.Map)
	db.readerTxs.Range(func(key, value interface{}) bool {
		tx := key.(*Tx)
		if tx.txnID < minTxnID {
			minTxnID = tx.txnID
		}
		return true // continue iteration
	})

	return minTxnID
}

// minReaderTxnForCheckpoint returns minimum snapshot version being viewed by active readers.
// Unlike minReaderTxn, this uses lastCommittedTxn as the floor when no readers exist.
// This prevents checkpoint from allocating pages that were just freed.
func (db *DB) minReaderTxnForCheckpoint(lastCommittedTxn uint64) uint64 {
	// Start with last committed transaction as minimum
	// This ensures pages freed by that transaction won't be reallocated during checkpoint
	minTxnID := lastCommittedTxn

	// Consider active write transaction (should not exist during checkpoint, but be safe)
	if writerTx := db.writerTx.Load(); writerTx != nil {
		if writerTx.txnID < minTxnID {
			minTxnID = writerTx.txnID
		}
	}

	// Consider all active read transactions (lock-free via sync.Map)
	// Now that readers use committed txnID as their snapshot, txnID == snapshot version
	db.readerTxs.Range(func(key, value interface{}) bool {
		tx := key.(*Tx)
		if tx.txnID < minTxnID {
			minTxnID = tx.txnID
		}
		return true // continue iteration
	})

	return minTxnID
}

// releasePages calls Release on the freelist with the given minimum transaction ID
func (db *DB) releasePages(minTxn uint64) {
	// Need to access the PageManager's freelist
	pm := db.pager
	pm.ReleasePages(minTxn)

	// Cleanup wal Page latches for checkpointed pages visible to all readers
	meta := pm.GetMeta()
	db.wal.CleanupLatch(meta.CheckpointTxnID, minTxn)

	// Cleanup relocated Page versions that are no longer needed
	db.cache.CleanupRelocatedVersions(minTxn)
}

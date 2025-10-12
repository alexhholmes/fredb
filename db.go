package fredb

import (
	"math"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/coordinator"
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
	coord  *coordinator.Coordinator
	cache  *cache.Cache
	closed atomic.Bool // Database closed flag

	// Transaction state
	writer    atomic.Pointer[Tx] // Current write transaction (nil if none)
	readers   sync.Map           // Active read transactions (map[*Tx]struct{})
	nextTxnID atomic.Uint64      // Monotonic transaction ID counter (incremented for each write Tx)

	// Background pending page releaser
	releaseC chan uint64    // Trigger release (unbuffered)
	stopC    chan struct{}  // Shutdown signal
	wg       sync.WaitGroup // Clean shutdown

	options DBOptions // Store options for reference
}

func Open(path string, options ...DBOption) (*DB, error) {
	// Apply options
	opts := defaultDBOptions()
	for _, opt := range options {
		opt(&opts)
	}

	coord, err := coordinator.NewCoordinator(path)
	if err != nil {
		return nil, err
	}

	// Page size is 4096 bytes, so 256 pages per MB
	c := cache.NewCache(opts.maxCacheSizeMB*256, coord)

	// Initialize root (was newTree logic)
	var root *base.Node
	meta := coord.GetMeta()

	if meta.RootPageID != 0 {
		// Load existing root
		rootPage, err := coord.ReadPage(meta.RootPageID)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		root = &base.Node{
			PageID: meta.RootPageID,
			Dirty:  false,
		}

		if err := root.Deserialize(rootPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// Bundle root with metadata and make visible atomically
		if err := coord.PutSnapshot(meta, root); err != nil {
			_ = coord.Close()
			return nil, err
		}
		coord.CommitSnapshot()
	} else {
		// Create new root - always a branch node, even when empty
		rootPageID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		// Create initial empty leaf
		leafPageID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		leaf := &base.Node{
			PageID:   leafPageID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}

		// Serialize and write the leaf
		leafPage, err := leaf.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(leafPageID, leafPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// Root is always a branch, pointing to the single leaf
		root = &base.Node{
			PageID:   rootPageID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   nil,
			Children: []base.PageID{leafPageID},
		}

		// Serialize and write the root
		rootPage, err := root.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(rootPageID, rootPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		meta.RootPageID = rootPageID
		if err := coord.PutSnapshot(meta, root); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// CRITICAL: Sync coord to persist the initial allocations
		// This ensures root and leaf are not returned by future AllocatePage() calls
		if err := coord.Sync(); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// Make metapage AND root visible to readers atomically
		coord.CommitSnapshot()
	}

	db := &DB{
		coord:    coord,
		cache:    c,
		releaseC: make(chan uint64),
		stopC:    make(chan struct{}),
		options:  opts,
	}
	db.nextTxnID.Store(meta.TxID) // Resume from last committed TxID

	// Start background releaser goroutine
	db.wg.Add(1)
	go func(db *DB) {
		defer db.wg.Done()
		for {
			select {
			case <-db.releaseC:
				// Reader-triggered release - recalculate minimum
				// Start with current next transaction ID
				minTxnID := db.nextTxnID.Load()

				// Consider active write transaction (atomic load)
				if writerTx := db.writer.Load(); writerTx != nil {
					if writerTx.txnID < minTxnID {
						minTxnID = writerTx.txnID
					}
				}

				// Consider all active read transactions (lock-free via sync.Map)
				db.readers.Range(func(key, value interface{}) bool {
					tx := key.(*Tx)
					if tx.txnID < minTxnID {
						minTxnID = tx.txnID
					}
					return true // continue iteration
				})

				db.coord.ReleasePages(minTxnID)
				db.cache.CleanupRelocatedVersions(minTxnID)
			case <-db.stopC:
				// Shutdown - release everything
				db.coord.ReleasePages(math.MaxUint64)
				db.cache.CleanupRelocatedVersions(math.MaxUint64)
				return
			}
		}
	}(db)

	return db, nil
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
		if db.writer.Load() != nil {
			return nil, ErrTxInProgress
		}

		// Writers get a new unique txnID (atomic increment)
		txnID := db.nextTxnID.Add(1)

		// Atomically load current snapshot (meta + root)
		snapshot := db.coord.GetSnapshot()

		// Create write transaction
		tx := &Tx{
			db:            db,
			txnID:         txnID,
			writable:      true,
			root:          snapshot.Root, // Atomic snapshot of root
			pages:         make(map[base.PageID]*base.Node),
			pending:       make(map[base.PageID]struct{}),
			freed:         make(map[base.PageID]struct{}),
			done:          false,
			nextVirtualID: -1, // Start virtual page IDs at -1
		}

		// Register writer (atomic store)
		db.writer.Store(tx)

		return tx, nil
	}

	// READERS: NO LOCK - completely lock-free path
	// Atomically capture current snapshot (meta + root)
	snapshot := db.coord.GetSnapshot()

	// Create read transaction
	tx := &Tx{
		db:       db,
		txnID:    snapshot.Meta.TxID, // Readers use committed txnID as snapshot
		writable: false,
		root:     snapshot.Root, // Atomic snapshot of root
		pending:  make(map[base.PageID]struct{}),
		freed:    make(map[base.PageID]struct{}),
		done:     false,
	}

	// Register reader (lock-free via sync.Map)
	db.readers.Store(tx, struct{}{})

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

	db.mu.Lock()
	defer db.mu.Unlock()

	// Mark database as closed
	db.closed.Store(true)

	// Flush root if dirty (was algo.close logic)
	snapshot := db.coord.GetSnapshot()
	if snapshot.Root != nil && snapshot.Root.Dirty {
		page, err := snapshot.Root.Serialize(snapshot.Meta.TxID)
		if err != nil {
			return err
		}
		if err := db.coord.WritePage(snapshot.Root.PageID, page); err != nil {
			return err
		}
		snapshot.Root.Dirty = false
	}

	// Flush cache
	if db.cache != nil {
		if err := db.cache.FlushDirty(db.coord); err != nil {
			return err
		}
	}

	// Final sync to ensure durability
	if err := db.coord.Sync(); err != nil {
		return err
	}

	// Close coord
	return db.coord.Close()
}

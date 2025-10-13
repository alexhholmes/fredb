package fredb

import (
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/coordinator"
	"fredb/internal/storage"
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
	opts := DefaultDBOptions()
	for _, opt := range options {
		opt(&opts)
	}

	// Create disk storage
	store, err := storage.NewStorage(path)
	if err != nil {
		return nil, err
	}

	// Create cache (Page size is 4096 bytes, so 256 pages per MB)
	cacheInstance := cache.NewCache(opts.maxCacheSizeMB * 256)

	// Create coordinator with dependencies
	coord, err := coordinator.NewCoordinator(store, cacheInstance)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

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
		// NEW DATABASE - Create root tree (directory) + __root__ bucket

		// 1. Create root tree (directory for bucket metadata)
		rootPageID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		rootLeafID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		rootLeaf := &base.Node{
			PageID:   rootLeafID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}

		root = &base.Node{
			PageID:   rootPageID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   nil,
			Children: []base.PageID{rootLeafID},
		}

		// 2. Create __root__ bucket's tree (default namespace)
		rootBucketRootID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		rootBucketLeafID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		rootBucketLeaf := &base.Node{
			PageID:   rootBucketLeafID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}

		rootBucketRoot := &base.Node{
			PageID:   rootBucketRootID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   nil,
			Children: []base.PageID{rootBucketLeafID},
		}

		// 3. Create __versions__ bucket's tree (for version mappings)
		versionsBucketRootID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		versionsBucketLeafID, err := coord.AllocatePage()
		if err != nil {
			_ = coord.Close()
			return nil, err
		}

		versionsBucketLeaf := &base.Node{
			PageID:   versionsBucketLeafID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}

		versionsBucketRoot := &base.Node{
			PageID:   versionsBucketRootID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   nil,
			Children: []base.PageID{versionsBucketLeafID},
		}

		// 4. Create bucket metadata for __root__ bucket (16 bytes: RootPageID + Sequence)
		rootMetadata := make([]byte, 16)
		binary.LittleEndian.PutUint64(rootMetadata[0:8], uint64(rootBucketRootID))
		binary.LittleEndian.PutUint64(rootMetadata[8:16], 0) // Sequence = 0

		// 5. Create bucket metadata for __versions__ bucket
		versionsMetadata := make([]byte, 16)
		binary.LittleEndian.PutUint64(versionsMetadata[0:8], uint64(versionsBucketRootID))
		binary.LittleEndian.PutUint64(versionsMetadata[8:16], 0) // Sequence = 0

		// 6. Insert both bucket metadata entries into root tree's leaf
		// Need to insert in sorted order: __root__ < __versions__
		rootLeaf.Keys = append(rootLeaf.Keys, []byte("__root__"))
		rootLeaf.Values = append(rootLeaf.Values, rootMetadata)

		rootLeaf.Keys = append(rootLeaf.Keys, []byte("__versions__"))
		rootLeaf.Values = append(rootLeaf.Values, versionsMetadata)

		rootLeaf.NumKeys = 2

		// 7. Serialize and write all 6 pages (root tree + 2 buckets)
		leafPage, err := rootLeaf.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(rootLeafID, leafPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		rootPage, err := root.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(rootPageID, rootPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// Write __root__ bucket pages
		bucketLeafPage, err := rootBucketLeaf.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(rootBucketLeafID, bucketLeafPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		bucketRootPage, err := rootBucketRoot.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(rootBucketRootID, bucketRootPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// Write __versions__ bucket pages
		versionsBucketLeafPage, err := versionsBucketLeaf.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(versionsBucketLeafID, versionsBucketLeafPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		versionsBucketRootPage, err := versionsBucketRoot.Serialize(0)
		if err != nil {
			_ = coord.Close()
			return nil, err
		}
		if err := coord.WritePage(versionsBucketRootID, versionsBucketRootPage); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// 8. Update meta to point to root tree
		meta.RootPageID = rootPageID

		if err := coord.PutSnapshot(meta, root); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// 9. CRITICAL: Sync coord to persist the initial allocations
		// This ensures root and leaf are not returned by future AllocatePage() calls
		if err := coord.Sync(); err != nil {
			_ = coord.Close()
			return nil, err
		}

		// 10. Make metapage AND root visible to readers atomically
		coord.CommitSnapshot()
	}

	db := &DB{
		coord:    coord,
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
				minTxID := db.nextTxnID.Load()

				// Consider active write transaction (atomic load)
				if writerTx := db.writer.Load(); writerTx != nil {
					if writerTx.txID < minTxID {
						minTxID = writerTx.txID
					}
				}

				// Consider all active read transactions (lock-free via sync.Map)
				db.readers.Range(func(key, value interface{}) bool {
					tx := key.(*Tx)
					if tx.txID < minTxID {
						minTxID = tx.txID
					}
					return true // Continue iteration
				})

				db.coord.ReleasePages(minTxID)
			case <-db.stopC:
				// Shutdown - release everything
				db.coord.ReleasePages(math.MaxUint64)
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
			txID:          txnID,
			writable:      true,
			root:          snapshot.Root, // Atomic snapshot of root
			pages:         make(map[base.PageID]*base.Node),
			freed:         make(map[base.PageID]struct{}),
			done:          false,
			nextVirtualID: -1, // Start virtual page IDs at -1
			buckets:       make(map[string]*Bucket),
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
		txID:     snapshot.Meta.TxID, // Readers use committed txnID as snapshot
		writable: false,
		root:     snapshot.Root, // Atomic snapshot of root
		freed:    make(map[base.PageID]struct{}),
		done:     false,
		buckets:  make(map[string]*Bucket),
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

	// Final sync to ensure durability
	if err := db.coord.Sync(); err != nil {
		return err
	}

	// Close coord
	return db.coord.Close()
}

// Stats returns database statistics from the coordinator
func (db *DB) Stats() coordinator.Stats {
	return db.coord.Stats()
}

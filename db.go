package fredb

import (
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"fredb/internal/base"
	"fredb/internal/cache"
	"fredb/internal/pager"
	"fredb/internal/readslots"
	"fredb/internal/storage"
	"fredb/internal/writebuf"
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
	pager  *pager.Pager
	closed atomic.Bool // Database closed flag

	cache *cache.Cache    // Page cache
	store storage.Storage // Underlying storage

	// Transaction state
	writer      atomic.Pointer[Tx]     // Current write transaction (nil if none)
	readers     sync.Map               // Fallback for unbounded readers (used when readerSlots == nil)
	readerSlots *readslots.ReaderSlots // Fixed-size slots (nil if maxReaders == 0)
	nextTxID    atomic.Uint64          // Monotonic transaction ID counter (incremented for each write Tx)

	options Options // Store options for reference
}

func Open(path string, options ...DBOption) (*DB, error) {
	// Apply options
	opts := DefaultDBOptions()
	for _, opt := range options {
		opt(&opts)
	}

	// Create disk storage
	var store storage.Storage
	var err error
	if opts.syncMode == SyncEveryCommit {
		store, err = storage.NewDirectIO(path)
	} else {
		store, err = storage.NewMMap(path)
	}
	if err != nil {
		return nil, err
	}

	// Create cache with eviction callback for DirectIO buffer pooling
	var onEvict func(base.PageID, *base.Node)
	if directIO, ok := store.(*storage.DirectIO); ok {
		onEvict = func(_ base.PageID, n *base.Node) {
			// Return pooled buffer to DirectIO pool
			// pageData is the pooled buffer for clean nodes
			if n.PageData != nil && !n.Dirty {
				directIO.PutBuffer(n.PageData)
			}
		}
	}
	c := cache.NewCache(opts.maxCacheSizeMB*256, onEvict)

	// Create pager with dependencies
	pager, err := pager.NewPager(store, c)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	// Initialize root (was newTree logic)
	var root *base.Node
	meta := pager.GetMeta()

	if meta.RootPageID != 0 {
		// Load existing root
		rootPage, err := store.ReadPage(meta.RootPageID)
		if err != nil {
			_ = pager.Close()
			return nil, err
		}

		root = &base.Node{
			PageID: meta.RootPageID,
			Dirty:  false,
		}

		if err := root.Deserialize(rootPage); err != nil {
			_ = pager.Close()
			return nil, err
		}

		// Bundle root with metadata and make visible atomically
		if err := pager.PutSnapshot(meta, root); err != nil {
			_ = pager.Close()
			return nil, err
		}
		pager.CommitSnapshot()
	} else {
		// NEW DATABASE - Create root tree (directory) + __root__ bucket

		// 1. Create root tree (directory for bucket metadata)
		rootPageID, err := pager.AssignPageID()
		if err != nil {
			_ = pager.Close()
			return nil, err
		}

		rootLeafID, err := pager.AssignPageID()
		if err != nil {
			_ = pager.Close()
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
		rootBucketRootID, err := pager.AssignPageID()
		if err != nil {
			_ = pager.Close()
			return nil, err
		}

		rootBucketLeafID, err := pager.AssignPageID()
		if err != nil {
			_ = pager.Close()
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

		// 3. Create bucket metadata for __root__ bucket (16 bytes: RootPageID + Sequence)
		// We serialize manually since we don't have a Bucket object yet
		metadata := make([]byte, 16)
		binary.LittleEndian.PutUint64(metadata[0:8], uint64(rootBucketRootID))
		binary.LittleEndian.PutUint64(metadata[8:16], 0) // Sequence = 0

		// 4. Insert __root__ bucket metadata into root tree's leaf
		// We need to manually insert since we don't have a transaction yet
		rootLeaf.Keys = append(rootLeaf.Keys, []byte("__root__"))

		rootLeaf.Values = append(rootLeaf.Values, metadata)
		rootLeaf.NumKeys = 1

		// 5. Serialize and write all 4 pages
		leafPage := &base.Page{}
		err = rootLeaf.Serialize(0, leafPage)
		if err != nil {
			_ = pager.Close()
			return nil, err
		}
		if err = store.WritePage(rootLeafID, leafPage); err != nil {
			_ = pager.Close()
			return nil, err
		}

		rootPage := &base.Page{}
		err = root.Serialize(0, rootPage)
		if err != nil {
			_ = pager.Close()
			return nil, err
		}
		if err = store.WritePage(rootPageID, rootPage); err != nil {
			_ = pager.Close()
			return nil, err
		}

		bucketLeafPage := &base.Page{}
		err = rootBucketLeaf.Serialize(0, bucketLeafPage)
		if err != nil {
			_ = pager.Close()
			return nil, err
		}
		if err = store.WritePage(rootBucketLeafID, bucketLeafPage); err != nil {
			_ = pager.Close()
			return nil, err
		}

		bucketRootPage := &base.Page{}
		err = rootBucketRoot.Serialize(0, bucketRootPage)
		if err != nil {
			_ = pager.Close()
			return nil, err
		}
		if err = store.WritePage(rootBucketRootID, bucketRootPage); err != nil {
			_ = pager.Close()
			return nil, err
		}

		// 6. Update meta to point to root tree
		meta.RootPageID = rootPageID

		if err = pager.PutSnapshot(meta, root); err != nil {
			_ = pager.Close()
			return nil, err
		}

		// 7. CRITICAL: Sync pager to persist the initial allocations
		// This ensures root and leaf are not returned by future AssignPageID() calls
		if err = store.Sync(); err != nil {
			_ = pager.Close()
			return nil, err
		}

		// 8. Make metapage AND root visible to readers atomically
		pager.CommitSnapshot()
	}

	db := &DB{
		pager:   pager,
		options: opts,
		cache:   c,
		store:   store,
	}
	db.nextTxID.Store(meta.TxID) // Resume from last committed TxID

	// Initialize reader tracking based on maxReaders option
	if opts.maxReaders > 0 {
		db.readerSlots = readslots.NewReaderSlots(opts.maxReaders)
	}
	// else: readerSlots stays nil, use sync.Map fallback

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
		txnID := db.nextTxID.Add(1)

		// Atomically load current snapshot (meta + root)
		snapshot := db.pager.GetSnapshot()

		// Create write transaction
		tx := &Tx{
			db:            db,
			txID:          txnID,
			writable:      true,
			root:          snapshot.Root, // Atomic snapshot of root
			pages:         make(map[base.PageID]*base.Node),
			acquired:      make(map[base.PageID]struct{}),
			freed:         make(map[base.PageID]struct{}),
			deletes:       make(map[string]base.PageID),
			done:          false,
			nextVirtualID: -1, // Start virtual page IDs at -1
			buckets:       make(map[string]*Bucket),
			writeBuf:      writebuf.NewBuffer(db.options.writeBufferSize),
		}

		// Register writer (atomic store)
		db.writer.Store(tx)

		return tx, nil
	}

	// READERS: NO LOCK - completely lock-free path
	// Atomically capture current snapshot (meta + root)
	snapshot := db.pager.GetSnapshot()

	// Create read transaction
	tx := &Tx{
		db:       db,
		txID:     snapshot.Meta.TxID, // Readers use committed txnID as snapshot
		writable: false,
		root:     snapshot.Root, // Atomic snapshot of root
		acquired: make(map[base.PageID]struct{}),
		buckets:  make(map[string]*Bucket),
		done:     false,
	}

	// Register reader using hybrid approach
	if db.readerSlots != nil {
		slot, err := db.readerSlots.Register(tx.txID)
		if err != nil {
			return nil, err
		}
		tx.readerSlot = slot
		tx.usedSlot = true
	} else {
		db.readers.Store(tx, struct{}{})
	}

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
	db.mu.Lock()
	defer db.mu.Unlock()

	// Mark database as closed
	db.closed.Store(true)

	// Release all pending pages
	db.pager.ReleasePages(math.MaxUint64)

	// Flush root if dirty (was algo.close logic)
	snapshot := db.pager.GetSnapshot()
	if snapshot.Root != nil && snapshot.Root.Dirty {
		page := &base.Page{}
		err := snapshot.Root.Serialize(snapshot.Meta.TxID, page)
		if err != nil {
			return err
		}
		if err := db.store.WritePage(snapshot.Root.PageID, page); err != nil {
			return err
		}
		snapshot.Root.Dirty = false
	}

	// Final sync to ensure durability
	if err := db.store.Sync(); err != nil {
		return err
	}

	// Close pager
	return db.pager.Close()
}

func (db *DB) Stats() pager.Stats {
	return db.pager.Stats()
}

// tryReleasePages releases pages that are safe to reuse based on active transactions.
// Hybrid approach: uses slots if bounded, else scans sync.Map
func (db *DB) tryReleasePages() {
	// Start with NEXT transaction ID (last assigned + 1)
	minTxID := db.nextTxID.Load() + 1

	// Consider active write transaction
	if writerTx := db.writer.Load(); writerTx != nil {
		if writerTx.txID < minTxID {
			minTxID = writerTx.txID
		}
	}

	// Get minimum reader txID using hybrid approach
	if db.readerSlots != nil {
		// Fixed slots: O(N) scan where N is bounded
		readerMinTxID := db.readerSlots.GetMinTxID()
		if readerMinTxID < minTxID {
			minTxID = readerMinTxID
		}
	} else {
		// Unbounded: scan sync.Map
		db.readers.Range(func(key, value interface{}) bool {
			tx := key.(*Tx)
			if tx.txID < minTxID {
				minTxID = tx.txID
			}
			return true
		})
	}

	db.pager.ReleasePages(minTxID)
}

// collectTreePages recursively collects all page IDs in a B+ tree via post-order traversal
func collectTreePages(tx *Tx, pageID base.PageID, pageIDs *[]base.PageID) error {
	// Load node from disk
	node, ok := tx.db.pager.LoadNode(pageID)
	if !ok {
		return ErrCorruption
	}

	// If branch node, recursively collect children first (post-order)
	if !node.IsLeaf() {
		for _, childID := range node.Children {
			if err := collectTreePages(tx, childID, pageIDs); err != nil {
				return err
			}
		}
	}

	// Add this node's page ID after processing children
	*pageIDs = append(*pageIDs, pageID)

	return nil
}

// freeTree frees all pages in a B+ tree (bucket) given its root page ID
func (db *DB) freeTree(rootID base.PageID) error {
	// Start new read transaction to safely traverse tree
	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Collect all page IDs to free
	pageIDs := make([]base.PageID, 0)
	if err := collectTreePages(tx, rootID, &pageIDs); err != nil {
		return err
	}

	// Now free them all at once
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, pageID := range pageIDs {
		db.pager.FreePage(pageID) // Add to freelist
	}

	return nil
}

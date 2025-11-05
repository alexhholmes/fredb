package fredb

import (
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"

	"github.com/google/btree"

	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/cache"
	internal "github.com/alexhholmes/fredb/internal/db"
	"github.com/alexhholmes/fredb/internal/pager"
	"github.com/alexhholmes/fredb/internal/storage"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	// Put conservatively to ensure branch nodes can hold multiple Keys.
	// With 4KB pages, limiting Keys to 1KB allows ~3 Keys per branch Node.
	MaxKeySize = 1024

	// MaxValueSize is the maximum length of a value, in bytes.
	// Without overflow pages, values must fit inline in leaf pages.
	// PageSize (4096) - PageHeaderSize (24) - LeafElementSize (16) - MaxKeySize (1024) = 3032 bytes
	MaxValueSize = base.MaxValueSize // 256 MB
)

type DB struct {
	mu    sync.Mutex // Lock only for writers
	pager *pager.Pager
	store *storage.Storage
	log   Logger

	writer   atomic.Pointer[Tx]    // Current write transaction (nil if none)
	readers  *internal.ReaderSlots // Fixed-size slots (nil if MaxReaders == 0)
	nextTxID atomic.Uint64         // Monotonic transaction ID counter (incremented for each write Tx)

	options Options        // Store options for reference
	closed  atomic.Bool    // Database closed flag
	txWg    sync.WaitGroup // Track active transactions for graceful shutdown
}

func Open(path string, options ...Option) (*DB, error) {
	// Apply options
	opts := &Options{}
	DefaultOptions()(opts)
	for _, opt := range options {
		opt(opts)
	}

	logger := opts.Logger
	logger.Info("Opening database")

	// Create disk storage
	store, err := storage.New(path)
	if err != nil {
		return nil, err
	}

	// Create cache (no eviction callback needed - nodes own their allocations)
	c := cache.NewCache(opts.CacheSizeMB*256, nil)

	// Create pager with dependencies
	pg, err := pager.NewPager(pager.SyncMode(opts.SyncMode), store, c)
	if err != nil {
		logger.Error("Fatal error opening database", "error", err)
		_ = store.Close()
		return nil, err
	}

	meta := pg.GetSnapshot().Meta

	if meta.RootPageID != 0 {
		logger.Info("Loading existing database")

		// Load existing root
		rootPage, err := store.ReadPage(meta.RootPageID)
		if err != nil {
			_ = pg.Close()
			return nil, err
		}

		root := &base.Node{}
		if err = root.Deserialize(rootPage); err != nil {
			if errors.Is(err, ErrCorruption) {
				logger.Error("Corrupted root page")
			}
			_ = pg.Close()
			return nil, err
		}

		// Sanity check
		if meta.RootPageID != root.PageID {
			logger.Error("Invalid root pageID", "given", meta.RootPageID, "expected", root.PageID)
			return nil, fmt.Errorf("invalid pageID: %d, expected %d", root.PageID, meta.RootPageID)
		}

		// Bundle root with metadata and make visible atomically
		if err = pg.PutSnapshot(meta, root); err != nil {
			_ = pg.Close()
			return nil, err
		}
		pg.CommitSnapshot()
	} else {
		logger.Info("Creating new database")
		err = internal.Initialize(meta, pg, store)
		if err != nil {
			logger.Error("Fatal error initializing database", "error", err)
			_ = pg.Close()
			return nil, err
		}
	}

	db := &DB{
		pager:   pg,
		options: *opts,
		store:   store,
		log:     logger,
	}
	db.nextTxID.Store(meta.TxID) // Next writer will get TxID + 1

	// Initialize reader tracking based on MaxReaders option
	if opts.MaxReaders < 1 {
		return nil, ErrInvalidMaxReaders
	}
	db.readers = internal.NewReaderSlots(opts.MaxReaders)

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

func (db *DB) Put(key, value []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.Put(key, value)
	})
}

func (db *DB) Delete(key []byte) error {
	return db.Update(func(tx *Tx) error {
		return tx.Delete(key)
	})
}

func (db *DB) Begin(writable bool) (*Tx, error) {
	db.txWg.Add(1)
	defer db.txWg.Done()

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
		txnID := db.nextTxID.Add(1) - 1

		// Atomically load current snapshot (meta + root)
		snapshot := db.pager.GetSnapshot()

		// Create write transaction
		tx := &Tx{
			db:       db,
			txID:     txnID,
			writable: true,
			root:     snapshot.Root, // Atomic snapshot of root
			pages: btree.NewG[*base.Node](2, func(a, b *base.Node) bool {
				return a.PageID < b.PageID
			}),
			freed:   make(map[base.PageID]struct{}),
			deletes: make(map[string]base.PageID),
			done:    false,
			buckets: make(map[string]*Bucket),
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
		buckets:  make(map[string]*Bucket),
		done:     false,
	}

	// Register reader in a slot
	unregister, err := db.readers.Register(tx.txID)
	if err != nil {
		return nil, err
	}
	tx.unregister = unregister

	return tx, nil
}

// View executes a function within a read-only transaction.
// If the function returns an error, the transaction is rolled back.
// If the function returns nil, the transaction is rolled back (read-only).
func (db *DB) View(fn func(*Tx) error) error {
	db.txWg.Add(1)
	defer db.txWg.Done()

	// Atomically check closed flag and register transaction
	if db.closed.Load() {
		return ErrDatabaseClosed
	}

	tx, err := db.Begin(false)
	if errors.Is(err, ErrTooManyReaders) {
		db.log.Warn("Too many readers", "max_readers", db.options.MaxReaders)
	} else if errors.Is(err, ErrCorruption) {
		db.log.Error("Database corruption detected")
	} else if err != nil {
		db.log.Warn("Read transaction error", "error", err)
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
	db.txWg.Add(1)
	defer db.txWg.Done()
	defer func() {
		db.log.Info("Transaction commit", "tx_id", db.nextTxID.Load())
	}()

	// Atomically check closed flag and register transaction
	if db.closed.Load() {
		return ErrDatabaseClosed
	}

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func(tx *Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := fn(tx); err != nil {
		if errors.Is(err, ErrCorruption) || errors.Is(err, ErrInvalidChecksum) {
			db.log.Error("Database corruption detected", "tx_id", db.nextTxID.Load())
		} else if errors.Is(err, ErrPageOverflow) {
			db.log.Error("Page overflow", "tx_id", db.nextTxID.Load())
		} else {
			db.log.Warn("Transaction error", "tx_id", db.nextTxID.Load(), "err", err)
		}
		return err
	}

	return tx.Commit()
}

// Compact creates a new compacted database at the given destination path.
// The current database remains open and usable, it is up to the reader of the
// database to close the old database. Returns a new DB instance for the
// compacted database.
func (db *DB) Compact(dst string) (*DB, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	src, err := db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func(src *Tx) {
		_ = src.Rollback()
	}(src)

	// Create disk storage
	store, err := storage.New(dst)
	if err != nil {
		return nil, err
	}

	// Create cache (no eviction callback needed - nodes own their allocations)
	c := cache.NewCache(db.options.CacheSizeMB*256, nil)

	// Create pager with dependencies
	pg, err := pager.NewPager(pager.SyncMode(db.options.SyncMode), store, c)
	if err != nil {
		db.log.Error("Fatal error opening compacted database", "error", err)
		_ = store.Close()
		return nil, err
	}

	// Check if database is new or existing
	meta := pg.GetSnapshot().Meta
	if meta.RootPageID != 0 {
		db.log.Error("Database already exists at compacted destination", "path", dst)
		_ = pg.Close()
		return nil, ErrDatabaseExists
	}

	err = internal.Initialize(meta, pg, store)
	if err != nil {
		db.log.Error("Fatal error initializing compacted database", "error", err)
		_ = pg.Close()
		return nil, err
	}

	compacted := &DB{
		pager:   pg,
		store:   store,
		options: db.options,
		log:     db.log,
	}
	compacted.nextTxID.Store(1) // Next writer will get TxID + 1
	compacted.readers = internal.NewReaderSlots(compacted.options.MaxReaders)

	// Walk the source database and copy live data to the new database.
	// Use batched transactions to prevent OOM on large databases.
	const batchSize = 10000 // Keys per transaction

	// Copy __root__ keys in batches
	cursor := src.Cursor()
	err = internal.CopyInBatches(cursor, batchSize, func(keys, values [][]byte) error {
		return compacted.Update(func(dstTx *Tx) error {
			for i := 0; i < len(keys); i++ {
				if err = dstTx.Put(keys[i], values[i]); err != nil {
					return fmt.Errorf("failed to put key during compaction: %w", err)
				}
			}
			return nil
		})
	})
	if err != nil {
		db.log.Error("Failed to copy root keys during compaction", "error", err)
		_ = compacted.Close()
		_ = os.Remove(dst)
		return nil, fmt.Errorf("compaction failed: %w", err)
	}

	// Copy buckets in batches
	err = src.ForEachBucket(func(name []byte, bucket *Bucket) error {
		// Create bucket first
		if err = compacted.Update(func(dstTx *Tx) error {
			_, err = dstTx.CreateBucket(name)
			return err
		}); err != nil {
			return fmt.Errorf("failed to create bucket %s during compaction: %w", string(name), err)
		}

		// Copy bucket contents in batches
		cursor = bucket.Cursor()
		return internal.CopyInBatches(cursor, batchSize, func(keys, values [][]byte) error {
			return compacted.Update(func(dstTx *Tx) error {
				dstBucket := dstTx.Bucket(name)
				if dstBucket == nil {
					return fmt.Errorf("bucket %s not found", string(name))
				}
				for i := 0; i < len(keys); i++ {
					if err = dstBucket.Put(keys[i], values[i]); err != nil {
						return fmt.Errorf("failed to put key in bucket: %w", err)
					}
				}
				return nil
			})
		})
	})
	if err != nil {
		db.log.Error("Failed to copy data during compaction", "error", err)
		_ = compacted.Close()
		_ = os.Remove(dst)
		return nil, fmt.Errorf("compaction failed: %w", err)
	}

	return compacted, nil
}

func (db *DB) Close() error {
	// Mark database as closed - new transactions will fail
	db.closed.Store(true)

	// Wait for all active transactions to complete
	db.txWg.Wait()
	db.mu.Lock()

	// Release all pending pages
	db.pager.Release(math.MaxUint64)

	// flush root if dirty (was algo.close logic)
	snapshot := db.pager.GetSnapshot()
	if snapshot.Root != nil && snapshot.Root.Dirty {
		rootPage, _, err := snapshot.Root.Serialize(snapshot.Meta.TxID, db.pager.Allocate)
		if err != nil {
			return err
		}

		err = db.store.WritePage(snapshot.Root.PageID, rootPage)
		if err != nil {
			return err
		}

		snapshot.Root.Dirty = false
	}

	// Safe to unlock, all pager operations finished.
	// Need to unlock because pager.Close may try to acquire locks causing
	// a deadlock if we hold db.mu here.
	db.mu.Unlock()

	// Final sync to ensure durability
	if err := db.store.Sync(); err != nil {
		return err
	}

	// Close pager
	err := db.pager.Close()
	if err != nil {
		return err
	}

	db.log.Info("Database closed")
	return nil
}

func (db *DB) Stats() pager.Stats {
	return db.pager.Stats()
}

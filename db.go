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

func Open(path string) (*db, error) {
	pager, err := NewDiskPageManager(path)
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

	// Replay WAL from last checkpoint to current
	err := dm.ReplayWAL(checkpointTxn, func(pageID PageID, page *Page) error {
		return dm.WritePage(pageID, page)
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
	newMeta := *meta
	newMeta.CheckpointTxnID = currentTxn
	if err := dm.PutMeta(&newMeta); err != nil {
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
	minReaderTxn := d.minReaderTxn()
	_ = d.store.cache.EvictCheckpointed(minReaderTxn)

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

// releasePages calls Release on the freelist with the given minimum transaction ID
func (d *db) releasePages(minTxn uint64) {
	// Need to access the DiskPageManager's freelist
	if dm, ok := d.store.pager.(*DiskPageManager); ok {
		dm.mu.Lock()
		dm.freelist.Release(minTxn)
		dm.mu.Unlock()
	}
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
		tx.pages = make(map[PageID]*Node)
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

	// Final checkpoint to flush WAL
	if err := d.checkpoint(); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Mark database as closed
	d.closed = true

	return d.store.Close()
}

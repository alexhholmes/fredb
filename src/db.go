package src

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrPageOverflow       = errors.New("page overflow: serialized data exceeds page size")
	ErrInvalidOffset      = errors.New("invalid offset: out of bounds")
	ErrInvalidMagicNumber = errors.New("invalid magic number")
	ErrInvalidVersion     = errors.New("invalid format version")
	ErrInvalidPageSize    = errors.New("invalid page size")
	ErrInvalidChecksum    = errors.New("invalid checksum")
)

type db struct {
	mu    sync.RWMutex
	store *BTree

	// Transaction state
	writerTx  *Tx    // Current write transaction (nil if none)
	readerTxs []*Tx  // Active read transactions
	nextTxnID uint64 // Monotonic transaction ID counter
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

	return &db{
		store:     btree,
		nextTxnID: meta.TxnID, // Resume from last committed TxnID
	}, nil
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
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.store.Close()
}

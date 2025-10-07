package fredb

import (
	"errors"
	"fmt"

	"fredb/internal/base"
)

var (
	ErrTxNotWritable = errors.New("transaction is read-only")
	ErrTxDone        = errors.New("transaction has been committed or rolled back")
	ErrTxInProgress  = errors.New("write transaction already in progress")
)

// Tx represents a transaction on the database.
//
// CONCURRENCY: Transactions are NOT thread-safe and must only be used by a single
// goroutine at a time. Calling Set/get/Delete/Commit/Rollback concurrently from
// multiple goroutines will cause panics and data corruption.
//
// Transactions provide a consistent view of the database at the point they were created.
// Read transactions can run concurrently, but only one write transaction can be active at a time.
type Tx struct {
	txnID    uint64                     // Unique transaction ID
	writable bool                       // Is this a read-write transaction?
	db       *DB                        // Database this transaction belongs to (concrete type for internal access)
	root     *base.Node                 // Root Node at transaction start
	pages    map[base.PageID]*base.Node // TX-LOCAL: uncommitted COW pages (write transactions only)
	pending  []base.PageID              // Pages allocated in this transaction (for COW)
	freed    []base.PageID              // Pages freed in this transaction (for freelist)
	done     bool                       // Has Commit() or Rollback() been called?
}

// Get retrieves the value for a key.
// Returns ErrKeyNotFound if the key does not exist.
func (tx *Tx) Get(key []byte) ([]byte, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}

	// Use transaction's snapshot root for MVCC isolation
	// tx.root is captured at Begin() time and provides snapshot isolation
	if tx.root == nil {
		return nil, ErrKeyNotFound
	}

	// Search from transaction's root (pass tx for versioned cache access)
	return tx.db.store.searchNode(tx, tx.root, key)
}

// Set stores a key-value pair.
// Returns ErrTxNotWritable if called on a read-only transaction.
func (tx *Tx) Set(key, value []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// validate key/value size before insertion
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Also active practical limit based on Page size
	// At minimum, a leaf Page must hold at least one key-value pair
	maxSize := base.PageSize - base.PageHeaderSize - base.LeafElementSize
	if len(key)+len(value) > maxSize {
		return ErrPageOverflow
	}

	// Use COW-aware insertion with splits
	// Keep changes in tx.root (transaction-local), not DB.store.root

	// Use tx.root if set, otherwise start from DB.store.root
	root := tx.root
	if root == nil {
		root = tx.db.store.root
	}

	// Handle root split with COW
	if root.IsFull() {
		// Split root using COW
		leftChild, rightChild, midKey, midVal, err := tx.db.store.splitChild(tx, root)
		if err != nil {
			return err
		}

		// Create new root using tx.allocatePage()
		newRootID, _, err := tx.allocatePage()
		if err != nil {
			return err
		}

		newRoot := &base.Node{
			PageID:   newRootID,
			Dirty:    true,
			IsLeaf:   false,
			NumKeys:  1,
			Keys:     [][]byte{midKey},
			Values:   [][]byte{midVal},
			Children: []base.PageID{leftChild.PageID, rightChild.PageID},
		}

		// store the new root in TX-local cache
		// The split Children were already stored in splitChild()
		tx.pages[newRootID] = newRoot

		root = newRoot
	}

	// Insert with recursive COW - retry until success or non-overflow error
	// This handles cascading splits when parent nodes also overflow
	maxRetries := 20 // Prevent infinite loops
	var newRoot *base.Node
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		newRoot, err = tx.db.store.insertNonFull(tx, root, key, value)
		if !errors.Is(err, ErrPageOverflow) {
			break // Success or non-overflow error
		}

		// Root couldn't fit the new entry - split it
		leftChild, rightChild, midKey, midVal, err := tx.db.store.splitChild(tx, root)
		if err != nil {
			return err
		}

		// Create new root
		newRootID, _, err := tx.allocatePage()
		if err != nil {
			return err
		}

		newRoot = &base.Node{
			PageID:   newRootID,
			Dirty:    true,
			IsLeaf:   false,
			NumKeys:  1,
			Keys:     [][]byte{midKey},
			Values:   [][]byte{midVal},
			Children: []base.PageID{leftChild.PageID, rightChild.PageID},
		}

		tx.pages[newRootID] = newRoot
		root = newRoot // Use new root for next retry
	}

	if err != nil {
		return err
	}

	// Update transaction-local root (NOT DB.store.root)
	// Changes become visible only on Commit()
	tx.root = newRoot

	return nil
}

// Delete removes a key.
// Returns ErrTxNotWritable if called on a read-only transaction.
func (tx *Tx) Delete(key []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// Full COW-aware deletion with merge/borrow support
	// Keep changes in tx.root (transaction-local), not DB.store.root

	// Use tx.root if set, otherwise start from DB.store.root
	root := tx.root
	if root == nil {
		root = tx.db.store.root
	}

	if root == nil || root.NumKeys == 0 {
		return ErrKeyNotFound
	}

	// Perform COW-aware recursive deletion
	newRoot, err := tx.db.store.deleteFromNode(tx, root, key)
	if err != nil {
		return err
	}

	// Handle root collapse: if root is non-leaf and has no Keys, promote its only child
	if !newRoot.IsLeaf && newRoot.NumKeys == 0 {
		if len(newRoot.Children) > 0 {
			// Track old root as freed
			tx.addFreed(newRoot.PageID)

			// Load the only child as new root
			newRoot, err = tx.db.store.loadNode(tx, newRoot.Children[0])
			if err != nil {
				return err
			}
		}
	}

	// Update transaction-local root (NOT DB.store.root)
	// Changes become visible only on Commit()
	tx.root = newRoot

	return nil
}

// Cursor creates a new cursor for iterating over Keys.
// The cursor is bound to this transaction's snapshot.
func (tx *Tx) Cursor() *Cursor {
	// Pass transaction to cursor for snapshot isolation
	return tx.db.store.newCursor(tx)
}

// Commit writes all changes and makes them visible to future transactions.
// Returns ErrTxNotWritable if called on a read-only transaction.
// Returns ErrTxDone if transaction has already been committed or rolled back.
func (tx *Tx) Commit() error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// Apply transaction changes to database
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Capture old root for rollback if commit fails
	oldRoot := tx.db.store.root

	// Apply transaction-local root to DB.store.root
	// This makes all COW changes visible to future transactions
	if tx.root != nil {
		tx.db.store.root = tx.root
	}

	// Write all TX-local pages to wal (not disk!)
	for pageID, node := range tx.pages {
		// Serialize Node to a Page with this transaction's ID
		page, err := node.Serialize(tx.txnID)
		if err != nil {
			// Restore old root on failure
			tx.db.store.root = oldRoot
			return err
		}

		// Write to wal instead of disk
		if err := tx.db.wal.AppendPage(tx.txnID, pageID, page); err != nil {
			// Restore old root on failure
			tx.db.store.root = oldRoot
			return err
		}

		// Clear Dirty flag after successful write
		node.Dirty = false

		// Flush to global versioned cache with this transaction's ID
		// This makes the new version visible to future transactions
		tx.db.store.cache.Put(pageID, tx.txnID, node)
	}

	// Append commit marker to wal
	if err := tx.db.wal.AppendCommit(tx.txnID); err != nil {
		tx.db.store.root = oldRoot
		return err
	}

	// Conditionally fsync wal based on sync mode (this is the commit point!)
	if err := tx.db.wal.Sync(); err != nil {
		tx.db.store.root = oldRoot
		return err
	}

	// Add freed pages to pending at this transaction ID
	// These pages were part of the previous version and can be reclaimed
	// when all readers that might reference them have finished.
	if len(tx.freed) > 0 {
		dm := tx.db.store.Pager
		dm.FreePending(tx.txnID, tx.freed)
	}

	// Write meta Page to disk for persistence
	// get current meta from storage
	meta := tx.db.store.Pager.GetMeta()

	// Build new meta Page with updated root and incremented TxnID
	if tx.root != nil {
		meta.RootPageID = tx.root.PageID
	}
	meta.TxnID = tx.txnID
	meta.Checksum = meta.CalculateChecksum()

	// Update storage's in-memory meta
	if err := tx.db.store.Pager.PutMeta(meta); err != nil {
		return err
	}

	tx.db.writerTx = nil

	// Mark transaction as done ONLY after all writes succeed
	// This ensures Rollback can clean up if any write fails
	tx.done = true

	return nil
}

// Rollback discards all changes made in the transaction.
// Safe to call after Commit() (becomes a no-op).
// Safe to call multiple times (idempotent).
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil // Already committed or rolled back
	}
	tx.done = true

	// Remove from DB tracking
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.writable {
		// Clear TX-local cache - discard all uncommitted changes
		tx.pages = nil

		// Return allocated but uncommitted pages to the freelist
		// These pages were allocated during the transaction but never used
		if len(tx.pending) > 0 {
			dm := tx.db.store.Pager
			// Add directly to free list, not pending - these can be reused immediately
			// since they were never part of any committed state
			for _, pageID := range tx.pending {
				dm.FreePage(pageID)
			}
		}

		tx.db.writerTx = nil
	} else {
		// Remove from readers slice
		for i, rtx := range tx.db.readerTxs {
			if rtx == tx {
				// Swap with last pathElement and truncate
				tx.db.readerTxs[i] = tx.db.readerTxs[len(tx.db.readerTxs)-1]
				tx.db.readerTxs = tx.db.readerTxs[:len(tx.db.readerTxs)-1]
				break
			}
		}

		// Trigger release - non-blocking send
		// The background goroutine will calculate the new minimum
		select {
		case tx.db.releaseC <- 0: // Value doesn't matter, it's just a trigger
		default:
			// Channel blocked, ticker will handle it
		}
	}

	return nil
}

// check verifies the transaction is still active.
// Returns ErrTxDone if the transaction has been committed or rolled back.
func (tx *Tx) check() error {
	if tx.done {
		return ErrTxDone
	}
	return nil
}

// Loads a Node using hybrid cache: tx.pages → versioned global cache → disk
func (tx *Tx) loadNode(id base.PageID) (*base.Node, error) {
	// 1. Check TX-local cache first (if writable tx with uncommitted changes)
	if tx.writable && tx.pages != nil {
		if node, exists := tx.pages[id]; exists {
			return node, nil
		}
	}

	// 2. GetOrLoad atomically checks cache or coordinates disk load
	node, found := tx.db.store.cache.GetOrLoad(id, tx.txnID, func() (*base.Node, uint64, error) {
		// Load from disk (called by at most one thread per id)
		page, err := tx.db.store.Pager.ReadPage(id)
		if err != nil {
			return nil, 0, err
		}

		// Create Node and Deserialize
		node := &base.Node{
			PageID: id,
			Dirty:  false,
		}

		// Try to Deserialize - if Page is empty (new Page), header.NumKeys will be 0
		header := page.Header()
		if header.NumKeys > 0 {
			if err := node.Deserialize(page); err != nil {
				return nil, 0, err
			}
		} else {
			// New/empty Page - initialize as empty leaf
			node.PageID = id
			node.IsLeaf = true
			node.NumKeys = 0
			node.Keys = make([][]byte, 0)
			node.Values = make([][]byte, 0)
			node.Children = make([]base.PageID, 0)
		}

		// Cycle detection: check if deserialized Node references itself
		if !node.IsLeaf {
			for _, childID := range node.Children {
				if childID == id {
					return nil, 0, ErrCorruption // Self-reference detected
				}
			}
		}

		// Return Node and its TxnID from disk header
		return node, header.TxnID, nil
	})

	if !found {
		// Version not visible or load failed
		return nil, ErrKeyNotFound
	}

	// Cycle detection on cached Node (fast path)
	if !node.IsLeaf {
		for _, childID := range node.Children {
			if childID == id {
				return nil, ErrCorruption // Self-reference detected
			}
		}
	}

	return node, nil
}

// EnsureWritable ensures a Node is safe to modify in this transaction.
// Performs COW only if the Node doesn't already belong to this transaction.
// Returns a writable Node (either the original if already owned, or a Clone).
func (tx *Tx) ensureWritable(node *base.Node) (*base.Node, error) {
	// 1. Check TX-local cache first - if already COW'd in this transaction
	if cloned, exists := tx.pages[node.PageID]; exists {
		return cloned, nil
	}

	// 2. Check if this Node already belongs to this transaction (pending allocations)
	// If its PageID is in tx.pending, it was allocated in this transaction
	for _, pid := range tx.pending {
		if pid == node.PageID {
			// Node already owned by this transaction, no COW needed
			return node, nil
		}
	}

	// 3. Node doesn't belong to this transaction, perform Copy-On-Write
	cloned := node.Clone()

	// Allocate new Page for cloned Node
	pageID, _, err := tx.allocatePage()
	if err != nil {
		return nil, err
	}

	// Set up cloned Node with new Page
	cloned.PageID = pageID
	cloned.Dirty = true

	// Don't Serialize here - let the caller modify the Node first
	// The caller will Serialize after modifications

	// Track old Page as freed
	// These pages will be added to freelist's pending list on commit
	// and reclaimed when all readers that might reference them have finished
	tx.addFreed(node.PageID)

	// store in TX-LOCAL cache (NOT global cache yet)
	// Will be flushed to global cache on Commit()
	tx.pages[pageID] = cloned

	return cloned, nil
}

// Allocates a new Page for this transaction.
// The allocated Page is tracked in tx.pending for COW semantics
func (tx *Tx) allocatePage() (base.PageID, *base.Page, error) {
	// Retry allocation if we get a Page that's in tx.freed
	// This can happen when background releaser moves pages from pending to free
	const maxRetries = 10
retryLoop:
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Allocate from storage (uses freelist or grows file)
		// PageManager.allocatePage() is thread-safe via mutex
		pageID, err := tx.db.store.Pager.AllocatePage()
		if err != nil {
			return 0, nil, err
		}

		// Check for duplicate in tx.pending (should never happen)
		for _, pid := range tx.pending {
			if pid == pageID {
				return 0, nil, fmt.Errorf("FATAL: freelist returned PageID %d already in tx.pending (txnID=%d, pending=%v, freed=%v)",
					pageID, tx.txnID, tx.pending, tx.freed)
			}
		}

		// Check if Page is in tx.freed (race with background releaser)
		// If so, skip and retry WITHOUT returning to freelist
		// Returning it would cause the freelist to give it back immediately, creating a loop
		// Instead, just skip - the freelist will eventually exhaust and grow the file
		for _, pid := range tx.freed {
			if pid == pageID {
				continue retryLoop // Skip this Page, try allocating again
			}
		}

		// Track in pending pages (for COW)
		tx.pending = append(tx.pending, pageID)

		// invalidate any stale cache entries for this reused PageID
		// When a Page is freed and reallocated, old versions must be removed
		tx.db.store.cache.Invalidate(pageID)

		// Read the freshly allocated Page
		page, err := tx.db.store.Pager.ReadPage(pageID)
		if err != nil {
			return 0, nil, err
		}

		return pageID, page, nil
	}

	return 0, nil, fmt.Errorf("FATAL: failed to allocate Page after %d retries (txnID=%d, pending=%v, freed=%v)",
		maxRetries, tx.txnID, tx.pending, tx.freed)
}

// AddFreed adds a Page to the freed list, checking for duplicates first.
// This prevents the same Page from being freed multiple times in a transaction.
func (tx *Tx) addFreed(pageID base.PageID) {
	if pageID == 0 {
		return
	}
	// Check if already freed to prevent duplicates
	for _, pid := range tx.freed {
		if pid == pageID {
			return // Already freed
		}
	}
	tx.freed = append(tx.freed, pageID)
}

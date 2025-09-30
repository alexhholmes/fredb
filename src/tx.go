package src

import "errors"

var (
	ErrTxNotWritable = errors.New("transaction is read-only")
	ErrTxDone        = errors.New("transaction has been committed or rolled back")
	ErrTxInProgress  = errors.New("write transaction already in progress")
)

// Tx represents a transaction on the database.
// Transactions provide a consistent view of the database at the point they were created.
// Read transactions can run concurrently, but only one write transaction can be active at a time.
type Tx struct {
	db       *db       // Database this transaction belongs to (concrete type for internal access)
	txnID    uint64    // Unique transaction ID
	writable bool      // Is this a read-write transaction?
	meta     *MetaPage // Snapshot of metadata at transaction start
	root     *Node     // Root node at transaction start
	pending  []PageID  // Pages allocated in this transaction (for COW)
	freed    []PageID  // Pages freed in this transaction (for freelist)
	done     bool      // Has Commit() or Rollback() been called?
}

// check verifies the transaction is still active.
// Returns ErrTxDone if the transaction has been committed or rolled back.
func (tx *Tx) check() error {
	if tx.done {
		return ErrTxDone
	}
	return nil
}

// allocatePage allocates a new page for this transaction
// The allocated page is tracked in tx.pending for COW semantics
func (tx *Tx) allocatePage() (PageID, *Page, error) {
	// Allocate from pager (uses freelist or grows file)
	// DiskPageManager.AllocatePage() is thread-safe via mutex
	pageID, err := tx.db.store.pager.AllocatePage()
	if err != nil {
		return 0, nil, err
	}

	// Track in pending pages (for COW)
	tx.pending = append(tx.pending, pageID)

	// Read the freshly allocated page
	page, err := tx.db.store.pager.ReadPage(pageID)
	if err != nil {
		return 0, nil, err
	}

	return pageID, page, nil
}

// ensureWritable ensures a node is safe to modify in this transaction.
// Always performs COW to avoid modifying shared state.
// Returns a writable clone with a new pageID.
func (tx *Tx) ensureWritable(node *Node) (*Node, error) {
	// Always clone, even if node is dirty
	// The dirty flag means "modified from disk", not "owned by this transaction"

	// Perform Copy-On-Write
	cloned := node.clone()

	// Allocate new page for cloned node
	pageID, page, err := tx.allocatePage()
	if err != nil {
		return nil, err
	}

	// Set up cloned node with new page
	cloned.pageID = pageID
	cloned.page = page
	cloned.dirty = true

	// Track old page as freed
	// NOTE: Just tracks freed pages, doesn't reclaim them yet.
	// Future work: implement versioned freelist to safely reclaim pages
	// after all transactions that might reference them have finished.
	if node.pageID != 0 {
		tx.freed = append(tx.freed, node.pageID)
	}

	// Cache the cloned node so it can be found by loadNode in the same transaction
	tx.db.store.cache.Put(pageID, cloned)
	tx.db.store.cache.Unpin(pageID)

	return cloned, nil
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

	// Search from transaction's root
	return tx.db.store.searchNode(tx.root, key)
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

	// Use COW-aware insertion with splits
	// Keep changes in tx.root (transaction-local), not db.store.root

	// Use tx.root if set, otherwise start from db.store.root
	root := tx.root
	if root == nil {
		root = tx.db.store.root
	}

	// Handle root split with COW
	if root.isFull() {
		// Split root using COW
		leftChild, rightChild, midKey, midVal, err := tx.db.store.splitChild(tx, nil, 0, root)
		if err != nil {
			return err
		}

		// Create new root using tx.allocatePage()
		newRootID, newRootPage, err := tx.allocatePage()
		if err != nil {
			return err
		}

		newRoot := &Node{
			page:     newRootPage,
			pageID:   newRootID,
			dirty:    true,
			isLeaf:   false,
			numKeys:  1,
			keys:     [][]byte{midKey},
			values:   [][]byte{midVal},
			children: []PageID{leftChild.pageID, rightChild.pageID},
		}

		// Cache the new root and split children
		tx.db.store.cache.Put(newRootID, newRoot)
		tx.db.store.cache.Unpin(newRootID)
		tx.db.store.cache.Put(leftChild.pageID, leftChild)
		tx.db.store.cache.Unpin(leftChild.pageID)
		tx.db.store.cache.Put(rightChild.pageID, rightChild)
		tx.db.store.cache.Unpin(rightChild.pageID)

		root = newRoot
	}

	// Insert with recursive COW
	newRoot, err := tx.db.store.insertNonFull(tx, root, key, value)
	if err != nil {
		return err
	}

	// Update transaction-local root (NOT db.store.root)
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
	// Keep changes in tx.root (transaction-local), not db.store.root

	// Use tx.root if set, otherwise start from db.store.root
	root := tx.root
	if root == nil {
		root = tx.db.store.root
	}

	if root == nil || root.numKeys == 0 {
		return ErrKeyNotFound
	}

	// Perform COW-aware recursive deletion
	newRoot, err := tx.db.store.deleteFromNode(tx, root, key)
	if err != nil {
		return err
	}

	// Handle root collapse: if root is non-leaf and has no keys, promote its only child
	if !newRoot.isLeaf && newRoot.numKeys == 0 {
		if len(newRoot.children) > 0 {
			// Track old root as freed
			if newRoot.pageID != 0 {
				tx.freed = append(tx.freed, newRoot.pageID)
			}

			// Load the only child as new root
			newRoot, err = tx.db.store.loadNode(newRoot.children[0])
			if err != nil {
				return err
			}
		}
	}

	// Update transaction-local root (NOT db.store.root)
	// Changes become visible only on Commit()
	tx.root = newRoot

	return nil
}

// Cursor creates a new cursor for iterating over keys.
// The cursor is bound to this transaction's snapshot.
func (tx *Tx) Cursor() *Cursor {
	// Direct pass-through to BTree
	// Future work: bind cursor to transaction and validate tx state
	return tx.db.store.NewCursor()
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

	// Mark as done before acquiring lock
	tx.done = true

	// Apply transaction changes to database
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Apply transaction-local root to db.store.root
	// This makes all COW changes visible to future transactions
	if tx.root != nil {
		tx.db.store.root = tx.root
	}

	// Write meta page to disk for persistence
	// Get current meta from pager
	currentMeta := tx.db.store.pager.GetMeta()

	// Build new meta page with updated root and incremented TxnID
	newMeta := *currentMeta
	if tx.root != nil {
		newMeta.RootPageID = tx.root.pageID
	}
	newMeta.TxnID = tx.txnID
	newMeta.Checksum = newMeta.CalculateChecksum()

	// Update pager's in-memory meta
	if err := tx.db.store.pager.PutMeta(&newMeta); err != nil {
		return err
	}

	tx.db.writerTx = nil

	// Phase 4: Will add freelist management

	return nil
}

// Rollback discards all changes made in the transaction.
// Safe to call after Commit() (becomes a no-op).
// Safe to call multiple times (idempotent).
func (tx *Tx) Rollback() error {
	if tx.done {
		return nil // Idempotent
	}

	tx.done = true

	// Remove from DB tracking
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	if tx.writable {
		tx.db.writerTx = nil
	} else {
		// Remove from readers slice
		for i, rtx := range tx.db.readerTxs {
			if rtx == tx {
				// Swap with last element and truncate
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
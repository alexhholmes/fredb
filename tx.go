package fredb

import (
	"bytes"
	"errors"

	"fredb/internal/algo"
	"fredb/internal/base"
	"fredb/internal/coordinator"
)

// Tx represents a transaction on the database.
//
// CONCURRENCY: Transactions are NOT thread-safe and must only be used by a single
// goroutine at a time. Calling Set/Get/Delete/Commit/Rollback concurrently from
// multiple goroutines will cause panics and data corruption.
//
// Transactions provide a consistent view of the database at the point they were created.
// Read transactions can run concurrently, but only one write transaction can be active at a time.
type Tx struct {
	txID     uint64 // Unique transaction ID
	writable bool   // Is this a read-write transaction?

	db       *DB                        // Database this transaction belongs to (concrete type for internal access)
	root     *base.Node                 // Root Node at transaction start (stores bucket metadata)
	buckets  map[string]*Bucket         // Cache of loaded buckets
	acquired map[base.PageID]struct{}   // Buckets acquired from coordinator (need release)
	pages    map[base.PageID]*base.Node // TX-LOCAL: uncommitted COW pages (write transactions only)
	freed    map[base.PageID]struct{}   // Pages freed in this transaction (for freelist)
	deletes  map[string]base.PageID     // Root pages of buckets deleted in this transaction, for background cleanup upon commit

	nextVirtualID int64 // Starts at -1, decrements: -1, -2, -3, ...
	done          bool  // Has Commit() or Rollback() been called?
}

// Get retrieves the value for a key from the default bucket.
// Returns ErrKeyNotFound if the key does not exist.
func (tx *Tx) Get(key []byte) ([]byte, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}

	// Delegate to __root__ bucket (default namespace)
	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		return nil, ErrBucketNotFound
	}

	val := bucket.Get(key)
	if val == nil {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

// search recursively searches for a key using algo functions
func (tx *Tx) search(node *base.Node, key []byte) ([]byte, error) {
	// Find position in current node
	i := 0
	for i < int(node.NumKeys) && bytes.Compare(key, node.Keys[i]) >= 0 {
		i++
	}
	// After loop: i points to first key > search_key (or NumKeys if all keys <= search_key)

	// If leaf node, check if key found
	if node.Leaf {
		// In leaf, we need to check the previous position (since loop went past equal keys)
		if i > 0 && bytes.Equal(key, node.Keys[i-1]) {
			return node.Values[i-1], nil
		}
		// Not found in leaf
		return nil, ErrKeyNotFound
	}

	// Branch node: continue descending
	// `i` is the correct child index (first child with keys >= search_key)
	child, err := tx.loadNode(node.Children[i])
	if err != nil {
		return nil, err
	}

	return tx.search(child, key)
}

// Set stores a key-value pair in the default bucket.
// Returns ErrTxNotWritable if called on a read-only transaction.
func (tx *Tx) Set(key, value []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// Delegate to __root__ bucket (default namespace)
	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		return ErrBucketNotFound
	}

	return bucket.Put(key, value)
}

// Delete removes a key from the default bucket.
// Returns ErrTxNotWritable if called on a read-only transaction.
func (tx *Tx) Delete(key []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// Delegate to __root__ bucket (default namespace)
	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		return ErrBucketNotFound
	}

	return bucket.Delete(key)
}

// Cursor creates a cursor for the default bucket.
// The cursor is bound to this transaction's snapshot.
func (tx *Tx) Cursor() *Cursor {
	// Delegate to __root__ bucket (default namespace)
	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		// Return empty cursor if __root__ doesn't exist
		return &Cursor{
			tx:    tx,
			valid: false,
		}
	}
	return bucket.Cursor()
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

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// Convert syncMode to storage package type
	var syncMode coordinator.SyncMode
	if tx.db.options.syncMode == SyncEveryCommit {
		syncMode = coordinator.SyncEveryCommit
	} else {
		syncMode = coordinator.SyncOff
	}

	// Phase 1: Map all virtual page IDs to real page IDs (no disk writes yet)
	_, err := tx.db.coord.MapVirtualPageIDs(tx.pages, tx.root)
	if err != nil {
		return err
	}

	// CRITICAL: After MapVirtualPageIDs, nodes in tx.pages have been updated with real PageIDs,
	// but the MAP is still keyed by the old virtual IDs. Rebuild the map with real PageIDs as keys.
	remappedPages := make(map[base.PageID]*base.Node)
	for _, node := range tx.pages {
		remappedPages[node.PageID] = node
	}
	tx.pages = remappedPages

	// Phase 2: Now insert bucket metadata with REAL page IDs into root tree
	for name, bucket := range tx.buckets {
		if bucket.writable {
			key := []byte(name)
			value := bucket.Serialize()

			// Validate key/value size before insertion
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
			// Keep changes in tx.root (transaction-local), not DB.root
			root := tx.root

			// Handle root split with COW
			if root.IsFull(key, value) {
				// Split root using COW
				leftChild, rightChild, midKey, _, err := tx.splitChild(root)
				if err != nil {
					return err
				}

				// Create new root using tx.allocatePage()
				newRootID, _, err := tx.allocatePage()
				if err != nil {
					return err
				}

				newRoot := algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)

				// CRITICAL: Add new root to tx.pages if it has a virtual ID
				// Otherwise it won't get a real page ID allocated during WriteTransaction
				if int64(newRootID) < 0 {
					tx.pages[newRootID] = newRoot
				}

				root = newRoot
			}

			// Insert with recursive COW - retry until success or non-overflow error
			// This handles cascading splits when parent nodes also overflow
			maxRetries := 20 // Prevent infinite loops

			for attempt := 0; attempt < maxRetries; attempt++ {
				newRoot, err := tx.insertNonFull(root, key, value)
				if !errors.Is(err, ErrPageOverflow) {
					// Either success or non-recoverable error
					if err == nil {
						root = newRoot // Only update root on success
					}
					break
				}

				// Root couldn't fit the new entry - split it
				leftChild, rightChild, midKey, _, err := tx.splitChild(root)
				if err != nil {
					return err
				}

				// Create new root
				newRootID, _, err := tx.allocatePage()
				if err != nil {
					return err
				}

				newRoot2 := algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)

				// CRITICAL: Add new root to tx.pages if it has a virtual ID
				// Otherwise it won't get a real page ID allocated during WriteTransaction
				if int64(newRootID) < 0 {
					tx.pages[newRootID] = newRoot2
				}

				root = newRoot2
			}

			// Update transaction-local root (NOT DB.root)
			// Changes become visible only on Commit()
			tx.root = root
		}
	}

	// Phase 2b: Map any NEW virtual pages that were created during bucket metadata insertion
	// These weren't present during Phase 1, so they need to be mapped now
	_, err = tx.db.coord.MapVirtualPageIDs(tx.pages, tx.root)
	if err != nil {
		return err
	}

	// Rebuild the map again with the newly mapped real PageIDs
	remappedPages = make(map[base.PageID]*base.Node)
	for _, node := range tx.pages {
		remappedPages[node.PageID] = node
	}
	tx.pages = remappedPages

	// Phase 3: Write everything to disk in a single operation
	// This handles: page writes, freed pages, meta update, sync (if needed), and CommitSnapshot
	err = tx.db.coord.WriteTransaction(
		tx.pages,
		tx.root,
		tx.freed,
		tx.txID,
		syncMode,
	)
	if err != nil {
		return err
	}

	tx.done = true

	// Phase 4: Add deleted buckets to coordinator's Deleted map BEFORE releasing buckets
	// This prevents new readers from acquiring deleted buckets after this commit
	if tx.writable {
		tx.db.coord.DeletedMu.Lock()
		for _, pageID := range tx.deletes {
			tx.db.coord.Deleted[pageID] = struct{}{}
		}
		tx.db.coord.DeletedMu.Unlock()
	}

	tx.db.writer.Store(nil)

	// Phase 5: Release acquired buckets after marking deleted ones
	// This must happen AFTER releasing DeletedMu to avoid deadlock in ReleaseBucket()
	// Only release buckets that were actually acquired (not newly created ones)
	tx.db.tryReleasePages()
	for pageID := range tx.acquired {
		tx.db.coord.ReleaseBucket(pageID, tx.db.freeTree)
	}

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

	// Clean up deleted buckets from Coordinator's Deleted map
	// If we marked buckets for deletion but are now rolling back, they should not be deleted
	if tx.writable && len(tx.deletes) > 0 {
		tx.db.coord.DeletedMu.Lock()
		for _, pageID := range tx.deletes {
			delete(tx.db.coord.Deleted, pageID)
		}
		tx.db.coord.DeletedMu.Unlock()
	}

	// Release all acquired buckets (both read and write transactions)
	// Only release buckets that were actually acquired (not newly created ones)
	for pageID := range tx.acquired {
		tx.db.coord.ReleaseBucket(pageID, tx.db.freeTree)
	}

	// Remove from DB tracking
	if tx.writable {
		// Writers need lock to clean up
		tx.db.mu.Lock()
		defer tx.db.mu.Unlock()

		// Discard all transaction-local state
		// Virtual pages were never allocated from Coordinator, so nothing to free
		tx.pages = nil

		tx.db.writer.Store(nil)

		tx.db.tryReleasePages()
	} else {
		// Readers: completely lock-free, zero cache line contention
		tx.db.readers.Delete(tx)
		// Page release is lazy - next writer will handle it
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

// EnsureWritable ensures a Node is safe to modify in this transaction.
// Performs COW only if the Node doesn't already belong to this transaction.
// Returns a writable Node (either the original if already owned, or a Clone).
func (tx *Tx) ensureWritable(node *base.Node) (*base.Node, error) {
	// 1. Check TX-local cache first - if already COW'd in this transaction
	if cloned, exists := tx.pages[node.PageID]; exists {
		return cloned, nil
	}

	// 2. Check if this Node was allocated in this transaction (virtual page ID)
	// Virtual page IDs are negative numbers
	if int64(node.PageID) < 0 {
		// Node already owned by this transaction, no COW needed
		// But we still need to add it to tx.pages so it gets committed
		tx.pages[node.PageID] = node
		return node, nil
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
// Returns a virtual page ID (negative) that will be mapped to a real page at commit time.
func (tx *Tx) allocatePage() (base.PageID, *base.Page, error) {
	// Generate virtual page ID (negative number)
	// These are transaction-local and never touch Coordinator until commit
	virtualID := base.PageID(tx.nextVirtualID)
	tx.nextVirtualID--

	// Return empty page (will be written at commit time with real page ID)
	return virtualID, &base.Page{}, nil
}

// AddFreed adds a Page to the freed list, checking for duplicates first.
// This prevents the same Page from being freed multiple times in a transaction.
func (tx *Tx) addFreed(pageID base.PageID) {
	if pageID == 0 {
		return
	}
	// Skip virtual page IDs - they were never real pages, so don't free them
	if int64(pageID) < 0 {
		return
	}
	// Add to map (automatically handles duplicates)
	tx.freed[pageID] = struct{}{}
}

// splitChild performs COW on the child being split and allocates the new sibling
func (tx *Tx) splitChild(child *base.Node) (*base.Node, *base.Node, []byte, []byte, error) {
	// I/O: COW BEFORE any computation
	child, err := tx.ensureWritable(child)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Pure: calculate split point
	sp := algo.CalculateSplitPoint(child)

	// Pure: extract right portion (read-only)
	rightKeys, rightVals, rightChildren := algo.ExtractRightPortion(child, sp)

	// I/O: allocate page for right node
	nodeID, _, err := tx.allocatePage()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// State: construct right node
	node := &base.Node{
		PageID:   nodeID,
		Dirty:    true,
		Leaf:     child.Leaf,
		NumKeys:  uint16(sp.RightCount),
		Keys:     rightKeys,
		Values:   rightVals,
		Children: rightChildren,
	}

	// State: truncate left (child already COW'd, safe to mutate)
	algo.TruncateLeft(child, sp)

	// I/O: store right node in tx cache
	tx.pages[nodeID] = node

	return child, node, sp.SeparatorKey, []byte{}, nil
}

// loadNode loads a node using version-aware coordinator: tx.pages → Coordinator → Storage
func (tx *Tx) loadNode(pageID base.PageID) (*base.Node, error) {
	// Check TX-local cache first (if writable tx with uncommitted changes)
	if tx.writable && tx.pages != nil {
		if node, exists := tx.pages[pageID]; exists {
			return node, nil
		}
	}

	// Use coordinator's version-aware loading
	node, err := tx.db.coord.GetNode(pageID)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// insertNonFull inserts into a non-full node with COW
// Returns the (possibly new) root node after COW
func (tx *Tx) insertNonFull(node *base.Node, key, value []byte) (*base.Node, error) {
	if node.Leaf {
		// COW before modifying leaf
		n, err := tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Pure: find insert position
		pos := algo.FindInsertPosition(n, key)

		// Check for update
		if pos < int(n.NumKeys) && bytes.Equal(n.Keys[pos], key) {
			// Save old value for rollback
			oldValue := make([]byte, len(n.Values[pos]))
			copy(oldValue, n.Values[pos])

			algo.ApplyLeafUpdate(n, pos, value)

			// Check size after update
			if err := n.CheckOverflow(); err != nil {
				// Rollback: restore old value
				n.Values[pos] = oldValue
				return nil, err
			}
			return n, nil
		}

		// Insert new key-value using algo
		algo.ApplyLeafInsert(n, pos, key, value)

		// Check size after insertion
		if err := n.CheckOverflow(); err != nil {
			// Rollback: remove the inserted key/value
			n.Keys = algo.RemoveAt(n.Keys, pos)
			n.Values = algo.RemoveAt(n.Values, pos)
			n.NumKeys--
			return nil, err
		}

		return n, nil
	}

	// Branch node - recursive COW
	// Find child to insert into
	i := algo.FindChildIndex(node, key)

	// Load child
	child, err := tx.loadNode(node.Children[i])
	if err != nil {
		return nil, err
	}

	// Handle full child with COW-aware split
	if child.IsFull(key, value) {
		// Split child using COW
		leftChild, rightChild, midKey, midVal, err := tx.splitChild(child)
		if err != nil {
			return nil, err
		}

		// COW parent to insert middle key and update pointers
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Save old state for rollback
		oldKeys := node.Keys
		oldValues := node.Values
		oldChildren := node.Children
		oldNumKeys := node.NumKeys

		// Apply split to parent using algo
		algo.ApplyChildSplit(node, i, leftChild, rightChild, midKey, midVal)

		// Check size after modification
		if err := node.CheckOverflow(); err != nil {
			// Rollback: restore old state
			node.Keys = oldKeys
			node.Values = oldValues
			node.Children = oldChildren
			node.NumKeys = oldNumKeys
			return nil, err
		}

		// Determine which child to use after split
		if bytes.Compare(key, midKey) >= 0 {
			i++
			child = rightChild
		} else {
			child = leftChild
		}
	}

	// Store original child PageID to detect COW
	oldChildID := child.PageID

	// Recursive insert (may COW child)
	newChild, err := tx.insertNonFull(child, key, value)
	if errors.Is(err, ErrPageOverflow) {
		// Child couldn't fit the key/value - split it (use original child, not nil from error)
		leftChild, rightChild, midKey, midVal, err := tx.splitChild(child)
		if err != nil {
			return nil, err
		}

		// COW parent to insert middle key and update pointers
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Save old state for rollback
		oldKeys := node.Keys
		oldValues := node.Values
		oldChildren := node.Children
		oldNumKeys := node.NumKeys

		// Apply split to parent using algo
		algo.ApplyChildSplit(node, i, leftChild, rightChild, midKey, midVal)

		// Check size after modification
		if err := node.CheckOverflow(); err != nil {
			// Rollback: restore old state
			node.Keys = oldKeys
			node.Values = oldValues
			node.Children = oldChildren
			node.NumKeys = oldNumKeys
			return nil, err
		}

		// Retry insert into correct child
		if bytes.Compare(key, midKey) >= 0 {
			i++
			rightChild, err = tx.insertNonFull(rightChild, key, value)
		} else {
			leftChild, err = tx.insertNonFull(leftChild, key, value)
		}
		if err != nil {
			return nil, err
		}

		return node, nil
	} else if err != nil {
		return nil, err
	}

	// Success - update child with returned value
	child = newChild

	// If child was COW'd, update parent pointer
	if child.PageID != oldChildID {
		// COW parent to update child pointer
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		node.Children[i] = child.PageID
		node.Dirty = true
		if err := node.CheckOverflow(); err != nil {
			return nil, err
		}
	}

	return node, nil
}

// deleteFromNode recursively deletes a key from the subtree rooted at node with COW
// Returns the (possibly new) node after COW
func (tx *Tx) deleteFromNode(node *base.Node, key []byte) (*base.Node, error) {
	// B+ tree: if this is a leaf, check if key exists and delete
	if node.Leaf {
		idx := algo.FindKeyInLeaf(node, key)
		if idx >= 0 {
			return tx.deleteFromLeaf(node, idx)
		}
		return nil, ErrKeyNotFound
	}

	// Branch node: descend to child (never delete from branch)
	// Find child where key might be using algo
	childIdx := algo.FindDeleteChildIndex(node, key)

	child, err := tx.loadNode(node.Children[childIdx])
	if err != nil {
		return nil, err
	}

	// Delete from child - CRITICAL: capture returned child to get new PageID after COW
	child, err = tx.deleteFromNode(child, key)
	if err != nil {
		return nil, err
	}

	// COW parent to update child pointer
	node, err = tx.ensureWritable(node)
	if err != nil {
		return nil, err
	}

	// Update child pointer (child may have been COW'd)
	node.Children[childIdx] = child.PageID

	// Check for underflow only if there are siblings to borrow from or merge with
	// When parent has only 1 child (e.g., root with single child), underflow is allowed
	if child.IsUnderflow() && len(node.Children) > 1 {
		// CRITICAL: capture both returned parent and child
		node, child, err = tx.fixUnderflow(node, childIdx, child)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// deleteFromLeaf performs COW on the leaf before deleting
func (tx *Tx) deleteFromLeaf(node *base.Node, idx int) (*base.Node, error) {
	// COW before modifying leaf
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, err
	}

	// Remove key and value using algo
	algo.ApplyLeafDelete(node, idx)

	return node, nil
}

// fixUnderflow fixes underflow in child at childIdx with COW semantics
// Returns (updatedParent, updatedChild, error)
func (tx *Tx) fixUnderflow(parent *base.Node, childIdx int, child *base.Node) (*base.Node, *base.Node, error) {
	// Try to borrow from left sibling
	if childIdx > 0 {
		leftSibling, err := tx.loadNode(parent.Children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}

		if algo.CanBorrowFrom(leftSibling) {
			child, leftSibling, parent, err = tx.borrowFromLeft(child, leftSibling, parent, childIdx-1)
			if err != nil {
				return nil, nil, err
			}
			// Update parent's child pointer for borrowed child
			parent.Children[childIdx] = child.PageID
			return parent, child, nil
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(parent.Children)-1 {
		rightSibling, err := tx.loadNode(parent.Children[childIdx+1])
		if err != nil {
			return nil, nil, err
		}

		if algo.CanBorrowFrom(rightSibling) {
			child, rightSibling, parent, err = tx.borrowFromRight(child, rightSibling, parent, childIdx)
			if err != nil {
				return nil, nil, err
			}
			// Update parent's child pointer for borrowed child
			parent.Children[childIdx] = child.PageID
			return parent, child, nil
		}
	}

	// Merge with a sibling
	if childIdx > 0 {
		// Merge with left sibling
		leftSibling, err := tx.loadNode(parent.Children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}
		parent, err = tx.mergeNodes(leftSibling, child, parent, childIdx-1)
		if err != nil {
			return nil, nil, err
		}
		// After merge, child is absorbed into leftSibling, so return leftSibling as the "child"
		return parent, leftSibling, nil
	}

	// Merge with right sibling
	rightSibling, err := tx.loadNode(parent.Children[childIdx+1])
	if err != nil {
		return nil, nil, err
	}
	parent, err = tx.mergeNodes(child, rightSibling, parent, childIdx)
	if err != nil {
		return nil, nil, err
	}
	// After merge, rightSibling is absorbed into child, child remains
	return parent, child, nil
}

// borrowFromLeft borrows a key from left sibling through parent (COW)
func (tx *Tx) borrowFromLeft(node, leftSibling, parent *base.Node, parentKeyIdx int) (*base.Node, *base.Node, *base.Node, error) {
	// COW all three nodes being modified
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, nil, nil, err
	}

	leftSibling, err = tx.ensureWritable(leftSibling)
	if err != nil {
		return nil, nil, nil, err
	}

	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, nil, nil, err
	}

	algo.BorrowFromLeft(node, leftSibling, parent, parentKeyIdx)

	// Update parent's children pointers to COW'd nodes
	parent.Children[parentKeyIdx] = leftSibling.PageID
	parent.Children[parentKeyIdx+1] = node.PageID

	return node, leftSibling, parent, nil
}

// borrowFromRight borrows a key from right sibling through parent (COW)
func (tx *Tx) borrowFromRight(node, rightSibling, parent *base.Node, parentKeyIdx int) (*base.Node, *base.Node, *base.Node, error) {
	// COW all three nodes being modified
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, nil, nil, err
	}

	rightSibling, err = tx.ensureWritable(rightSibling)
	if err != nil {
		return nil, nil, nil, err
	}

	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, nil, nil, err
	}

	algo.BorrowFromRight(node, rightSibling, parent, parentKeyIdx)

	// Update parent's children pointers to COW'd nodes
	parent.Children[parentKeyIdx] = node.PageID
	parent.Children[parentKeyIdx+1] = rightSibling.PageID

	return node, rightSibling, parent, nil
}

// mergeNodes merges two nodes with COW semantics
func (tx *Tx) mergeNodes(leftNode, rightNode, parent *base.Node, parentKeyIdx int) (*base.Node, error) {
	// COW left node (will receive merged content)
	leftNode, err := tx.ensureWritable(leftNode)
	if err != nil {
		return nil, err
	}

	// COW parent (will have separator removed)
	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, err
	}

	// Try merging - check if result would overflow
	// For large keys/values, two underflow nodes might be nearly full in bytes
	// Calculate merged size before actually merging
	mergedSize := leftNode.Size() + rightNode.Size()
	if !leftNode.Leaf {
		// Branch nodes: add separator key size
		mergedSize += len(parent.Keys[parentKeyIdx])
	}

	if mergedSize > base.PageSize {
		// Merge would overflow - skip merge and allow underflow
		// This is acceptable for large key/value scenarios
		// The nodes will remain separate even though they're below MinKeysPerNode
		return parent, nil
	}

	algo.MergeNodes(leftNode, rightNode, parent.Keys[parentKeyIdx])
	algo.ApplyBranchRemoveSeparator(parent, parentKeyIdx)
	// Update parent's child pointer to merged node
	parent.Children[parentKeyIdx] = leftNode.PageID

	// Track right node as freed
	tx.addFreed(rightNode.PageID)

	return parent, nil
}

// Bucket returns an existing bucket or nil
func (tx *Tx) Bucket(name []byte) *Bucket {
	if err := tx.check(); err != nil {
		return nil
	}

	// Check if transaction deleted bucket
	if _, deleted := tx.deletes[string(name)]; deleted {
		return nil
	}

	// Check tx cache first
	if b, exists := tx.buckets[string(name)]; exists {
		return b
	}

	// Load bucket metadata from root tree (including __root__ bucket)
	meta, err := tx.search(tx.root, name)
	if err != nil {
		return nil
	}

	// Deserialize bucket metadata
	if len(meta) < 16 {
		return nil
	}

	bucket := &Bucket{}
	bucket.Deserialize(meta)
	bucket.tx = tx
	bucket.writable = tx.writable
	bucket.name = name
	bucket.root, err = tx.loadNode(bucket.rootID)
	if err != nil {
		return nil
	}

	// Important, get a lock on the bucket through the coordinator in case
	// the bucket is being deleted concurrently by another transaction.
	// This must be released on rollback or commit.
	// Exception: __root__ bucket is never deleted, so no need to track it
	if string(name) != "__root__" {
		acquired := tx.db.coord.AcquireBucket(bucket.rootID)
		if !acquired {
			// Debug: bucket is marked for deletion
			return nil
		}
		// Track that we acquired this bucket (for later release)
		tx.acquired[bucket.rootID] = struct{}{}
	}

	tx.buckets[string(name)] = bucket
	return bucket
}

// CreateBucket creates a new bucket
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	if err := tx.check(); err != nil {
		return nil, err
	}
	if !tx.writable {
		return nil, ErrTxNotWritable
	}

	// Validate bucket name
	if len(name) == 0 {
		return nil, errors.New("bucket name cannot be empty")
	}
	if string(name) == "__root__" {
		return nil, errors.New("cannot create reserved bucket __root__")
	}

	// Check if bucket was deleted in this transaction
	if _, deleted := tx.deletes[string(name)]; deleted {
		return nil, errors.New("cannot recreate bucket deleted in same transaction")
	}

	// Check if bucket already exists
	if tx.Bucket(name) != nil {
		return nil, errors.New("bucket already exists")
	}

	// Allocate page for bucket's root (branch node)
	bucketRootID, _, err := tx.allocatePage()
	if err != nil {
		return nil, err
	}

	// Allocate page for bucket's initial leaf
	bucketLeafID, _, err := tx.allocatePage()
	if err != nil {
		return nil, err
	}

	// Create bucket's leaf node (empty)
	bucketLeaf := base.NewLeaf(bucketLeafID, make([][]byte, 0), make([][]byte, 0))

	// Create bucket's root node (branch with single child)
	bucketRoot := base.NewBranch(bucketRootID, make([][]byte, 0), []base.PageID{bucketLeafID})

	// Add nodes to transaction cache
	tx.pages[bucketLeafID] = bucketLeaf
	tx.pages[bucketRootID] = bucketRoot

	// Create and cache bucket
	// NOTE: We don't persist metadata to root tree yet - that happens at Commit()
	// This avoids the chicken-and-egg problem with virtual vs real page IDs
	// We don't need to track this bucket's reference in tx.buckets yet either
	// because it's a new bucket that can't be accessed until after Commit(),
	// and because there is only a single writer transaction at a time.
	bucket := &Bucket{
		tx:       tx,
		root:     bucketRoot,
		name:     name,
		sequence: 0,
		writable: true,
	}

	tx.buckets[string(name)] = bucket
	return bucket, nil
}

// CreateBucketIfNotExists convenience method
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	if b := tx.Bucket(name); b != nil {
		return b, nil
	}
	return tx.CreateBucket(name)
}

// DeleteBucket removes a bucket and all its data
func (tx *Tx) DeleteBucket(name []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// Validate bucket name
	if len(name) == 0 {
		return errors.New("bucket name cannot be empty")
	}
	if string(name) == "__root__" {
		return errors.New("cannot delete reserved bucket __root__")
	}

	// Check if already deleted in this transaction
	if _, deleted := tx.deletes[string(name)]; deleted {
		return ErrBucketNotFound
	}

	// Load bucket metadata WITHOUT acquiring (since we're deleting it)
	// We don't need to protect it from deletion - we ARE the deleter
	meta, err := tx.search(tx.root, name)
	if err != nil {
		return ErrBucketNotFound
	}

	if len(meta) < 16 {
		return ErrBucketNotFound
	}

	// Deserialize just to get the rootID
	var bucket Bucket
	bucket.Deserialize(meta)

	// Delete bucket metadata from root tree
	root, err := tx.deleteFromNode(tx.root, name)
	if err != nil {
		return err
	}
	tx.root = root

	// Remove from cache if present
	delete(tx.buckets, string(name))

	// Mark bucket's root page for deletion on commit
	// The bucket will remain accessible to existing readers until their refcount drops to 0
	tx.deletes[string(name)] = bucket.rootID

	return nil
}

// ForEachBucket iterates over all buckets
func (tx *Tx) ForEachBucket(fn func(name []byte, b *Bucket) error) error {
	if err := tx.check(); err != nil {
		return err
	}

	// Create cursor for root tree (bucket directory)
	c := &Cursor{
		tx:         tx,
		bucketRoot: tx.root,
		valid:      false,
	}

	// Iterate over all bucket metadata
	for k, v := c.First(); k != nil; k, v = c.Next() {
		// Skip __root__ bucket
		if string(k) == "__root__" {
			continue
		}

		// Deserialize bucket metadata
		if len(v) < 16 {
			continue
		}

		var err error
		var bucket Bucket
		bucket.Deserialize(v)
		bucket.tx = tx
		bucket.writable = tx.writable
		bucket.name = k
		bucket.root, err = tx.loadNode(bucket.rootID)
		if err != nil {
			continue
		}

		// Call user function
		if err := fn(k, &bucket); err != nil {
			return err
		}
	}

	return nil
}

// ForEach iterates over all key-value pairs in the default bucket
func (tx *Tx) ForEach(fn func(key, value []byte) error) error {
	if err := tx.check(); err != nil {
		return err
	}

	// Get __root__ bucket (default namespace)
	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		return nil // No keys if bucket doesn't exist
	}

	// Delegate to bucket's ForEach
	return bucket.ForEach(fn)
}

// ForEachPrefix iterates over all key-value pairs in the default bucket that start with the given prefix
func (tx *Tx) ForEachPrefix(prefix []byte, fn func(key, value []byte) error) error {
	if err := tx.check(); err != nil {
		return err
	}

	// Get __root__ bucket (default namespace)
	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		return nil // No keys if bucket doesn't exist
	}

	// Create cursor for iteration
	c := bucket.Cursor()

	// Seek to the first key >= prefix
	k, v := c.Seek(prefix)

	// Iterate while keys match prefix
	for k != nil {
		// Check if key still has the prefix
		if !bytes.HasPrefix(k, prefix) {
			break // No more keys with this prefix
		}

		// Call user function
		if err := fn(k, v); err != nil {
			return err
		}

		// Move to next key
		k, v = c.Next()
	}

	return nil
}

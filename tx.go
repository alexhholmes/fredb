package fredb

import (
	"bytes"
	"errors"

	"fredb/internal/algo"
	"fredb/internal/base"
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
	txnID    uint64                     // Unique transaction ID
	writable bool                       // Is this a read-write transaction?
	db       *DB                        // Database this transaction belongs to (concrete type for internal access)
	root     *base.Node                 // Root Node at transaction start
	pages    map[base.PageID]*base.Node // TX-LOCAL: uncommitted COW pages (write transactions only)
	pending  map[base.PageID]struct{}   // Pages allocated in this transaction (for COW)
	freed    map[base.PageID]struct{}   // Pages freed in this transaction (for freelist)
	done     bool                       // Has Commit() or Rollback() been called?

	// Virtual page ID allocation for deferred real allocation
	nextVirtualID int64 // Starts at -1, decrements: -1, -2, -3, ...
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
	return tx.search(tx.root, key)
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
	if node.IsLeaf {
		// In leaf, we need to check the previous position (since loop went past equal keys)
		if i > 0 && bytes.Equal(key, node.Keys[i-1]) {
			return node.Values[i-1], nil
		}
		// Not found in leaf
		return nil, ErrKeyNotFound
	}

	// Branch node: continue descending
	// i is the correct child index (first child with keys >= search_key)
	child, err := tx.loadNode(node.Children[i])
	if err != nil {
		return nil, err
	}

	return tx.search(child, key)
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
	// Keep changes in tx.root (transaction-local), not DB.root

	// Use tx.root (already set from snapshot in Begin)
	root := tx.root

	// Handle root split with COW
	if root.IsFull() {
		// Split root using COW
		leftChild, rightChild, midKey, midVal, err := tx.splitChild(root)
		if err != nil {
			return err
		}

		// Create new root using tx.allocatePage()
		newRootID, _, err := tx.allocatePage()
		if err != nil {
			return err
		}

		newRoot := algo.NewBranchRoot(leftChild, rightChild, midKey, midVal, newRootID)

		// Don't add root to tx.pages - root is tracked separately in tx.root
		// The split Children were already stored in splitChild()

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
		leftChild, rightChild, midKey, midVal, err := tx.splitChild(root)
		if err != nil {
			return err
		}

		// Create new root
		newRootID, _, err := tx.allocatePage()
		if err != nil {
			return err
		}

		root = algo.NewBranchRoot(leftChild, rightChild, midKey, midVal, newRootID)

		// Don't add root to tx.pages - root is tracked separately in tx.root
		// Use new root for next retry (already assigned to root)
	}

	// Update transaction-local root (NOT DB.root)
	// Changes become visible only on Commit()
	tx.root = root

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
	// Keep changes in tx.root (transaction-local), not DB.root

	// Use tx.root (already set from snapshot in Begin)
	root := tx.root

	// Root is always a branch node (never nil, never leaf)
	// Even with NumKeys=0, root can have a child leaf with data

	// Perform COW-aware recursive deletion - capture returned root
	root, err := tx.deleteFromNode(root, key)
	if err != nil {
		return err
	}

	// Update transaction-local root (NOT DB.root)
	// Changes become visible only on Commit()
	tx.root = root

	return nil
}

// Cursor creates a new cursor for iterating over Keys.
// The cursor is bound to this transaction's snapshot.
func (tx *Tx) Cursor() *Cursor {
	// Pass transaction to cursor for snapshot isolation
	return &Cursor{
		tx:    tx,
		valid: false,
	}
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

	// Apply transaction changes to database (writers only need mu)
	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	// NEW: Allocate real page IDs for all virtual pages
	// Temporary map for remapping (local to this function, stack-allocated)
	virtualToReal := make(map[base.PageID]base.PageID)

	// Pass 1: Allocate real page IDs for all virtual pages (identified by map key)
	for pageID := range tx.pages {
		if int64(pageID) < 0 { // Virtual page ID (map key) - PageID is uint64, virtual IDs are negative
			// NOW allocate from PageManager (at commit time)
			realPageID, err := tx.db.pager.AllocatePage()
			if err != nil {
				// Rollback partial allocation - return already-allocated real pages
				for _, allocated := range virtualToReal {
					_ = tx.db.pager.FreePage(allocated)
				}
				return err
			}

			// Track mapping for remapping pass
			virtualToReal[pageID] = realPageID
		}
	}

	// Pass 2: Update all nodes' PageID fields and remap child pointers
	for pageID, node := range tx.pages {
		// Update this node's PageID if the map key was virtual
		if int64(pageID) < 0 {
			if realID, exists := virtualToReal[pageID]; exists {
				node.PageID = realID
			}
		}

		// Also check if node.PageID itself is virtual (defensive check for any edge cases)
		if int64(node.PageID) < 0 {
			if realID, exists := virtualToReal[node.PageID]; exists {
				node.PageID = realID
			}
		}

		// Remap child pointers in branch nodes
		if !node.IsLeaf {
			for i, childID := range node.Children {
				if realID, isVirtual := virtualToReal[childID]; isVirtual {
					node.Children[i] = realID
				}
			}
		}
	}

	// Pass 3: Handle root separately (not in tx.pages)
	// Allocate real page ID for root if it's virtual
	if tx.root != nil && int64(tx.root.PageID) < 0 {
		realPageID, err := tx.db.pager.AllocatePage()
		if err != nil {
			// Rollback partial allocation
			for _, allocated := range virtualToReal {
				_ = tx.db.pager.FreePage(allocated)
			}
			return err
		}
		virtualToReal[tx.root.PageID] = realPageID
		tx.root.PageID = realPageID
	}

	// Also remap root's children pointers
	if tx.root != nil && !tx.root.IsLeaf {
		for i, childID := range tx.root.Children {
			if realID, isVirtual := virtualToReal[childID]; isVirtual {
				tx.root.Children[i] = realID
			}
		}
	}

	// Write all TX-local pages directly to disk
	// IMPORTANT: Populate cache BEFORE updating root to prevent race condition
	// where concurrent readers capture new root but pages aren't available yet
	for _, node := range tx.pages {
		// Use node.PageID (already remapped to real page ID)

		// Serialize Node to a Page with this transaction's ID
		page, err := node.Serialize(tx.txnID)
		if err != nil {
			return err
		}

		// Write directly to pager (disk) using real page ID
		if err := tx.db.pager.WritePage(node.PageID, page); err != nil {
			return err
		}

		// Clear Dirty flag after successful write
		node.Dirty = false

		// Flush to global versioned cache with this transaction's ID using real page ID
		// This makes the new version visible to future transactions
		tx.db.cache.Put(node.PageID, tx.txnID, node)
	}

	// Write root separately (not in tx.pages)
	if tx.root != nil && tx.root.Dirty {
		// Serialize root
		page, err := tx.root.Serialize(tx.txnID)
		if err != nil {
			return err
		}

		// Write root to disk
		if err := tx.db.pager.WritePage(tx.root.PageID, page); err != nil {
			return err
		}

		// Clear Dirty flag
		tx.root.Dirty = false

		// Add root to cache
		tx.db.cache.Put(tx.root.PageID, tx.txnID, tx.root)
	}

	// Add freed pages to pending at this transaction ID
	// These pages were part of the previous version and can be reclaimed
	// when all readers that might reference them have finished.
	if len(tx.freed) > 0 {
		dm := tx.db.pager
		// Convert map to slice for FreePending
		freedSlice := make([]base.PageID, 0, len(tx.freed))
		for pageID := range tx.freed {
			freedSlice = append(freedSlice, pageID)
		}
		err := dm.FreePending(tx.txnID, freedSlice)
		if err != nil {
			// TODO
		}
	}

	// Write meta Page to disk for persistence
	// get current meta from storage
	meta := tx.db.pager.GetMeta()

	// Build new meta Page with updated root and incremented TxID
	if tx.root != nil {
		meta.RootPageID = tx.root.PageID
	}
	meta.TxID = tx.txnID
	meta.Checksum = meta.CalculateChecksum()

	// Update storage's in-memory meta WITH root pointer atomically
	// This makes all COW changes visible to future transactions
	// CRITICAL: Done AFTER cache population to ensure pages are available
	if err := tx.db.pager.PutSnapshot(meta, tx.root); err != nil {
		return err
	}

	// Conditionally fsync based on sync mode (this is the commit point!)
	if tx.db.options.syncMode == SyncEveryCommit {
		if err := tx.db.pager.Sync(); err != nil {
			return err
		}
	}

	// Commit the meta page after syncing
	tx.db.pager.CommitSnapshot()

	tx.db.writer.Store(nil)

	// Trigger background releaser to reclaim pending pages
	// Non-blocking send - if channel is full, next trigger will handle it
	select {
	case tx.db.releaseC <- 0:
	default:
	}

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
	if tx.writable {
		// Writers need lock to clean up
		tx.db.mu.Lock()
		defer tx.db.mu.Unlock()

		// Discard all transaction-local state
		// Virtual pages were never allocated from PageManager, so nothing to free
		tx.pages = nil
		tx.pending = nil

		tx.db.writer.Store(nil)
	} else {
		// Readers: lock-free removal from sync.Map
		tx.db.readers.Delete(tx)

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
	if _, inPending := tx.pending[node.PageID]; inPending {
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
// The allocated Page is tracked in tx.pending for COW semantics.
func (tx *Tx) allocatePage() (base.PageID, *base.Page, error) {
	// Generate virtual page ID (negative number)
	// These are transaction-local and never touch PageManager until commit
	virtualID := base.PageID(tx.nextVirtualID)
	tx.nextVirtualID--

	// Track as allocated in this transaction
	tx.pending[virtualID] = struct{}{}

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
	newNodeID, _, err := tx.allocatePage()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// State: construct right node
	newNode := &base.Node{
		PageID:   newNodeID,
		Dirty:    true,
		IsLeaf:   child.IsLeaf,
		NumKeys:  uint16(sp.RightCount),
		Keys:     rightKeys,
		Values:   rightVals,
		Children: rightChildren,
	}

	// State: truncate left (child already COW'd, safe to mutate)
	algo.TruncateLeft(child, sp)

	// I/O: store right node in tx cache
	tx.pages[newNodeID] = newNode

	return child, newNode, sp.SeparatorKey, []byte{}, nil
}

// loadFromDisk loads a node from disk with relocation support
func (tx *Tx) loadFromDisk(pageID base.PageID) (*base.Node, uint64, error) {
	// Check if a relocated version exists that's visible to this transaction
	relocPageID, relocTxnID := tx.db.pager.GetLatestVisible(pageID, tx.txnID)
	if relocPageID != 0 {
		// Found relocated version - read from relocated location
		page, err := tx.db.pager.ReadPage(relocPageID)
		if err != nil {
			return nil, 0, err
		}

		// Deserialize relocated page
		node := &base.Node{
			PageID: pageID, // Use original PageID, not relocated
			Dirty:  false,
		}

		if err := node.Deserialize(page); err != nil {
			return nil, 0, err
		}

		return node, relocTxnID, nil
	}

	// No relocated version - read from original location
	page, err := tx.db.pager.ReadPage(pageID)
	if err != nil {
		return nil, 0, err
	}

	// Deserialize page
	node := &base.Node{
		PageID: pageID,
		Dirty:  false,
	}

	header := page.Header()
	if err := node.Deserialize(page); err != nil {
		return nil, 0, err
	}

	// Cycle detection
	if !node.IsLeaf {
		for _, childID := range node.Children {
			if childID == pageID {
				return nil, 0, ErrCorruption
			}
		}
	}

	return node, header.TxnID, nil
}

// loadNode loads a node using hybrid cache: tx.pages → versioned global cache → disk
func (tx *Tx) loadNode(pageID base.PageID) (*base.Node, error) {
	// 1. Check TX-local cache first (if writable tx with uncommitted changes)
	if tx.writable && tx.pages != nil {
		if node, exists := tx.pages[pageID]; exists {
			return node, nil
		}
	}

	// 2. GetOrLoad atomically checks cache or coordinates disk load
	node, found := tx.db.cache.GetOrLoad(pageID, tx.txnID, func() (*base.Node, uint64, error) {
		return tx.loadFromDisk(pageID)
	})

	if !found {
		return nil, ErrKeyNotFound
	}

	// 3. Cycle detection on cached node
	if !node.IsLeaf {
		for _, childID := range node.Children {
			if childID == pageID {
				return nil, ErrCorruption
			}
		}
	}

	return node, nil
}

// insertNonFull inserts into a non-full node with COW
// Returns the (possibly new) root node after COW
func (tx *Tx) insertNonFull(node *base.Node, key, value []byte) (*base.Node, error) {
	if node.IsLeaf {
		// COW before modifying leaf
		node, err := tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}

		// Pure: find insert position
		pos := algo.FindInsertPosition(node, key)

		// Check for update
		if pos < int(node.NumKeys) && bytes.Equal(node.Keys[pos], key) {
			// Save old value for rollback
			oldValue := make([]byte, len(node.Values[pos]))
			copy(oldValue, node.Values[pos])

			algo.ApplyLeafUpdate(node, pos, value)

			// Check size after update
			if err := node.CheckOverflow(); err != nil {
				// Rollback: restore old value
				node.Values[pos] = oldValue
				return nil, err
			}
			return node, nil
		}

		// Insert new key-value using algo
		algo.ApplyLeafInsert(node, pos, key, value)

		// Check size after insertion
		if err := node.CheckOverflow(); err != nil {
			// Rollback: remove the inserted key/value
			node.Keys = algo.RemoveAt(node.Keys, pos)
			node.Values = algo.RemoveAt(node.Values, pos)
			node.NumKeys--
			return nil, err
		}

		return node, nil
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
	if child.IsFull() {
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
	if node.IsLeaf {
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

	algo.MergeNodes(leftNode, rightNode, parent.Keys[parentKeyIdx])
	algo.ApplyBranchRemoveSeparator(parent, parentKeyIdx)
	// Update parent's child pointer to merged node
	parent.Children[parentKeyIdx] = leftNode.PageID

	// Track right node as freed
	tx.addFreed(rightNode.PageID)

	return parent, nil
}

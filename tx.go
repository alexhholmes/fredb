package fredb

import (
	"bytes"
	"errors"

	"github.com/google/btree"

	"github.com/alexhholmes/fredb/internal/algo"
	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/pager"
)

// Tx represents a transaction on the database.
//
// CONCURRENCY: Transactions are NOT thread-safe and must only be used by a single
// goroutine at a time. Calling Put/Get/Delete/Commit/Rollback concurrently from
// multiple goroutines will cause panics and data corruption.
//
// Transactions provide a consistent view of the database at the point they were created.
// Read transactions can run concurrently, but only one write transaction can be active at a time.
type Tx struct {
	txID     uint64 // Writers: unique ID, Readers: snapshot of last committed write
	writable bool   // Is this a read-write transaction?
	done     bool   // Has Commit() or Rollback() been called?

	db   *DB           // Database this transaction belongs to (concrete type for internal access)
	root base.PageData // Root page at transaction start (stores bucket metadata, different from the `__root__` bucket root)

	// Bucket tracking
	buckets  map[string]*Bucket       // Cache of loaded buckets
	acquired map[base.PageID]struct{} // Buckets acquired from pager (need release)
	deletes  map[string]base.PageID   // Root pages of buckets deleted in this transaction, for background cleanup upon commit

	// Page tracking
	pages     *btree.BTreeG[base.PageData]     // TX-LOCAL: uncommitted COW pages (write transactions only)
	freed     map[base.PageID]struct{}         // Pages freed in this transaction (for freelist)
	allocated map[base.PageID]pager.Allocation // Pages allocated in this transaction (false for freelist allocation, true for increment allocation)

	// Reader tracking
	unregister func() // Slot unregister function in readerSlots array (only for read-only transactions)
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

	return val, nil
}

// search recursively searches for a key using algo functions
func (tx *Tx) search(node base.PageData, key []byte) ([]byte, error) {
	// Handle based on page type
	if node.PageType() == base.LeafPageFlag {
		leaf := node.(*base.LeafPage)
		// Find position in leaf
		i := 0
		for i < int(leaf.Header.NumKeys) && bytes.Compare(key, leaf.Keys[i]) >= 0 {
			i++
		}
		// In leaf, check if key found at previous position
		if i > 0 && bytes.Equal(key, leaf.Keys[i-1]) {
			return leaf.Values[i-1], nil
		}
		return nil, ErrKeyNotFound
	}

	// Branch node: find child and descend
	branch := node.(*base.BranchPage)
	i := 0
	for i < int(branch.Header.NumKeys) && bytes.Compare(key, branch.Keys[i]) >= 0 {
		i++
	}

	children := branch.Children()
	child, err := tx.loadNode(children[i])
	if err != nil {
		return nil, err
	}

	return tx.search(child, key)
}

// Put writes a key-value pair to the default bucket.
// Returns ErrTxNotWritable if called on a read-only transaction.
// Returns ErrKeyTooLarge if key exceeds MaxKeySize (32KB).
// Returns ErrValueTooLarge if value exceeds MaxValueSize (16MB).
func (tx *Tx) Put(key, value []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

	// Validate key/value size
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	bucket := tx.Bucket([]byte("__root__"))
	if bucket == nil {
		return ErrBucketNotFound
	}
	return bucket.Put(key, value)
}

// Delete removes a key from the default bucket.
// Returns ErrTxNotWritable if called on a read-only transaction.
// Idempotent: returns nil if key doesn't exist.
func (tx *Tx) Delete(key []byte) error {
	if err := tx.check(); err != nil {
		return err
	}
	if !tx.writable {
		return ErrTxNotWritable
	}

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

	// deserialize bucket metadata
	if len(meta) < 16 {
		return nil
	}

	bucket := &Bucket{}
	bucket.deserialize(meta)
	bucket.tx = tx
	bucket.writable = tx.writable
	bucket.name = name
	bucket.root, err = tx.loadNode(bucket.rootID)
	if err != nil {
		return nil
	}

	// Important, get a lock on the bucket through the pager in case
	// the bucket is being deleted concurrently by another transaction.
	// This must be released on rollback or commit.
	// Exception: __root__ bucket is never deleted, so no need to track it
	if string(name) != "__root__" {
		acquired := tx.db.pager.AcquireBucket(bucket.rootID)
		if !acquired {
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

	bucketRootID := tx.allocatePage()
	bucketLeafID := tx.allocatePage()

	// Create bucket's leaf node (empty)
	bucketLeaf := base.NewLeafPage()
	bucketLeaf.SetPageID(bucketLeafID)
	bucketLeaf.SetDirty(true)

	// Create bucket's root node (branch with single child)
	bucketRoot := base.NewBranchPage()
	bucketRoot.SetPageID(bucketRootID)
	bucketRoot.SetDirty(true)
	bucketRoot.FirstChild = bucketLeafID

	// Add nodes to transaction cache
	tx.pages.ReplaceOrInsert(bucketLeaf)
	tx.pages.ReplaceOrInsert(bucketRoot)

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

	// deserialize just to get the rootID
	var bucket Bucket
	bucket.deserialize(meta)

	// Delete bucket metadata from root tree
	root, err := tx.deleteFromNode(tx.root, name)
	if err != nil {
		return err
	}
	tx.root = root

	// Shrink tree: if root is internal with single child, make child the new root
	if tx.root.PageType() == base.BranchPageFlag {
		branch := tx.root.(*base.BranchPage)
		children := branch.Children()
		if len(children) == 1 {
			child, err := tx.loadNode(children[0])
			if err != nil {
				return err
			}
			tx.root = child
		}
	}

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

		// deserialize bucket metadata
		if len(v) < 16 {
			continue
		}

		var err error
		var bucket Bucket
		bucket.deserialize(v)
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

	for name, bucket := range tx.buckets {
		if bucket.writable {
			key := []byte(name)
			value := bucket.serialize()

			// Validate key/value size before insertion
			if len(key) > MaxKeySize {
				return ErrKeyTooLarge
			}
			if len(value) > MaxValueSize {
				return ErrValueTooLarge
			}

			// Check key size fits in page (values can overflow)
			maxKeySize := base.PageSize - base.PageHeaderSize - base.LeafElementSize
			if len(key) > maxKeySize {
				return ErrPageOverflow
			}

			// Use COW-aware insertion with splits
			// Keep changes in tx.root (transaction-local), not DB.root
			root := tx.root

			// Handle root split with COW
			var needsSplit bool
			if root.PageType() == base.LeafPageFlag {
				needsSplit = root.(*base.LeafPage).IsFull(key, value)
			} else {
				needsSplit = root.(*base.BranchPage).IsFull(key)
			}

			if needsSplit {
				// Split root using COW
				leftChild, rightChild, midKey, _, err := tx.splitChild(root, key)
				if err != nil {
					return err
				}

				// Create new root using tx.allocatePage()
				newRootID := tx.allocatePage()
				newRoot := algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)

				// CRITICAL: Add new root to tx.pages if it has a virtual ID
				// Otherwise it won't get a real page ID allocated during Commit
				if int64(newRootID) < 0 {
					tx.pages.ReplaceOrInsert(newRoot)
				}

				root = newRoot
			}

			// Insert with recursive CoW - retry until success
			for {
				newRoot, err := tx.insertNonFull(root, key, value)
				if !errors.Is(err, ErrPageOverflow) {
					// Either success or non-recoverable error
					if err == nil {
						root = newRoot // Only update root on success
					}
					break
				}

				// Root couldn't fit the new entry - split it
				leftChild, rightChild, midKey, _, err := tx.splitChild(root, key)
				if err != nil {
					return err
				}

				// Create new root
				newRootID := tx.allocatePage()
				newRoot2 := algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)

				// CRITICAL: Add new root to tx.pages if it has a virtual ID
				// Otherwise it won't get a real page ID allocated during Commit
				if int64(newRootID) < 0 {
					tx.pages.ReplaceOrInsert(newRoot2)
				}

				root = newRoot2
			}

			// Update transaction-local root (NOT DB.root)
			// Changes become visible only on Commit()
			tx.root = root
		}
	}

	// Phase 3: Write everything to disk in a single operation
	// This handles: page writes, freed pages, meta update, sync (if needed), and CommitSnapshot
	err := tx.db.pager.Commit(
		tx.pages,
		tx.root,
		tx.freed,
		tx.txID,
	)
	if err != nil {
		return err
	}

	tx.done = true

	// Phase 4: Add deleted buckets to pager's Deleted map BEFORE releasing buckets
	// This prevents new readers from acquiring deleted buckets after this commit
	if tx.writable {
		tx.db.pager.DeletedMu.Lock()
		for _, pageID := range tx.deletes {
			tx.db.pager.Deleted[pageID] = struct{}{}
		}
		tx.db.pager.DeletedMu.Unlock()
	}

	tx.db.writer.Store(nil)

	// Phase 5: Release acquired buckets after marking deleted ones
	// This must happen AFTER releasing DeletedMu to avoid deadlock in ReleaseBucket()
	// Only release buckets that were actually acquired (not newly created ones)
	tx.tryReleasePages()
	for pageID := range tx.acquired {
		tx.db.pager.ReleaseBucket(pageID, tx.freeTree)
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

	// Release all acquired buckets (both read and write transactions)
	// Only release buckets that were actually acquired (not newly created ones)
	for pageID := range tx.acquired {
		tx.db.pager.ReleaseBucket(pageID, tx.freeTree)
	}

	// Remove from DB tracking
	if tx.writable {
		// Writers need lock to clean up
		tx.db.mu.Lock()
		defer tx.db.mu.Unlock()

		tx.db.writer.Store(nil)

		tx.tryReleasePages()

		// Return freelist pages back to freelist (fresh pages become holes)
		for pageID, allocated := range tx.allocated {
			if allocated == pager.FromIncrement {
				tx.db.pager.FreePage(pageID)
			}
		}
	} else {
		tx.unregister()
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

// EnsureWritable ensures a page is safe to modify in this transaction.
// Performs COW only if the page doesn't already belong to this transaction.
// Returns a writable page (either the original if already owned, or a Clone).
func (tx *Tx) ensureWritable(node base.PageData) (base.PageData, error) {
	// Create a dummy for lookup
	dummy := base.NewLeafPage()
	dummy.SetPageID(node.GetPageID())
	if cloned, exists := tx.pages.Get(dummy); exists {
		return cloned, nil
	}

	// Clone based on type
	var cloned base.PageData
	switch n := node.(type) {
	case *base.LeafPage:
		cloned = n.Clone()
	case *base.BranchPage:
		cloned = n.Clone()
	case *base.OverflowPage:
		cloned = n.Clone()
	default:
		return nil, errors.New("unknown page type")
	}

	// Allocate new page ID
	newID := tx.allocatePage()
	cloned.SetPageID(newID)
	cloned.SetDirty(true)

	// Free the old page
	tx.addFreed(node.GetPageID())

	// Store in TX-LOCAL cache (NOT global cache yet)
	// Will be flushed to global cache on Commit()
	tx.pages.ReplaceOrInsert(cloned)

	return cloned, nil
}

// Allocates a new Page for this transaction.
func (tx *Tx) allocatePage() base.PageID {
	id, allocated := tx.db.pager.AssignPageID()
	tx.allocated[id] = allocated
	return id
}

// AddFreed adds a Page to the freed list, checking for duplicates first.
// This prevents the same Page from being freed multiple times in a transaction.
func (tx *Tx) addFreed(pageID base.PageID) {
	if pageID == 0 {
		return
	}

	// Page is from a previous transaction - safe to free
	tx.freed[pageID] = struct{}{}
}

// splitChild performs COW on the child being split and allocates the new sibling
func (tx *Tx) splitChild(child base.PageData, insertKey []byte) (base.PageData, base.PageData, []byte, []byte, error) {
	// I/O: COW BEFORE any computation
	child, err := tx.ensureWritable(child)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Pure: calculate split point with adaptive hint
	sp := algo.CalculateSplitPointWithHint(child, insertKey, algo.SplitBalanced)

	// Pure: extract right portion (read-only)
	rightKeys, rightVals, rightChildren := algo.ExtractRightPortion(child, sp)

	// I/O: allocate page for right node
	nodeID := tx.allocatePage()

	// State: construct right node based on type
	var node base.PageData
	if child.PageType() == base.LeafPageFlag {
		leaf := base.NewLeafPage()
		leaf.SetPageID(nodeID)
		leaf.SetDirty(true)
		leaf.Header.NumKeys = uint16(sp.RightCount)
		leaf.Keys = rightKeys
		leaf.Values = rightVals
		node = leaf
	} else {
		branch := base.NewBranchPage()
		branch.SetPageID(nodeID)
		branch.SetDirty(true)
		branch.Header.NumKeys = uint16(sp.RightCount)
		branch.Keys = rightKeys
		if len(rightChildren) > 0 {
			branch.FirstChild = rightChildren[0]
			branch.Elements = make([]base.BranchElement, len(rightChildren)-1)
			for i := 1; i < len(rightChildren); i++ {
				branch.Elements[i-1].ChildID = rightChildren[i]
			}
		}
		node = branch
	}

	// State: truncate left (child already COW'd, safe to mutate)
	algo.TruncateLeft(child, sp)

	// I/O: store right node in tx cache
	tx.pages.ReplaceOrInsert(node)

	return child, node, sp.SeparatorKey, []byte{}, nil
}

// loadNode loads a node using the pager: tx.pages → Pager → Storage
func (tx *Tx) loadNode(pageID base.PageID) (base.PageData, error) {
	// Check TX-local cache first (if writable tx with uncommitted changes)
	if tx.writable && tx.pages != nil {
		// Create a dummy page for lookup (comparator only uses PageID)
		dummy := base.NewLeafPage()
		dummy.SetPageID(pageID)
		if node, exists := tx.pages.Get(dummy); exists {
			return node, nil
		}
	}

	node, err := tx.db.pager.GetNode(pageID)
	if err != nil {
		return nil, err
	}

	return node, nil
}

// insertNonFull inserts into a non-full node with COW
// Returns the (possibly new) root node after COW
func (tx *Tx) insertNonFull(node base.PageData, key, value []byte) (base.PageData, error) {
	if node.PageType() == base.LeafPageFlag {
		// COW before modifying leaf
		n, err := tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}
		leaf := n.(*base.LeafPage)

		// Pure: find insert position
		pos := algo.FindInsertPosition(leaf, key)

		// Check for update
		if pos < int(leaf.Header.NumKeys) && bytes.Equal(leaf.Keys[pos], key) {
			// Make a copy of old value for rollback
			oldValue := leaf.Values[pos]
			oldValueCopy := make([]byte, len(oldValue))
			copy(oldValueCopy, oldValue)

			algo.ApplyLeafUpdate(leaf, pos, value)

			// Check size after update
			if err := leaf.CheckOverflow(); err != nil {
				// Rollback: restore old value
				leaf.Values[pos] = oldValueCopy
				return nil, err
			}
			return leaf, nil
		}

		// Insert new key-value using algo
		algo.ApplyLeafInsert(leaf, pos, key, value)

		// Check size after insertion
		if err := leaf.CheckOverflow(); err != nil {
			// Rollback: remove the inserted key/value
			leaf.Keys = algo.RemoveAt(leaf.Keys, pos)
			leaf.Values = algo.RemoveAt(leaf.Values, pos)
			leaf.Header.NumKeys--
			return nil, err
		}

		return leaf, nil
	}

	// Branch node - recursive COW
	branch := node.(*base.BranchPage)

	// Find child to insert into
	i := algo.FindChildIndex(branch, key)

	// Load child
	children := branch.Children()
	child, err := tx.loadNode(children[i])
	if err != nil {
		return nil, err
	}

	// Handle full child with COW-aware split
	var needsSplit bool
	if child.PageType() == base.LeafPageFlag {
		needsSplit = child.(*base.LeafPage).IsFull(key, value)
	} else {
		needsSplit = child.(*base.BranchPage).IsFull(key)
	}

	if needsSplit {
		// Split child using COW
		leftChild, rightChild, midKey, midVal, err := tx.splitChild(child, key)
		if err != nil {
			return nil, err
		}

		// COW parent to insert middle key and update pointers
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}
		branch = node.(*base.BranchPage)

		// Save old state for rollback
		oldKeys := branch.Keys
		oldFirstChild := branch.FirstChild
		oldElements := branch.Elements
		oldNumKeys := branch.Header.NumKeys

		// Apply split to parent using algo
		algo.ApplyChildSplit(branch, i, leftChild, rightChild, midKey, midVal)

		// Check size after modification
		if err := branch.CheckOverflow(); err != nil {
			// Rollback: restore old state
			branch.Keys = oldKeys
			branch.FirstChild = oldFirstChild
			branch.Elements = oldElements
			branch.Header.NumKeys = oldNumKeys
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
	oldChildID := child.GetPageID()

	// Recursive insert (may COW child)
	newChild, err := tx.insertNonFull(child, key, value)
	if errors.Is(err, ErrPageOverflow) {
		// Child couldn't fit the key/value - split it (use original child, not nil from error)
		leftChild, rightChild, midKey, midVal, err := tx.splitChild(child, key)
		if err != nil {
			return nil, err
		}

		// COW parent to insert middle key and update pointers
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}
		branch = node.(*base.BranchPage)

		// Save old state for rollback
		oldKeys := branch.Keys
		oldFirstChild := branch.FirstChild
		oldElements := branch.Elements
		oldNumKeys := branch.Header.NumKeys

		// Apply split to parent using algo
		algo.ApplyChildSplit(branch, i, leftChild, rightChild, midKey, midVal)

		// Check size after modification
		if err := branch.CheckOverflow(); err != nil {
			// Rollback: restore old state
			branch.Keys = oldKeys
			branch.FirstChild = oldFirstChild
			branch.Elements = oldElements
			branch.Header.NumKeys = oldNumKeys
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

		return branch, nil
	} else if err != nil {
		return nil, err
	}

	// Success - update child with returned value
	child = newChild

	// If child was COW'd, update parent pointer
	if child.GetPageID() != oldChildID {
		// COW parent to update child pointer
		node, err = tx.ensureWritable(node)
		if err != nil {
			return nil, err
		}
		branch = node.(*base.BranchPage)

		// Update child pointer in FirstChild or Elements
		children = branch.Children()
		if i == 0 {
			branch.FirstChild = child.GetPageID()
		} else {
			branch.Elements[i-1].ChildID = child.GetPageID()
		}
		branch.SetDirty(true)
		if err := branch.CheckOverflow(); err != nil {
			return nil, err
		}
	}

	return node, nil
}

// deleteFromNode recursively deletes a key from the subtree rooted at node with COW
// Returns the (possibly new) node after COW
func (tx *Tx) deleteFromNode(node base.PageData, key []byte) (base.PageData, error) {
	// B+ tree: if this is a leaf, check if key exists and delete
	if node.PageType() == base.LeafPageFlag {
		idx := algo.FindKeyInLeaf(node, key)
		if idx >= 0 {
			return tx.deleteFromLeaf(node, idx)
		}
		return nil, ErrKeyNotFound
	}

	// Branch node: descend to child (never delete from branch)
	branch := node.(*base.BranchPage)

	// Find child where key might be using algo
	childIdx := algo.FindDeleteChildIndex(node, key)

	children := branch.Children()
	child, err := tx.loadNode(children[childIdx])
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
	branch = node.(*base.BranchPage)

	// Update child pointer (child may have been COW'd)
	if childIdx == 0 {
		branch.FirstChild = child.GetPageID()
	} else {
		branch.Elements[childIdx-1].ChildID = child.GetPageID()
	}

	// Check for underflow only if there are siblings to borrow from or merge with
	// When parent has only 1 child (e.g., root with single child), underflow is allowed
	children = branch.Children()
	var childUnderflow bool
	if child.PageType() == base.LeafPageFlag {
		childUnderflow = child.(*base.LeafPage).IsUnderflow()
	} else {
		childUnderflow = child.(*base.BranchPage).IsUnderflow()
	}

	if childUnderflow && len(children) > 1 {
		// CRITICAL: capture both returned parent and child
		node, child, err = tx.fixUnderflow(node, childIdx, child)
		if err != nil {
			return nil, err
		}
	}

	return node, nil
}

// deleteFromLeaf performs COW on the leaf before deleting
func (tx *Tx) deleteFromLeaf(node base.PageData, idx int) (base.PageData, error) {
	// COW before modifying leaf
	node, err := tx.ensureWritable(node)
	if err != nil {
		return nil, err
	}
	leaf := node.(*base.LeafPage)

	// Remove key and value using algo
	algo.ApplyLeafDelete(leaf, idx)

	return leaf, nil
}

// fixUnderflow fixes underflow in child at childIdx with COW semantics
// Returns (updatedParent, updatedChild, error)
func (tx *Tx) fixUnderflow(parent base.PageData, childIdx int, child base.PageData) (base.PageData, base.PageData, error) {
	branch := parent.(*base.BranchPage)
	children := branch.Children()

	// Try to borrow from left sibling
	if childIdx > 0 {
		leftSibling, err := tx.loadNode(children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}

		var canBorrow bool
		if leftSibling.PageType() == base.LeafPageFlag {
			leaf := leftSibling.(*base.LeafPage)
			canBorrow = leaf.Header.NumKeys > 1 && !leaf.IsUnderflow()
		} else {
			branch := leftSibling.(*base.BranchPage)
			canBorrow = branch.Header.NumKeys > 1 && !branch.IsUnderflow()
		}

		if canBorrow {
			child, leftSibling, parent, err = tx.borrowFromLeft(child, leftSibling, parent, childIdx-1)
			if err != nil {
				return nil, nil, err
			}
			// Update parent's child pointer for borrowed child
			branch = parent.(*base.BranchPage)
			if childIdx == 0 {
				branch.FirstChild = child.GetPageID()
			} else {
				branch.Elements[childIdx-1].ChildID = child.GetPageID()
			}
			return parent, child, nil
		}
	}

	// Try to borrow from right sibling
	if childIdx < len(children)-1 {
		rightSibling, err := tx.loadNode(children[childIdx+1])
		if err != nil {
			return nil, nil, err
		}

		var canBorrow bool
		if rightSibling.PageType() == base.LeafPageFlag {
			leaf := rightSibling.(*base.LeafPage)
			canBorrow = leaf.Header.NumKeys > 1 && !leaf.IsUnderflow()
		} else {
			branch := rightSibling.(*base.BranchPage)
			canBorrow = branch.Header.NumKeys > 1 && !branch.IsUnderflow()
		}

		if canBorrow {
			child, rightSibling, parent, err = tx.borrowFromRight(child, rightSibling, parent, childIdx)
			if err != nil {
				return nil, nil, err
			}
			// Update parent's child pointer for borrowed child
			branch = parent.(*base.BranchPage)
			if childIdx == 0 {
				branch.FirstChild = child.GetPageID()
			} else {
				branch.Elements[childIdx-1].ChildID = child.GetPageID()
			}
			return parent, child, nil
		}
	}

	// Merge with a sibling
	if childIdx > 0 {
		// Merge with left sibling
		leftSibling, err := tx.loadNode(children[childIdx-1])
		if err != nil {
			return nil, nil, err
		}
		parent, _, err = tx.mergeNodes(leftSibling, child, parent, childIdx-1)
		if err != nil {
			return nil, nil, err
		}
		// Redistributed - both nodes remain, child is still at childIdx
		return parent, child, nil
	}

	// Merge with right sibling
	rightSibling, err := tx.loadNode(children[childIdx+1])
	if err != nil {
		return nil, nil, err
	}
	parent, _, err = tx.mergeNodes(child, rightSibling, parent, childIdx)
	if err != nil {
		return nil, nil, err
	}
	// After merge (or redistribution), child remains as the result node
	return parent, child, nil
}

// borrowFromLeft borrows a key from left sibling through parent (COW)
func (tx *Tx) borrowFromLeft(node, leftSibling, parent base.PageData, parentKeyIdx int) (base.PageData, base.PageData, base.PageData, error) {
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

	// Call algo function based on node type
	if node.PageType() == base.LeafPageFlag {
		algo.BorrowFromLeftLeaf(node.(*base.LeafPage), leftSibling.(*base.LeafPage), parent.(*base.BranchPage), parentKeyIdx)
	} else {
		algo.BorrowFromLeftBranch(node.(*base.BranchPage), leftSibling.(*base.BranchPage), parent.(*base.BranchPage), parentKeyIdx)
	}

	// Update parent's children pointers to COW'd nodes
	branch := parent.(*base.BranchPage)
	if parentKeyIdx == 0 {
		branch.FirstChild = leftSibling.GetPageID()
	} else {
		branch.Elements[parentKeyIdx-1].ChildID = leftSibling.GetPageID()
	}
	if parentKeyIdx+1 == 0 {
		branch.FirstChild = node.GetPageID()
	} else {
		branch.Elements[parentKeyIdx].ChildID = node.GetPageID()
	}

	return node, leftSibling, parent, nil
}

// borrowFromRight borrows a key from right sibling through parent (COW)
func (tx *Tx) borrowFromRight(node, rightSibling, parent base.PageData, parentKeyIdx int) (base.PageData, base.PageData, base.PageData, error) {
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

	// Call algo function based on node type
	if node.PageType() == base.LeafPageFlag {
		algo.BorrowFromRightLeaf(node.(*base.LeafPage), rightSibling.(*base.LeafPage), parent.(*base.BranchPage), parentKeyIdx)
	} else {
		algo.BorrowFromRightBranch(node.(*base.BranchPage), rightSibling.(*base.BranchPage), parent.(*base.BranchPage), parentKeyIdx)
	}

	// Update parent's children pointers to COW'd nodes
	branch := parent.(*base.BranchPage)
	if parentKeyIdx == 0 {
		branch.FirstChild = node.GetPageID()
	} else {
		branch.Elements[parentKeyIdx-1].ChildID = node.GetPageID()
	}
	if parentKeyIdx+1 == 0 {
		branch.FirstChild = rightSibling.GetPageID()
	} else {
		branch.Elements[parentKeyIdx].ChildID = rightSibling.GetPageID()
	}

	return node, rightSibling, parent, nil
}

// mergeNodes merges two nodes with COW semantics
// Returns (parent, merged, error) where merged=true if nodes were actually merged, false if redistributed
func (tx *Tx) mergeNodes(leftNode, rightNode, parent base.PageData, parentKeyIdx int) (base.PageData, bool, error) {
	// COW left node (will receive merged content)
	leftNode, err := tx.ensureWritable(leftNode)
	if err != nil {
		return nil, false, err
	}

	// COW right node (may need it for redistribution)
	rightNode, err = tx.ensureWritable(rightNode)
	if err != nil {
		return nil, false, err
	}

	// COW parent (will have separator removed or updated)
	parent, err = tx.ensureWritable(parent)
	if err != nil {
		return nil, false, err
	}

	// Try merging - check if result would overflow
	// For large keys/values, two underflow nodes might be nearly full in bytes
	// Calculate merged size before actually merging
	var mergedSize int
	if leftNode.PageType() == base.LeafPageFlag {
		leftLeaf := leftNode.(*base.LeafPage)
		rightLeaf := rightNode.(*base.LeafPage)
		mergedSize = leftLeaf.Size() + rightLeaf.Size()
	} else {
		leftBranch := leftNode.(*base.BranchPage)
		rightBranch := rightNode.(*base.BranchPage)
		mergedSize = leftBranch.Size() + rightBranch.Size()
		// Branch nodes: add separator key size
		branch := parent.(*base.BranchPage)
		mergedSize += len(branch.Keys[parentKeyIdx])
	}

	if mergedSize > base.PageSize {
		// Merge would overflow - redistribute keys to balance nodes instead
		// This keeps both nodes valid (above MinKeysPerNode if possible)
		// without violating page size constraints
		err := tx.redistributeNodes(leftNode, rightNode, parent, parentKeyIdx)
		if err != nil {
			return nil, false, err
		}
		// Update parent's child pointers (both nodes remain)
		branch := parent.(*base.BranchPage)
		if parentKeyIdx == 0 {
			branch.FirstChild = leftNode.GetPageID()
		} else {
			branch.Elements[parentKeyIdx-1].ChildID = leftNode.GetPageID()
		}
		if parentKeyIdx+1 == 0 {
			branch.FirstChild = rightNode.GetPageID()
		} else {
			branch.Elements[parentKeyIdx].ChildID = rightNode.GetPageID()
		}
		return parent, false, nil // false = redistributed, not merged
	}

	// Perform merge based on node type
	branch := parent.(*base.BranchPage)
	if leftNode.PageType() == base.LeafPageFlag {
		leftLeaf := leftNode.(*base.LeafPage)
		rightLeaf := rightNode.(*base.LeafPage)
		algo.MergeNodesLeaf(leftLeaf, rightLeaf, branch.Keys[parentKeyIdx])
	} else {
		leftBranch := leftNode.(*base.BranchPage)
		rightBranch := rightNode.(*base.BranchPage)
		algo.MergeNodesBranch(leftBranch, rightBranch, branch.Keys[parentKeyIdx])
	}
	algo.ApplyBranchRemoveSeparator(branch, parentKeyIdx)

	// Update parent's child pointer to merged node
	if parentKeyIdx == 0 {
		branch.FirstChild = leftNode.GetPageID()
	} else {
		branch.Elements[parentKeyIdx-1].ChildID = leftNode.GetPageID()
	}

	// Track right node as freed
	tx.addFreed(rightNode.GetPageID())

	return parent, true, nil // true = actually merged
}

// redistributeNodes redistributes keys evenly between two siblings when merge would overflow
// This is called when both nodes are underflow but merging them would exceed page size
func (tx *Tx) redistributeNodes(leftNode, rightNode, parent base.PageData, parentKeyIdx int) error {
	if leftNode.PageType() == base.LeafPageFlag {
		leftLeaf := leftNode.(*base.LeafPage)
		rightLeaf := rightNode.(*base.LeafPage)
		parentBranch := parent.(*base.BranchPage)

		// Combine all keys and values
		totalKeys := int(leftLeaf.Header.NumKeys) + int(rightLeaf.Header.NumKeys)
		allKeys := make([][]byte, 0, totalKeys)
		allValues := make([][]byte, 0, totalKeys)

		allKeys = append(allKeys, leftLeaf.Keys[:leftLeaf.Header.NumKeys]...)
		allKeys = append(allKeys, rightLeaf.Keys[:rightLeaf.Header.NumKeys]...)
		allValues = append(allValues, leftLeaf.Values[:leftLeaf.Header.NumKeys]...)
		allValues = append(allValues, rightLeaf.Values[:rightLeaf.Header.NumKeys]...)

		// Find split point by size, not count
		// Target: split when left node reaches ~half of combined size
		totalSize := 0
		for i := 0; i < totalKeys; i++ {
			totalSize += base.LeafElementSize + len(allKeys[i]) + len(allValues[i])
		}
		targetSize := totalSize / 2

		leftCount := 0
		leftSize := base.PageHeaderSize
		for i := 0; i < totalKeys-1; i++ { // -1 to ensure right has at least 1 key
			entrySize := base.LeafElementSize + len(allKeys[i]) + len(allValues[i])
			if leftSize+entrySize > targetSize && i > 0 {
				break
			}
			leftSize += entrySize
			leftCount++
		}

		// Ensure at least 1 key in each node
		if leftCount == 0 {
			leftCount = 1
		}
		if leftCount >= totalKeys {
			leftCount = totalKeys - 1
		}

		// Split at calculated point
		leftLeaf.Header.NumKeys = uint16(leftCount)
		leftLeaf.Keys = make([][]byte, leftCount)
		leftLeaf.Values = make([][]byte, leftCount)
		copy(leftLeaf.Keys, allKeys[:leftCount])
		copy(leftLeaf.Values, allValues[:leftCount])

		rightCount := totalKeys - leftCount
		rightLeaf.Header.NumKeys = uint16(rightCount)
		rightLeaf.Keys = make([][]byte, rightCount)
		rightLeaf.Values = make([][]byte, rightCount)
		copy(rightLeaf.Keys, allKeys[leftCount:])
		copy(rightLeaf.Values, allValues[leftCount:])

		// Update parent separator to first key of right node
		parentBranch.Keys[parentKeyIdx] = rightLeaf.Keys[0]

		// Check for overflow after redistribution
		if err := leftLeaf.CheckOverflow(); err != nil {
			return err
		}
		if err := rightLeaf.CheckOverflow(); err != nil {
			return err
		}

		leftLeaf.SetDirty(true)
		rightLeaf.SetDirty(true)
	} else {
		leftBranch := leftNode.(*base.BranchPage)
		rightBranch := rightNode.(*base.BranchPage)
		parentBranch := parent.(*base.BranchPage)

		// Branch node: include parent separator key in redistribution
		totalKeys := int(leftBranch.Header.NumKeys) + int(rightBranch.Header.NumKeys) + 1 // +1 for parent separator

		allKeys := make([][]byte, 0, totalKeys)
		allChildren := make([]base.PageID, 0, totalKeys+1)

		// Left node keys and children
		leftChildren := leftBranch.Children()
		allKeys = append(allKeys, leftBranch.Keys[:leftBranch.Header.NumKeys]...)
		allChildren = append(allChildren, leftChildren[:leftBranch.Header.NumKeys+1]...)

		// Parent separator
		allKeys = append(allKeys, parentBranch.Keys[parentKeyIdx])

		// Right node keys and children
		rightChildren := rightBranch.Children()
		allKeys = append(allKeys, rightBranch.Keys[:rightBranch.Header.NumKeys]...)
		allChildren = append(allChildren, rightChildren[:rightBranch.Header.NumKeys+1]...)

		// Find split point by size
		totalSize := 0
		for i := 0; i < totalKeys; i++ {
			totalSize += base.BranchElementSize + len(allKeys[i])
		}
		targetSize := totalSize / 2

		splitIdx := 0
		leftSize := base.PageHeaderSize + 8 // +8 for first child pointer
		for i := 0; i < totalKeys-1; i++ {  // -1 to ensure right has at least 1 key
			entrySize := base.BranchElementSize + len(allKeys[i])
			if leftSize+entrySize > targetSize && i > 0 {
				break
			}
			leftSize += entrySize
			splitIdx++
		}

		// Ensure at least 1 key in each side
		if splitIdx == 0 {
			splitIdx = 1
		}
		if splitIdx >= totalKeys-1 {
			splitIdx = totalKeys - 2
		}

		// Split keys (separator goes to parent)
		newSeparator := allKeys[splitIdx]

		leftBranch.Header.NumKeys = uint16(splitIdx)
		leftBranch.Keys = make([][]byte, splitIdx)
		newLeftChildren := allChildren[:splitIdx+1]
		leftBranch.FirstChild = newLeftChildren[0]
		leftBranch.Elements = make([]base.BranchElement, len(newLeftChildren)-1)
		for i := 1; i < len(newLeftChildren); i++ {
			leftBranch.Elements[i-1].ChildID = newLeftChildren[i]
		}
		copy(leftBranch.Keys, allKeys[:splitIdx])

		rightCount := totalKeys - splitIdx - 1 // -1 because separator goes to parent
		rightBranch.Header.NumKeys = uint16(rightCount)
		rightBranch.Keys = make([][]byte, rightCount)
		newRightChildren := allChildren[splitIdx+1:]
		rightBranch.FirstChild = newRightChildren[0]
		rightBranch.Elements = make([]base.BranchElement, len(newRightChildren)-1)
		for i := 1; i < len(newRightChildren); i++ {
			rightBranch.Elements[i-1].ChildID = newRightChildren[i]
		}
		copy(rightBranch.Keys, allKeys[splitIdx+1:])

		// Update parent separator
		parentBranch.Keys[parentKeyIdx] = newSeparator

		// Check for overflow after redistribution
		if err := leftBranch.CheckOverflow(); err != nil {
			return err
		}
		if err := rightBranch.CheckOverflow(); err != nil {
			return err
		}

		leftBranch.SetDirty(true)
		rightBranch.SetDirty(true)
	}

	parent.SetDirty(true)

	return nil
}

// tryReleasePages releases pages that are safe to reuse based on active transactions.
func (tx *Tx) tryReleasePages() {
	minTxID := tx.db.nextTxID.Load()

	// Consider active write transaction
	if writerTx := tx.db.writer.Load(); writerTx != nil {
		if writerTx.txID < minTxID {
			minTxID = writerTx.txID
		}
	}

	// Check reader slots (returns 0 if no readers)
	readerMinTxID := tx.db.readerSlots.MinTxID()
	if readerMinTxID > 0 && readerMinTxID < minTxID {
		minTxID = readerMinTxID
	}

	tx.db.pager.ReleasePages(minTxID)
}

// freeTree frees all pages in a B+ tree (bucket) given its root page ID
func (tx *Tx) freeTree(rootID base.PageID) error {
	tx, err := tx.db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	pageIDs := make([]base.PageID, 0)

	// Stack-based post-order traversal
	type stackItem struct {
		pageID            base.PageID
		childrenProcessed bool
	}
	stack := []stackItem{{pageID: rootID, childrenProcessed: false}}

	for len(stack) > 0 {
		item := stack[len(stack)-1]

		if item.childrenProcessed {
			// Children done, add this page and pop
			pageIDs = append(pageIDs, item.pageID)
			stack = stack[:len(stack)-1]
		} else {
			node, ok := tx.db.pager.LoadNode(item.pageID)
			if !ok {
				return ErrCorruption
			}

			// Mark as processed
			stack[len(stack)-1].childrenProcessed = true

			// Push children in reverse for correct post-order
			if node.PageType() == base.BranchPageFlag {
				branch := node.(*base.BranchPage)
				children := branch.Children()
				for i := len(children) - 1; i >= 0; i-- {
					stack = append(stack, stackItem{
						pageID:            children[i],
						childrenProcessed: false,
					})
				}
			}
		}
	}

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()

	for _, pageID := range pageIDs {
		tx.db.pager.FreePage(pageID)
	}

	return nil
}

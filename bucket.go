package fredb

import (
	"bytes"
	"encoding/binary"
	"errors"

	"fredb/internal/algo"
	"fredb/internal/base"
)

// Bucket represents a namespace with its own B+tree
type Bucket struct {
	tx       *Tx
	rootID   base.PageID
	root     *base.Node
	name     []byte
	sequence uint64
	writable bool
}

// Serialize encodes bucket metadata to bytes (16 bytes)
// Format: RootPageID (8 bytes) + Sequence (8 bytes)
func (b *Bucket) Serialize() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(b.root.PageID))
	binary.LittleEndian.PutUint64(buf[8:16], b.sequence)
	return buf
}

// Deserialize decodes bucket metadata from bytes
// Returns (rootPageID, sequence)
func (b *Bucket) Deserialize(data []byte) {
	if len(data) < 16 {
		return
	}
	b.rootID = base.PageID(binary.LittleEndian.Uint64(data[0:8]))
	b.sequence = binary.LittleEndian.Uint64(data[8:16])
}

// Get retrieves the value for a key from this bucket
func (b *Bucket) Get(key []byte) []byte {
	// Check write buffer first (read-your-writes consistency)
	if b.tx.writeBuf != nil {
		compositeKey := "__root__\x00" + string(key)
		value, deleted, found := b.tx.writeBuf.Get(compositeKey)
		if found {
			if deleted {
				return nil // Tombstone - key was deleted
			}
			return value // Already a defensive copy
		}
	}

	// Fall back to tree search
	if b.root == nil {
		return nil
	}

	val, err := b.tx.search(b.root, key)
	if err != nil {
		return nil
	}

	// Important: return a copy of the value to avoid external mutation
	result := make([]byte, len(val))
	copy(result, val)
	return result
}

// Put stores a key-value pair in this bucket
func (b *Bucket) Put(key, value []byte) error {
	if !b.writable {
		return ErrTxNotWritable
	}

	// Validate key/value size
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Check practical limit based on page size
	maxSize := base.PageSize - base.PageHeaderSize - base.LeafElementSize
	if len(key)+len(value) > maxSize {
		return ErrPageOverflow
	}

	// Handle root split if needed
	if b.root.IsFull(key, value) {
		leftChild, rightChild, midKey, _, err := b.tx.splitChild(b.root, key)
		if err != nil {
			return err
		}

		newRootID, _, err := b.tx.allocatePage()
		if err != nil {
			return err
		}

		b.root = algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)
		// Add new root to tx.pages so it gets committed with real PageID
		b.tx.pages[newRootID] = b.root
	}

	// Insert with retry logic (handle cascading splits)
	// Loop until success or non-recoverable error
	for {
		newRoot, err := b.tx.insertNonFull(b.root, key, value)
		if !errors.Is(err, ErrPageOverflow) {
			// Either success or non-recoverable error
			if err == nil {
				b.root = newRoot
			}
			return err
		}

		// Root couldn't fit - split it
		leftChild, rightChild, midKey, _, err := b.tx.splitChild(b.root, key)
		if err != nil {
			return err
		}

		newRootID, _, err := b.tx.allocatePage()
		if err != nil {
			return err
		}

		b.root = algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)
		// Add new root to tx.pages so it gets committed with real PageID
		b.tx.pages[newRootID] = b.root
	}
}

// Delete removes a key from this bucket
func (b *Bucket) Delete(key []byte) error {
	if !b.writable {
		return ErrTxNotWritable
	}

	return b.tx.deleteBuffered(key)
}

// BulkLoad efficiently loads a large number of sorted key-value pairs.
// The function fn is called with a BulkLoader that provides a Set method.
// Keys must be provided in sorted order or an error will be returned.
// This is much faster than individual Put calls as it builds the tree bottom-up
// without COW overhead or tree rebalancing.
func (b *Bucket) BulkLoad(fn func(*BulkLoader) error) error {
	if !b.writable {
		return ErrTxNotWritable
	}

	loader := &BulkLoader{
		tx:        b.tx,
		bucket:    b,
		nodeCache: make(map[base.PageID]*base.Node),
	}

	// Allocate initial leaf (use real page ID since we write to disk immediately)
	leafID := b.tx.db.pager.AssignPage()
	loader.currentLeaf = &base.Node{
		PageID:  leafID,
		Dirty:   true,
		NumKeys: 0,
		Keys:    make([][]byte, 0),
		Values:  make([][]byte, 0),
	}

	// Execute user's load function
	if err := fn(loader); err != nil {
		return err
	}

	// Build internal nodes bottom-up (writes leaves and branches to disk)
	if err := loader.finalize(); err != nil {
		return err
	}

	// Add shadowRoot to tx.pages so it gets committed with metadata
	if loader.shadowRoot != nil {
		b.tx.pages[loader.shadowRoot.PageID] = loader.shadowRoot
	}

	// Free old tree pages
	if b.root != nil {
		if err := b.tx.db.freeTree(b.root.PageID); err != nil {
			return err
		}
	}

	// Atomic swap (tx-local)
	b.root = loader.shadowRoot

	return nil
}

// Cursor returns a cursor for iterating over this bucket's keys
func (b *Bucket) Cursor() *Cursor {
	// Flush any buffered writes before creating cursor
	// This ensures the cursor sees all mutations within the same transaction
	if b.tx.writable && b.tx.writeBuf.Len() > 0 {
		_ = b.tx.flushBuffer("__root__")
	}

	return &Cursor{
		tx:         b.tx,
		bucketRoot: b.root,
		valid:      false,
	}
}

// ForEach executes a function for each key-value pair in the bucket
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// ForEachPrefix iterates over all key-value pairs in the bucket that start with the given prefix
func (b *Bucket) ForEachPrefix(prefix []byte, fn func(key, value []byte) error) error {
	// Create cursor for iteration
	c := b.Cursor()

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

// Writable returns whether this bucket is writable
func (b *Bucket) Writable() bool {
	return b.writable
}

// NextSequence returns the next auto-increment value for this bucket
func (b *Bucket) NextSequence() (uint64, error) {
	if !b.writable {
		return 0, ErrTxNotWritable
	}
	b.sequence++
	return b.sequence, nil
}

// Sequence returns the current sequence value
func (b *Bucket) Sequence() uint64 {
	return b.sequence
}

// SetSequence sets the sequence value
func (b *Bucket) SetSequence(v uint64) error {
	if !b.writable {
		return ErrTxNotWritable
	}
	b.sequence = v
	return nil
}

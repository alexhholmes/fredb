package fredb

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/alexhholmes/fredb/internal/algo"
	"github.com/alexhholmes/fredb/internal/base"
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

// Get retrieves the value for a key from this bucket
func (b *Bucket) Get(key []byte) []byte {
	if b.root == nil {
		return nil
	}

	val, err := b.tx.search(b.root, key)
	if err != nil {
		return nil
	}

	return val
}

// Put stores a key-value pair in this bucket
func (b *Bucket) Put(key, value []byte) error {
	if !b.writable {
		return ErrTxNotWritable
	}

	// Validate key/value size (same as Put)
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Copy key and value to prevent aliasing bugs
	// User may reuse the provided buffers after this call returns
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Use copies for all subsequent operations
	key = keyCopy
	value = valueCopy

	// Handle root split if needed
	if b.root.IsFull(key, value) {
		leftChild, rightChild, midKey, _, err := b.tx.splitChild(b.root, key)
		if err != nil {
			return err
		}

		newRootID := b.tx.db.pager.Allocate()
		if err != nil {
			return err
		}

		b.root = algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)
		// Add new root to tx.pages immediately so clone can find it
		b.tx.pages.ReplaceOrInsert(b.root)
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

		newRootID := b.tx.db.pager.Allocate()
		if err != nil {
			return err
		}

		b.root = algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)
		// Add new root to tx.pages so it gets committed with real PageID
		b.tx.pages.ReplaceOrInsert(b.root)
	}
}

// Delete removes a key from this bucket
// Returns nil if key doesn't exist (idempotent)
func (b *Bucket) Delete(key []byte) error {
	if !b.writable {
		return ErrTxNotWritable
	}

	newRoot, err := b.tx.deleteFromNode(b.root, key)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return err
	}
	if newRoot != nil {
		b.root = newRoot
	}

	// Shrink tree: if root is internal with single child, make child the new root
	if !b.root.IsLeaf() && len(b.root.Children) == 1 {
		child, err := b.tx.loadNode(b.root.Children[0])
		if err != nil {
			return err
		}
		b.root = child
	}

	return nil
}

// Cursor returns a cursor for iterating over this bucket's keys
func (b *Bucket) Cursor() *Cursor {
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

// serialize encodes bucket metadata to bytes (16 bytes)
// Format: RootPageID (8 bytes) + Sequence (8 bytes)
func (b *Bucket) serialize() []byte {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(b.root.PageID))
	binary.LittleEndian.PutUint64(buf[8:16], b.sequence)
	return buf
}

// deserialize decodes bucket metadata from bytes
// Returns (rootPageID, sequence)
func (b *Bucket) deserialize(data []byte) {
	if len(data) < 16 {
		return
	}
	b.rootID = base.PageID(binary.LittleEndian.Uint64(data[0:8]))
	b.sequence = binary.LittleEndian.Uint64(data[8:16])
}

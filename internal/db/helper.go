package db

import (
	"encoding/binary"

	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/pager"
	"github.com/alexhholmes/fredb/internal/storage"
)

// CursorIterator is an interface for iterating over key-value pairs in a cursor.
type CursorIterator interface {
	First() (key []byte, value []byte)
	Next() (key []byte, value []byte)
}

// CopyInBatches copies key-value pairs from a cursor to a destination DB in batches.
// The putFn function is called for each batch to perform the actual Put operations.
func CopyInBatches(cursor CursorIterator, batchSize int, putFn func(keys, values [][]byte) error) error {
	keyBatch := make([][]byte, 0, batchSize)
	valBatch := make([][]byte, 0, batchSize)

	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		// Accumulate batch
		keyCopy := make([]byte, len(k))
		valCopy := make([]byte, len(v))
		copy(keyCopy, k)
		copy(valCopy, v)
		keyBatch = append(keyBatch, keyCopy)
		valBatch = append(valBatch, valCopy)

		// Commit batch when full
		if len(keyBatch) >= batchSize {
			if err := putFn(keyBatch, valBatch); err != nil {
				return err
			}
			keyBatch = keyBatch[:0]
			valBatch = valBatch[:0]
		}
	}

	// Commit remaining keys
	if len(keyBatch) > 0 {
		if err := putFn(keyBatch, valBatch); err != nil {
			return err
		}
	}

	return nil
}

// Initialize creates a new database with root tree and __root__ bucket.
func Initialize(meta base.MetaPage, pg *pager.Pager, store *storage.Storage) error {
	// NEW DATABASE - Create root tree (directory) + __root__ bucket

	// 1. Create root tree (directory for bucket metadata)
	rootPageID := pg.Allocate(1)
	rootLeafID := pg.Allocate(1)

	rootLeaf := &base.Node{
		PageID:   rootLeafID,
		Dirty:    true,
		NumKeys:  0,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: nil,
	}

	root := &base.Node{
		PageID:   rootPageID,
		Dirty:    true,
		NumKeys:  0,
		Keys:     make([][]byte, 0),
		Values:   nil,
		Children: []base.PageID{rootLeafID},
	}

	// 2. Create __root__ bucket's tree (default namespace)
	rootBucketRootID := pg.Allocate(1)
	rootBucketLeafID := pg.Allocate(1)

	rootBucketLeaf := &base.Node{
		PageID:   rootBucketLeafID,
		Dirty:    true,
		NumKeys:  0,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: nil,
	}

	rootBucketRoot := &base.Node{
		PageID:   rootBucketRootID,
		Dirty:    true,
		NumKeys:  0,
		Keys:     make([][]byte, 0),
		Values:   nil,
		Children: []base.PageID{rootBucketLeafID},
	}

	// 3. Create bucket metadata for __root__ bucket (16 bytes: RootPageID + Sequence)
	// We serialize manually since we don't have a Bucket object yet
	metadata := make([]byte, 16)
	binary.LittleEndian.PutUint64(metadata[0:8], uint64(rootBucketRootID))
	binary.LittleEndian.PutUint64(metadata[8:16], 0) // Sequence = 0

	// 4. Insert __root__ bucket metadata into root tree's leaf
	// We need to manually insert since we don't have a transaction yet
	rootLeaf.Keys = append(rootLeaf.Keys, []byte("__root__"))

	rootLeaf.Values = append(rootLeaf.Values, metadata)
	rootLeaf.NumKeys = 1

	// 5. serialize and write all 4 pages
	leafPage, _, err := rootLeaf.Serialize(0, pg.Allocate)
	if err != nil {
		_ = pg.Close()
		return err
	}
	if err = store.WritePage(rootLeaf.PageID, leafPage); err != nil {
		_ = pg.Close()
		return err
	}
	pg.TrackWrite(rootLeaf.PageID)

	rootPage, _, err := root.Serialize(0, pg.Allocate)
	if err != nil {
		_ = pg.Close()
		return err
	}
	if err = store.WritePage(root.PageID, rootPage); err != nil {
		_ = pg.Close()
		return err
	}
	pg.TrackWrite(root.PageID)

	bucketLeafPage, _, err := rootBucketLeaf.Serialize(0, pg.Allocate)
	if err != nil {
		_ = pg.Close()
		return err
	}
	if err = store.WritePage(rootBucketLeaf.PageID, bucketLeafPage); err != nil {
		_ = pg.Close()
		return err
	}
	pg.TrackWrite(rootBucketLeaf.PageID)

	bucketRootPage, _, err := rootBucketRoot.Serialize(0, pg.Allocate)
	if err != nil {
		_ = pg.Close()
		return err
	}
	if err = store.WritePage(rootBucketRoot.PageID, bucketRootPage); err != nil {
		_ = pg.Close()
		return err
	}
	pg.TrackWrite(rootBucketRoot.PageID)

	// 6. Update meta to point to root tree
	meta.RootPageID = rootPageID

	if err = pg.PutSnapshot(meta, root); err != nil {
		_ = pg.Close()
		return err
	}

	// 7. CRITICAL: Sync pg to persist the initial allocations
	// This ensures root and leaf are not returned by future Allocate() calls
	if err = store.Sync(); err != nil {
		_ = pg.Close()
		return err
	}

	// 8. Make metapage AND root visible to readers atomically
	pg.CommitSnapshot()

	return nil
}

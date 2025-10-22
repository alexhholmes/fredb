package fredb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketLoad(t *testing.T) {
	db, _ := setup(t)

	// Test loading __root__ bucket in read transaction
	err := db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")
		require.NotNil(t, bucket.root, "bucket root should not be nil")
		require.Equal(t, "__root__", string(bucket.name), "bucket name should be __root__")
		require.Equal(t, uint64(0), bucket.sequence, "bucket sequence should be 0")
		require.False(t, bucket.writable, "bucket should not be writable in read tx")
		return nil
	})
	require.NoError(t, err, "view transaction should succeed")

	// Test loading __root__ bucket in write transaction
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")
		require.True(t, bucket.writable, "bucket should be writable in write tx")
		return nil
	})
	require.NoError(t, err, "update transaction should succeed")

	// Test loading non-existent bucket
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("nonexistent"))
		require.Nil(t, bucket, "non-existent bucket should return nil")
		return nil
	})
	require.NoError(t, err, "view transaction should succeed")

	// Test bucket caching
	err = db.View(func(tx *Tx) error {
		bucket1 := tx.Bucket([]byte("__root__"))
		bucket2 := tx.Bucket([]byte("__root__"))
		require.Same(t, bucket1, bucket2, "bucket should be cached")
		return nil
	})
	require.NoError(t, err, "view transaction should succeed")
}

func TestBucketCRUD(t *testing.T) {
	db, _ := setup(t)

	// Test Set/Get
	err := db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")

		require.NoError(t, bucket.Set([]byte("key1"), []byte("value1")))
		require.NoError(t, bucket.Set([]byte("key2"), []byte("value2")))

		val1 := bucket.Get([]byte("key1"))
		require.Equal(t, "value1", string(val1), "should get correct value for key1")

		val2 := bucket.Get([]byte("key2"))
		require.Equal(t, "value2", string(val2), "should get correct value for key2")

		return nil
	})
	require.NoError(t, err, "put/get should succeed")

	// Test persistence
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")

		val1 := bucket.Get([]byte("key1"))
		require.Equal(t, "value1", string(val1), "persisted value should be correct")

		return nil
	})
	require.NoError(t, err, "persistence test should succeed")

	// Test Delete
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")

		require.NoError(t, bucket.Delete([]byte("key1")))

		val := bucket.Get([]byte("key1"))
		require.Nil(t, val, "deleted key should return nil")

		val2 := bucket.Get([]byte("key2"))
		require.Equal(t, "value2", string(val2), "key2 should still exist")

		return nil
	})
	require.NoError(t, err, "delete should succeed")

	// Test read-only restrictions
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")

		err := bucket.Set([]byte("key3"), []byte("value3"))
		require.ErrorIs(t, err, ErrTxNotWritable, "put should fail in read-only tx")

		err = bucket.Delete([]byte("key2"))
		require.ErrorIs(t, err, ErrTxNotWritable, "delete should fail in read-only tx")

		return nil
	})
	require.NoError(t, err, "read-only test should succeed")

	// Test Cursor and ForEach
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))

		require.NoError(t, bucket.Set([]byte("a"), []byte("1")))
		require.NoError(t, bucket.Set([]byte("b"), []byte("2")))
		require.NoError(t, bucket.Set([]byte("c"), []byte("3")))

		// Test Cursor
		c := bucket.Cursor()
		count := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		require.Equal(t, 4, count, "cursor should iterate over 4 keys (key2, a, b, c)")

		// Test ForEach
		count = 0
		err := bucket.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, 4, count, "foreach should iterate over 4 keys")

		return nil
	})
	require.NoError(t, err, "cursor/foreach test should succeed")

	// Test Sequence
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))

		seq1, err := bucket.NextSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(1), seq1, "first sequence should be 1")

		seq2, err := bucket.NextSequence()
		require.NoError(t, err)
		require.Equal(t, uint64(2), seq2, "second sequence should be 2")

		return nil
	})
	require.NoError(t, err, "sequence test should succeed")
}

func TestBucketCreateDelete(t *testing.T) {
	db, _ := setup(t)

	// Create bucket
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("test"))
		require.NoError(t, err)
		require.NotNil(t, bucket)
		require.Equal(t, "test", string(bucket.name))

		// Add data to bucket
		require.NoError(t, bucket.Set([]byte("key1"), []byte("value1")))
		return nil
	})
	require.NoError(t, err)

	// Verify bucket persists
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("test"))
		require.NotNil(t, bucket)

		val := bucket.Get([]byte("key1"))
		require.Equal(t, "value1", string(val))
		return nil
	})
	require.NoError(t, err)

	// Delete bucket
	err = db.Update(func(tx *Tx) error {
		err := tx.DeleteBucket([]byte("test"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Verify bucket is gone
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("test"))
		require.Nil(t, bucket, "deleted bucket should return nil")
		return nil
	})
	require.NoError(t, err)
}

func TestBucketDeleteSameTransaction(t *testing.T) {
	db, _ := setup(t)

	// Create bucket
	err := db.Update(func(tx *Tx) error {
		_, err := tx.CreateBucket([]byte("test"))
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	// Delete and try to access in same transaction
	err = db.Update(func(tx *Tx) error {
		// Delete bucket
		err := tx.DeleteBucket([]byte("test"))
		require.NoError(t, err)

		// Try to access deleted bucket - should return nil
		bucket := tx.Bucket([]byte("test"))
		require.Nil(t, bucket, "deleted bucket should return nil in same tx")

		// Try to recreate deleted bucket - should fail
		bucket2, err := tx.CreateBucket([]byte("test"))
		require.Error(t, err, "cannot recreate bucket deleted in same transaction")
		require.Nil(t, bucket2)

		return nil
	})
	require.NoError(t, err)
}

func TestBucketDeleteWithConcurrentReaders(t *testing.T) {
	db, _ := setup(t)

	// Create bucket with data
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, bucket.Set([]byte("key1"), []byte("value1")))
		require.NoError(t, bucket.Set([]byte("key2"), []byte("value2")))
		return nil
	})
	require.NoError(t, err)

	// Start read transaction before deletion
	tx1, err := db.Begin(false)
	require.NoError(t, err)
	defer tx1.Rollback()

	bucket1 := tx1.Bucket([]byte("test"))
	require.NotNil(t, bucket1, "bucket should exist for reader started before deletion")

	// Delete bucket in write transaction
	err = db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("test"))
	})
	require.NoError(t, err)

	// Reader started before deletion can still read
	val1 := bucket1.Get([]byte("key1"))
	require.Equal(t, "value1", string(val1), "reader should still see data")

	val2 := bucket1.Get([]byte("key2"))
	require.Equal(t, "value2", string(val2), "reader should still see data")

	// New readers after deletion should not see bucket
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("test"))
		require.Nil(t, bucket, "new readers should not see deleted bucket")
		return nil
	})
	require.NoError(t, err)

	// Close reader - bucket pages should be freed after this
	require.NoError(t, tx1.Rollback())
}

func TestBucketDeleteRollback(t *testing.T) {
	db, _ := setup(t)

	// Create bucket
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, bucket.Set([]byte("key1"), []byte("value1")))
		return nil
	})
	require.NoError(t, err)

	// Delete bucket but rollback
	tx, err := db.Begin(true)
	require.NoError(t, err)
	err = tx.DeleteBucket([]byte("test"))
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	// Bucket should still exist after rollback
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("test"))
		require.NotNil(t, bucket, "bucket should exist after rollback")

		val := bucket.Get([]byte("key1"))
		require.Equal(t, "value1", string(val), "data should still exist")
		return nil
	})
	require.NoError(t, err)
}

func TestBucketDeleteNonExistent(t *testing.T) {
	db, _ := setup(t)

	// Try to delete non-existent bucket
	err := db.Update(func(tx *Tx) error {
		err := tx.DeleteBucket([]byte("nonexistent"))
		require.Error(t, err, "deleting non-existent bucket should fail")
		return nil
	})
	require.NoError(t, err)
}

func TestBucketReferenceCountingMultipleReaders(t *testing.T) {
	db, _ := setup(t)

	// Create bucket
	err := db.Update(func(tx *Tx) error {
		bucket, err := tx.CreateBucket([]byte("test"))
		require.NoError(t, err)
		require.NoError(t, bucket.Set([]byte("key1"), []byte("value1")))
		return nil
	})
	require.NoError(t, err)

	// Start multiple readers
	tx1, err := db.Begin(false)
	require.NoError(t, err)
	defer tx1.Rollback()

	tx2, err := db.Begin(false)
	require.NoError(t, err)
	defer tx2.Rollback()

	tx3, err := db.Begin(false)
	require.NoError(t, err)
	defer tx3.Rollback()

	// All readers acquire bucket
	bucket1 := tx1.Bucket([]byte("test"))
	require.NotNil(t, bucket1)

	bucket2 := tx2.Bucket([]byte("test"))
	require.NotNil(t, bucket2)

	bucket3 := tx3.Bucket([]byte("test"))
	require.NotNil(t, bucket3)

	// Delete bucket
	err = db.Update(func(tx *Tx) error {
		return tx.DeleteBucket([]byte("test"))
	})
	require.NoError(t, err)

	// All readers should still be able to read
	val1 := bucket1.Get([]byte("key1"))
	require.Equal(t, "value1", string(val1))

	val2 := bucket2.Get([]byte("key1"))
	require.Equal(t, "value1", string(val2))

	val3 := bucket3.Get([]byte("key1"))
	require.Equal(t, "value1", string(val3))

	// New readers should not see bucket
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("test"))
		require.Nil(t, bucket)
		return nil
	})
	require.NoError(t, err)

	// Close readers one by one
	require.NoError(t, tx1.Rollback())
	require.NoError(t, tx2.Rollback())
	require.NoError(t, tx3.Rollback())
}

package fredb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBucketLoad(t *testing.T) {
	db, err := Open("test_bucket_load.db")
	require.NoError(t, err, "should open database")
	defer func() {
		db.Close()
		os.Remove("test_bucket_load.db")
	}()

	// Test loading __root__ bucket in read transaction
	err = db.View(func(tx *Tx) error {
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
	db, err := Open("test_bucket_crud.db")
	require.NoError(t, err, "should open database")
	defer func() {
		db.Close()
		os.Remove("test_bucket_crud.db")
	}()

	// Test Put/Get
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket, "root bucket should exist")

		require.NoError(t, bucket.Put([]byte("key1"), []byte("value1")))
		require.NoError(t, bucket.Put([]byte("key2"), []byte("value2")))

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

		err := bucket.Put([]byte("key3"), []byte("value3"))
		require.ErrorIs(t, err, ErrTxNotWritable, "put should fail in read-only tx")

		err = bucket.Delete([]byte("key2"))
		require.ErrorIs(t, err, ErrTxNotWritable, "delete should fail in read-only tx")

		return nil
	})
	require.NoError(t, err, "read-only test should succeed")

	// Test Cursor and ForEach
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))

		require.NoError(t, bucket.Put([]byte("a"), []byte("1")))
		require.NoError(t, bucket.Put([]byte("b"), []byte("2")))
		require.NoError(t, bucket.Put([]byte("c"), []byte("3")))

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

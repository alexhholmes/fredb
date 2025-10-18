package fredb

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBulkLoad_BasicCorrectness(t *testing.T) {
	tmpfile := fmt.Sprintf("/tmp/test_bulkload_basic_%s.db", t.Name())
	defer os.RemoveAll(tmpfile)
	defer os.RemoveAll(tmpfile + ".wal")

	db, err := Open(tmpfile, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// BulkLoad 10,000 sorted key-value pairs
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		return bucket.BulkLoad(func(l *BulkLoader) error {
			for i := 0; i < 10000; i++ {
				key := []byte(fmt.Sprintf("key%08d", i))
				value := []byte(fmt.Sprintf("value%08d", i))
				if err := l.Set(key, value); err != nil {
					return err
				}
			}
			return nil
		})
	})
	require.NoError(t, err)

	// Verify all keys are retrievable with correct values
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		for i := 0; i < 10000; i++ {
			key := []byte(fmt.Sprintf("key%08d", i))
			expectedValue := []byte(fmt.Sprintf("value%08d", i))

			value := bucket.Get(key)
			require.NotNil(t, value, "key %s should exist", key)
			require.Equal(t, expectedValue, value, "value mismatch for key %s", key)
		}

		return nil
	})
	require.NoError(t, err)

	// Verify iteration with Cursor returns all keys in order
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		c := bucket.Cursor()
		count := 0
		var lastKey []byte

		for k, v := c.First(); k != nil; k, v = c.Next() {
			// Verify order
			if lastKey != nil {
				require.True(t, bytes.Compare(k, lastKey) > 0, "keys should be in sorted order")
			}

			// Verify value matches
			expectedValue := []byte(fmt.Sprintf("value%08d", count))
			require.Equal(t, expectedValue, v, "value mismatch during iteration")

			lastKey = k
			count++
		}

		require.Equal(t, 10000, count, "should iterate over exactly 10000 keys")
		return nil
	})
	require.NoError(t, err)
}

func TestBulkLoad_PerformanceVsPut(t *testing.T) {
	const numKeys = 100000

	// Test Put operations
	tmpfilePut := fmt.Sprintf("/tmp/test_bulkload_put_%s.db", t.Name())
	defer os.RemoveAll(tmpfilePut)
	defer os.RemoveAll(tmpfilePut + ".wal")

	dbPut, err := Open(tmpfilePut, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer dbPut.Close()

	putStart := time.Now()
	err = dbPut.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		for i := 0; i < numKeys; i++ {
			key := []byte(fmt.Sprintf("key%08d", i))
			value := []byte(fmt.Sprintf("value%08d", i))
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	putDuration := time.Since(putStart)

	// Test BulkLoad operations
	tmpfileBulk := fmt.Sprintf("/tmp/test_bulkload_bulk_%s.db", t.Name())
	defer os.RemoveAll(tmpfileBulk)
	defer os.RemoveAll(tmpfileBulk + ".wal")

	dbBulk, err := Open(tmpfileBulk, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer dbBulk.Close()

	bulkStart := time.Now()
	err = dbBulk.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		return bucket.BulkLoad(func(l *BulkLoader) error {
			for i := 0; i < numKeys; i++ {
				key := []byte(fmt.Sprintf("key%08d", i))
				value := []byte(fmt.Sprintf("value%08d", i))
				if err := l.Set(key, value); err != nil {
					return err
				}
			}
			return nil
		})
	})
	require.NoError(t, err)
	bulkDuration := time.Since(bulkStart)

	// Print durations for visibility
	t.Logf("Put duration: %v", putDuration)
	t.Logf("BulkLoad duration: %v", bulkDuration)
	t.Logf("Speedup: %.2fx", float64(putDuration)/float64(bulkDuration))

	// Assert bulkDuration < putDuration
	require.Less(t, bulkDuration, putDuration, "BulkLoad should be faster than individual Puts")
}

func TestBulkLoad_UnsortedKeysError(t *testing.T) {
	tmpfile := fmt.Sprintf("/tmp/test_bulkload_unsorted_%s.db", t.Name())
	defer os.RemoveAll(tmpfile)
	defer os.RemoveAll(tmpfile + ".wal")

	db, err := Open(tmpfile, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// BulkLoad with intentionally unsorted keys
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		return bucket.BulkLoad(func(l *BulkLoader) error {
			// Insert in wrong order
			if err := l.Set([]byte("key3"), []byte("value3")); err != nil {
				return err
			}
			if err := l.Set([]byte("key1"), []byte("value1")); err != nil {
				return err
			}
			return nil
		})
	})

	// Assert error contains "ascending"
	require.Error(t, err)
	require.Contains(t, err.Error(), "ascending", "error should mention sorted order requirement")
}

func TestBulkLoad_EmptyLoad(t *testing.T) {
	tmpfile := fmt.Sprintf("/tmp/test_bulkload_empty_%s.db", t.Name())
	defer os.RemoveAll(tmpfile)
	defer os.RemoveAll(tmpfile + ".wal")

	db, err := Open(tmpfile, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// First, put some data
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)
		return bucket.Put([]byte("test"), []byte("value"))
	})
	require.NoError(t, err)

	// Call BulkLoad with no Set operations
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		return bucket.BulkLoad(func(l *BulkLoader) error {
			// Do nothing - don't call Set at all
			return nil
		})
	})

	// BulkLoad creates an empty tree even when no keys are Set
	// This is because an initial leaf is allocated before calling the user function
	// The result is a valid but empty tree
	require.NoError(t, err, "BulkLoad with no Set calls succeeds but creates empty tree")

	// Verify the tree is now empty (old data replaced with empty tree)
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		value := bucket.Get([]byte("test"))
		require.Nil(t, value, "tree should be empty after empty BulkLoad")

		c := bucket.Cursor()
		k, _ := c.First()
		require.Nil(t, k, "cursor should find no keys in empty tree")

		return nil
	})
	require.NoError(t, err)
}

func TestBulkLoad_SingleKey(t *testing.T) {
	tmpfile := fmt.Sprintf("/tmp/test_bulkload_single_%s.db", t.Name())
	defer os.RemoveAll(tmpfile)
	defer os.RemoveAll(tmpfile + ".wal")

	db, err := Open(tmpfile, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// BulkLoad exactly one key
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		return bucket.BulkLoad(func(l *BulkLoader) error {
			return l.Set([]byte("singlekey"), []byte("singlevalue"))
		})
	})
	require.NoError(t, err)

	// Verify it's retrievable
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		value := bucket.Get([]byte("singlekey"))
		require.NotNil(t, value)
		require.Equal(t, []byte("singlevalue"), value)

		return nil
	})
	require.NoError(t, err)
}

func TestBulkLoad_ReplacesExistingData(t *testing.T) {
	tmpfile := fmt.Sprintf("/tmp/test_bulkload_replace_%s.db", t.Name())
	defer os.RemoveAll(tmpfile)
	defer os.RemoveAll(tmpfile + ".wal")

	db, err := Open(tmpfile, WithMaxCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// Put 1,000 keys normally
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("old%08d", i))
			value := []byte(fmt.Sprintf("oldvalue%08d", i))
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// BulkLoad 2,000 different keys
	err = db.Update(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		return bucket.BulkLoad(func(l *BulkLoader) error {
			for i := 0; i < 2000; i++ {
				key := []byte(fmt.Sprintf("new%08d", i))
				value := []byte(fmt.Sprintf("newvalue%08d", i))
				if err := l.Set(key, value); err != nil {
					return err
				}
			}
			return nil
		})
	})
	require.NoError(t, err)

	// Verify only the 2,000 new keys exist (old ones gone)
	err = db.View(func(tx *Tx) error {
		bucket := tx.Bucket([]byte("__root__"))
		require.NotNil(t, bucket)

		// Old keys should be gone
		for i := 0; i < 1000; i++ {
			key := []byte(fmt.Sprintf("old%08d", i))
			value := bucket.Get(key)
			require.Nil(t, value, "old key %s should not exist after BulkLoad", key)
		}

		// New keys should exist
		for i := 0; i < 2000; i++ {
			key := []byte(fmt.Sprintf("new%08d", i))
			expectedValue := []byte(fmt.Sprintf("newvalue%08d", i))
			value := bucket.Get(key)
			require.NotNil(t, value, "new key %s should exist", key)
			require.Equal(t, expectedValue, value, "value mismatch for key %s", key)
		}

		// Verify exact count via cursor
		c := bucket.Cursor()
		count := 0
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			count++
		}
		require.Equal(t, 2000, count, "should have exactly 2000 keys")

		return nil
	})
	require.NoError(t, err)
}

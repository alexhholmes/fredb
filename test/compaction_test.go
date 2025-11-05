package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb"
)

func TestCompactBasicCopy(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_compact_basic_%s.db", t.Name())
	compactFile := tmpfile + ".compact"
	os.Remove(tmpfile)
	os.Remove(compactFile)
	defer os.Remove(tmpfile)
	defer os.Remove(compactFile)

	// Create database with test data
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	// Insert 1000 keys
	expectedData := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := []byte(fmt.Sprintf("value-%06d", i))
		expectedData[string(key)] = string(value)
		err := db.Put(key, value)
		require.NoError(t, err, "Failed to insert key %d", i)
	}

	// Compact to new file
	compactedDB, err := db.Compact(compactFile)
	require.NoError(t, err, "Compaction failed")
	require.NotNil(t, compactedDB, "Compacted DB should not be nil")

	// Verify all data exists in compacted DB
	for key, expectedValue := range expectedData {
		value, err := compactedDB.Get([]byte(key))
		require.NoError(t, err, "Failed to get key %s from compacted DB", key)
		assert.Equal(t, expectedValue, string(value), "Value mismatch for key %s", key)
	}

	// Verify original DB still works
	value, err := db.Get([]byte("key-000500"))
	require.NoError(t, err)
	assert.Equal(t, "value-000500", string(value))

	// Close both databases
	require.NoError(t, compactedDB.Close())
	require.NoError(t, db.Close())
}

func TestCompactWithFragmentation(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_compact_frag_%s.db", t.Name())
	compactFile := tmpfile + ".compact"
	os.Remove(tmpfile)
	os.Remove(compactFile)
	defer os.Remove(tmpfile)
	defer os.Remove(compactFile)

	// Create database
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	// Insert 2000 keys
	for i := 0; i < 2000; i++ {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := []byte(fmt.Sprintf("value-%06d", i))
		err := db.Put(key, value)
		require.NoError(t, err)
	}

	// Delete every other key to create fragmentation
	deletedKeys := make(map[string]bool)
	for i := 0; i < 2000; i += 2 {
		key := fmt.Sprintf("key-%06d", i)
		err := db.Delete([]byte(key))
		require.NoError(t, err)
		deletedKeys[key] = true
	}

	// Close to ensure file size is stable
	err = db.Close()
	require.NoError(t, err)

	// Get original file size
	origInfo, err := os.Stat(tmpfile)
	require.NoError(t, err)
	origSize := origInfo.Size()

	// Refredb.Open and compact
	db, err = fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	compactedDB, err := db.Compact(compactFile)
	require.NoError(t, err)

	// Close compacted DB to ensure it's written
	err = compactedDB.Close()
	require.NoError(t, err)

	// Get compacted file size
	compactInfo, err := os.Stat(compactFile)
	require.NoError(t, err)
	compactSize := compactInfo.Size()

	// Verify compaction reduced file size
	t.Logf("Original size: %d bytes, Compacted size: %d bytes, Reduction: %.1f%%",
		origSize, compactSize, 100.0*(1.0-float64(compactSize)/float64(origSize)))
	assert.Less(t, compactSize, origSize, "Compacted file should be smaller than original")

	// Refredb.Open compacted DB and verify remaining data
	compactedDB, err = fredb.Open(compactFile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)
	defer compactedDB.Close()

	// Verify deleted keys are gone
	for key := range deletedKeys {
		value, err := compactedDB.Get([]byte(key))
		require.NoError(t, err)
		assert.Nil(t, value, "Deleted key %s should not exist", key)
	}

	// Verify remaining keys exist
	for i := 1; i < 2000; i += 2 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		expectedValue := []byte(fmt.Sprintf("value-%06d", i))
		value, err := compactedDB.Get(key)
		require.NoError(t, err, "Failed to get key-%06d", i)
		assert.Equal(t, expectedValue, value, "Value mismatch for key-%06d", i)
	}

	require.NoError(t, db.Close())
}

func TestCompactWithBuckets(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_compact_buckets_%s.db", t.Name())
	compactFile := tmpfile + ".compact"
	os.Remove(tmpfile)
	os.Remove(compactFile)
	defer os.Remove(tmpfile)
	defer os.Remove(compactFile)

	// Create database
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	// Create buckets and insert data
	err = db.Update(func(tx *fredb.Tx) error {
		// Create bucket1
		b1, err := tx.CreateBucket([]byte("bucket1"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("b1-key-%03d", i))
			value := []byte(fmt.Sprintf("b1-value-%03d", i))
			if err := b1.Put(key, value); err != nil {
				return err
			}
		}

		// Create bucket2
		b2, err := tx.CreateBucket([]byte("bucket2"))
		if err != nil {
			return err
		}
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("b2-key-%03d", i))
			value := []byte(fmt.Sprintf("b2-value-%03d", i))
			if err := b2.Put(key, value); err != nil {
				return err
			}
		}

		// Also insert into __root__
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("root-key-%03d", i))
			value := []byte(fmt.Sprintf("root-value-%03d", i))
			if err := tx.Put(key, value); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	// Compact
	compactedDB, err := db.Compact(compactFile)
	require.NoError(t, err)
	require.NotNil(t, compactedDB)

	// Verify bucket data in compacted DB
	err = compactedDB.View(func(tx *fredb.Tx) error {
		// Check bucket1
		b1 := tx.Bucket([]byte("bucket1"))
		require.NotNil(t, b1, "bucket1 should exist")
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("b1-key-%03d", i))
			expectedValue := []byte(fmt.Sprintf("b1-value-%03d", i))
			value := b1.Get(key)
			assert.Equal(t, expectedValue, value)
		}

		// Check bucket2
		b2 := tx.Bucket([]byte("bucket2"))
		require.NotNil(t, b2, "bucket2 should exist")
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("b2-key-%03d", i))
			expectedValue := []byte(fmt.Sprintf("b2-value-%03d", i))
			value := b2.Get(key)
			assert.Equal(t, expectedValue, value)
		}

		// Check __root__ data
		for i := 0; i < 50; i++ {
			key := []byte(fmt.Sprintf("root-key-%03d", i))
			expectedValue := []byte(fmt.Sprintf("root-value-%03d", i))
			value, err := tx.Get(key)
			require.NoError(t, err)
			assert.Equal(t, expectedValue, value)
		}

		return nil
	})
	require.NoError(t, err)

	require.NoError(t, compactedDB.Close())
	require.NoError(t, db.Close())
}

func TestCompactEmptyDatabase(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_compact_empty_%s.db", t.Name())
	compactFile := tmpfile + ".compact"
	os.Remove(tmpfile)
	os.Remove(compactFile)
	defer os.Remove(tmpfile)
	defer os.Remove(compactFile)

	// Create empty database
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	// Compact empty database
	compactedDB, err := db.Compact(compactFile)
	require.NoError(t, err)
	require.NotNil(t, compactedDB)

	// Verify compacted DB is functional
	err = compactedDB.Put([]byte("test"), []byte("value"))
	require.NoError(t, err)

	value, err := compactedDB.Get([]byte("test"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)

	require.NoError(t, compactedDB.Close())
	require.NoError(t, db.Close())
}

func TestCompactLargeValues(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_compact_large_%s.db", t.Name())
	compactFile := tmpfile + ".compact"
	os.Remove(tmpfile)
	os.Remove(compactFile)
	defer os.Remove(tmpfile)
	defer os.Remove(compactFile)

	// Create database with large values
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	// Insert keys with large values (over 3KB to trigger overflow)
	largeValues := make(map[string][]byte)
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("large-key-%03d", i)
		value := make([]byte, 10*1024) // 10KB
		for j := range value {
			value[j] = byte((i + j) % 256)
		}
		largeValues[key] = value
		err := db.Put([]byte(key), value)
		require.NoError(t, err)
	}

	// Compact
	compactedDB, err := db.Compact(compactFile)
	require.NoError(t, err)

	// Verify large values preserved
	for key, expectedValue := range largeValues {
		value, err := compactedDB.Get([]byte(key))
		require.NoError(t, err, "Failed to get %s", key)
		assert.Equal(t, expectedValue, value, "Value mismatch for %s", key)
	}

	require.NoError(t, compactedDB.Close())
	require.NoError(t, db.Close())
}

func TestCompactDestinationExists(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_compact_exists_%s.db", t.Name())
	compactFile := tmpfile + ".compact"
	os.Remove(tmpfile)
	os.Remove(compactFile)
	defer os.Remove(tmpfile)
	defer os.Remove(compactFile)

	// Create source database
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	err = db.Put([]byte("key"), []byte("value"))
	require.NoError(t, err)

	// Create destination database (already exists)
	existingDB, err := fredb.Open(compactFile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)
	err = existingDB.Close()
	require.NoError(t, err)

	// Try to compact - should fail
	compactedDB, err := db.Compact(compactFile)
	assert.Error(t, err, "Compacting to existing DB should fail")
	assert.Nil(t, compactedDB, "Should not return DB on error")
	assert.ErrorIs(t, err, fredb.ErrDatabaseExists)
}

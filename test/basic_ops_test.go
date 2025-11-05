package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb"
)

func TestDBBasicOperations(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// Test Put and get
	key := []byte("test-key")
	value := []byte("test-value")

	err := db.Put(key, value)
	assert.NoError(t, err, "Failed to Put key")

	retrieved, err := db.Get(key)
	assert.NoError(t, err, "Failed to get key")
	assert.Equal(t, value, retrieved)

	// Test Update
	newValue := []byte("updated-value")
	err = db.Put(key, newValue)
	assert.NoError(t, err, "Failed to update key")

	retrieved, err = db.Get(key)
	assert.NoError(t, err, "Failed to get updated key")
	assert.Equal(t, newValue, retrieved)

	// Test Delete
	err = db.Delete(key)
	assert.NoError(t, err, "Failed to delete key")

	// Verify key is deleted
	val, err := db.Get(key)
	assert.NoError(t, err)
	assert.Nil(t, val)
}

func TestDBErrors(t *testing.T) {
	t.Parallel()

	db, _ := setup(t)

	// get non-existent key
	val, err := db.Get([]byte("non-existent"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Delete non-existent key (idempotent - should not error)
	err = db.Delete([]byte("non-existent"))
	assert.NoError(t, err)
}

func TestDBClose(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_close.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// Put a key before closing
	err = db.Put([]byte("key"), []byte("value"))
	assert.NoError(t, err, "Failed to Put key")

	// close the DB
	err = db.Close()
	assert.NoError(t, err, "Failed to close DB")

	// Multiple close calls should not panic
	err = db.Close()
	if err != nil {
		t.Logf("Second close returned error (expected): %v", err)
	}
}

func TestBTreeBasicOps(t *testing.T) {
	t.Parallel()

	// Test basic get/Put operations
	db, _ := setup(t)

	// Insert key-value pair
	err := db.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// get existing key
	val, err := db.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value1", string(val))

	// Update existing key
	err = db.Put([]byte("key1"), []byte("value2"))
	assert.NoError(t, err)

	val, err = db.Get([]byte("key1"))
	assert.NoError(t, err)
	assert.Equal(t, "value2", string(val))

	// get non-existent key (should return nil value)
	val, err = db.Get([]byte("nonexistent"))
	assert.Nil(t, err)
}

func TestBTreeUpdate(t *testing.T) {
	t.Parallel()

	// Test that Put updates existing Keys rather than duplicating
	db, _ := setup(t)

	// Insert key with value1
	err := db.Put([]byte("testkey"), []byte("value1"))
	assert.NoError(t, err)

	// Update same key with value2
	err = db.Put([]byte("testkey"), []byte("value2"))
	assert.NoError(t, err)

	// Verify get returns value2
	val, err := db.Get([]byte("testkey"))
	assert.NoError(t, err)
	assert.Equal(t, "value2", string(val))

	// Insert more Keys to ensure no duplication
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		val := []byte(fmt.Sprintf("value%d", i))
		db.Put(key, val)
	}

	// Update the original key again
	err = db.Put([]byte("testkey"), []byte("value3"))
	assert.NoError(t, err)

	val, err = db.Get([]byte("testkey"))
	assert.NoError(t, err)
	assert.Equal(t, "value3", string(val))
}

func TestBTreeDelete(t *testing.T) {
	t.Parallel()

	// Test basic delete operations
	db, _ := setup(t)

	// Insert some Keys
	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, k := range keys {
		err := db.Put([]byte(k), []byte("value-"+k))
		assert.NoError(t, err)
	}

	// Delete middle key
	err := db.Delete([]byte("key3"))
	assert.NoError(t, err)

	// Verify key3 is gone
	val, err := db.Get([]byte("key3"))
	assert.NoError(t, err)
	assert.Nil(t, val)

	// Verify other Keys still exist
	for _, k := range []string{"key1", "key2", "key4", "key5"} {
		val, err := db.Get([]byte(k))
		if assert.NoError(t, err) {
			assert.Equal(t, "value-"+k, string(val))
		}
	}

	// Delete first key
	err = db.Delete([]byte("key1"))
	assert.NoError(t, err)

	// Delete last key
	err = db.Delete([]byte("key5"))
	assert.NoError(t, err)

	// Delete non-existent key (idempotent - should not error)
	err = db.Delete([]byte("nonexistent"))
	assert.NoError(t, err)

	// Verify remaining Keys
	for _, k := range []string{"key2", "key4"} {
		val, err := db.Get([]byte(k))
		if assert.NoError(t, err) {
			assert.Equal(t, "value-"+k, string(val))
		}
	}
}

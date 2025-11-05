package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb"
	"github.com/alexhholmes/fredb/internal/base"
)

func TestDiskPageManagerBasic(t *testing.T) {
	t.Parallel()

	// Create temp file
	tmpfile := "/tmp/test_disk.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create new database
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// Insert some data
	require.NoError(t, db.Put([]byte("key1"), []byte("value1")), "Failed to Put key1")
	require.NoError(t, db.Put([]byte("key2"), []byte("value2")), "Failed to Put key2")

	// Verify data
	val, err := db.Get([]byte("key1"))
	require.NoError(t, err, "Failed to get key1")
	assert.Equal(t, "value1", string(val))

	// close (flushes to disk)
	require.NoError(t, db.Close(), "Failed to close DB")

	// Reopen database
	db2, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB")

	// Verify data persisted
	val, err = db2.Get([]byte("key1"))
	require.NoError(t, err, "Failed to get key1 after reopen")
	assert.Equal(t, "value1", string(val))

	val, err = db2.Get([]byte("key2"))
	require.NoError(t, err, "Failed to get key2 after reopen")
	assert.Equal(t, "value2", string(val))

	// Cleanup
	require.NoError(t, db2.Close(), "Failed to close db2")
}

func TestDiskPageManagerPersistence(t *testing.T) {
	t.Parallel()

	// Use unique filename per test to avoid parallel test collisions
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.DB", t.Name())
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create database and insert 100 Keys
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		require.NoError(t, db.Put(key, value), "Failed to Put key %d", i)
	}

	require.NoError(t, db.Close(), "Failed to close DB")

	// Reopen and verify all Keys
	db2, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to reopen DB")

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i * 2)}

		value, err := db2.Get(key)
		require.NoError(t, err, "Failed to get key %d after reopen", i)
		assert.Equal(t, expectedValue, value, "Key %d: value mismatch", i)
	}

	require.NoError(t, db2.Close(), "Failed to close db2")
}

func TestDiskPageManagerDelete(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_delete.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create database
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	// Insert Keys
	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("2"))
	db.Put([]byte("c"), []byte("3"))

	// Delete one
	require.NoError(t, db.Delete([]byte("b")), "Failed to delete")

	// close
	db.Close()

	// Reopen and verify
	db2, _ := fredb.Open(tmpfile)

	// Should still have a and c
	_, err = db2.Get([]byte("a"))
	assert.NoError(t, err, "Key 'a' should exist")
	_, err = db2.Get([]byte("c"))
	assert.NoError(t, err, "Key 'c' should exist")

	// b should be gone
	val, err := db2.Get([]byte("b"))
	assert.Nil(t, val, "Key 'b' should return nil value")
	assert.Nil(t, err, "Key 'b' should return nil error")
	assert.NoError(t, db2.Close(), "Failed to close DB")

	db2.Close()
}

// TestDBFileFormat validates the on-disk format
func TestDBFileFormat(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_format.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB and write some data
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// validate file exists and has correct structure
	info, err := os.Stat(tmpfile)
	require.NoError(t, err, "DB file not found")

	// File should be at least 3 pages (reserved 0, meta 1-2)
	minSize := int64(base.PageSize * 3)
	assert.GreaterOrEqual(t, info.Size(), minSize, "File too small")

	// File size should be Page-aligned
	assert.Equal(t, int64(0), info.Size()%int64(base.PageSize), "File size not Page-aligned")

	// Read both meta pages
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")
	defer file.Close()

	page0 := &base.Page{}
	n, err := file.Read(page0.Data[:])
	require.NoError(t, err, "Failed to read meta Page 0")
	assert.Equal(t, base.PageSize, n, "Short read")

	page1 := &base.Page{}
	n, err = file.Read(page1.Data[:])
	require.NoError(t, err, "Failed to read meta Page 1")
	assert.Equal(t, base.PageSize, n, "Short read")

	page2 := &base.Page{}
	n, err = file.Read(page2.Data[:])
	require.NoError(t, err, "Failed to read meta Page 2")
	assert.Equal(t, base.PageSize, n, "Short read")

	meta1 := page1.DeserializeMeta()
	meta2 := page2.DeserializeMeta()

	// close() increments TxID from 0 to 1, so writes to Page 1 (1 % 2 = 1)
	// Page 1 should have the latest meta with RootPageID Put
	t.Logf("Meta Page 1 TxID: %d, RootPageID: %d", meta1.TxID, meta1.RootPageID)
	t.Logf("Meta Page 2 TxID: %d, RootPageID: %d", meta2.TxID, meta2.RootPageID)

	// Pick the Page with highest TxID (should be Page 1)
	var meta *base.MetaPage
	if meta1.TxID > meta2.TxID {
		meta = meta1
	} else {
		meta = meta2
	}

	// validate magic number
	assert.Equal(t, base.MagicNumber, meta.Magic, "Invalid magic number")

	// validate version
	assert.Equal(t, base.FormatVersion, meta.Version, "Invalid version")

	// validate Page size
	assert.Equal(t, uint16(base.PageSize), meta.PageSize, "Invalid Page size")

	// validate RootPageID is persisted after close
	assert.NotEqual(t, base.PageID(0), meta.RootPageID, "RootPageID is zero after close - should be persisted to meta")

	// validate checksum
	assert.NoError(t, meta.Validate(), "Meta validation failed")

	t.Logf("Meta Page validated successfully:")
	t.Logf("  Magic: 0x%08x", meta.Magic)
	t.Logf("  Version: %d", meta.Version)
	t.Logf("  storage.PageSize: %d", meta.PageSize)
	t.Logf("  RootPageID: %d", meta.RootPageID)
	t.Logf("  FreelistID: %d", meta.FreelistID)
	t.Logf("  FreelistPages: %d", meta.FreelistPages)
	t.Logf("  TxID: %d", meta.TxID)
	t.Logf("  NumPages: %d", meta.NumPages)
	t.Logf("  File size: %d bytes (%d pages)", info.Size(),
		info.Size()/int64(base.PageSize))
}

// TestDBFileHexDump creates a DB file and prints hex dump for manual inspection
func TestDBFileHexDump(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("Skipping hex dump test in short mode")
	}

	tmpfile := "/tmp/test_db_hexdump.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create DB with known data
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("testkey"), []byte("testvalue"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Read first 4 pages and dump them
	file, err := os.Open(tmpfile)
	require.NoError(t, err, "Failed to open file")
	defer file.Close()

	// Read meta Page 0
	page0 := make([]byte, base.PageSize)
	_, err = file.Read(page0)
	require.NoError(t, err, "Failed to read Page 0")

	t.Logf("\n=== Page 0 (Meta) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page0[:128]))

	// Read meta Page 1
	page1 := make([]byte, base.PageSize)
	_, err = file.Read(page1)
	require.NoError(t, err, "Failed to read Page 1")

	t.Logf("\n=== Page 1 (Meta backup) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page1[:128]))

	// Read freelist Page 2
	page2 := make([]byte, base.PageSize)
	_, err = file.Read(page2)
	require.NoError(t, err, "Failed to read Page 2")

	t.Logf("\n=== Page 2 (Freelist) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page2[:128]))

	// Read root Page (Page 3 likely)
	page3 := make([]byte, base.PageSize)
	_, err = file.Read(page3)
	require.NoError(t, err, "Failed to read Page 3")

	t.Logf("\n=== Page 3 (B-tree root) - First 256 bytes ===")
	t.Logf("%s", formatHexDump(page3[:256]))
}

func formatHexDump(data []byte) string {
	result := ""
	for i := 0; i < len(data); i += 16 {
		end := i + 16
		if end > len(data) {
			end = len(data)
		}

		// OffPut
		result += fmt.Sprintf("%08x: ", i)

		// Hex bytes
		for j := i; j < end; j++ {
			result += fmt.Sprintf("%02x ", data[j])
		}

		// Padding
		for j := end; j < i+16; j++ {
			result += "   "
		}

		// ASCII
		result += " |"
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] <= 126 {
				result += string(data[j])
			} else {
				result += "."
			}
		}
		result += "|\n"
	}
	return result
}

func TestDBRestartPersistence(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_restart_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	defer os.Remove(tmpfile)

	// Phase 1: Insert 100 values one at a time
	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%03d", i))
		value := []byte(fmt.Sprintf("value%03d", i))
		err := db.Update(func(tx *fredb.Tx) error {
			return tx.Put(key, value)
		})
		require.NoError(t, err, "Failed to insert key%03d", i)
	}

	// Close the database
	err = db.Close()
	require.NoError(t, err)

	// Phase 2: Reopen and read the last value
	db, err = fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err)
	defer db.Close()

	// Read the last key inserted
	lastKey := []byte("key099")
	expectedValue := []byte("value099")

	err = db.View(func(tx *fredb.Tx) error {
		val, err := tx.Get(lastKey)
		if err != nil {
			return err
		}
		if val == nil {
			return fmt.Errorf("key099 returned nil after restart")
		}
		assert.Equal(t, expectedValue, val, "Value mismatch after restart")
		return nil
	})
	require.NoError(t, err)

	// Verify all 100 keys are readable
	err = db.View(func(tx *fredb.Tx) error {
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key%03d", i))
			val, err := tx.Get(key)
			if err != nil {
				return err
			}
			if val == nil {
				return fmt.Errorf("key%03d returned nil after restart", i)
			}
		}
		return nil
	})
	require.NoError(t, err, "Failed to read all keys after restart")
}

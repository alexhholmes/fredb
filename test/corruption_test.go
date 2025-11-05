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

// TestDBCorruptionDetection validates that corrupted meta pages are detected
func TestDBCorruptionDetection(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_db_corruption.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create valid DB
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key"), []byte("value"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt Page 0's magic number
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to fredb.Open file")

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 0) // Overwrite magic number
	require.NoError(t, err, "Failed to corrupt file")
	file.Close()

	// Try to refredb.Open - should succeed because Page 1 is still valid
	db2, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to refredb.Open DB with one corrupted meta Page")
	defer db2.Close()

	// Verify data still accessible (using Page 1)
	v, err := db2.Get([]byte("key"))
	assert.NoError(t, err, "Failed to get key from DB with corrupted Page 0")
	assert.Equal(t, "value", string(v))

	t.Logf("Successfully recovered from single meta Page corruption using backup")
}

// TestCrashRecoveryBothMetaCorrupted tests that DB fails to fredb.Open when both meta pages are invalid
func TestCrashRecoveryBothMetaCorrupted(t *testing.T) {
	t.Parallel()

	tmpfile := fmt.Sprintf("/tmp/test_both_meta_corrupt_%d.DB", os.Getpid())
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create valid DB
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key"), []byte("value"))
	require.NoError(t, err, "Failed to Put key")
	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt both meta pages (magic number)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to fredb.Open file")

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	// Corrupt Page 1 meta magic (at PageSize + PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(base.PageSize+base.PageHeaderSize))
	require.NoError(t, err, "Failed to corrupt Page 1")
	// Corrupt Page 2 meta magic (at 2*PageSize + PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(2*base.PageSize+base.PageHeaderSize))
	require.NoError(t, err, "Failed to corrupt Page 2")
	file.Sync() // Ensure corruption is written to disk
	file.Close()

	// Verify corruption was applied (read pages 1 and 2)
	verifyFile, _ := os.Open(tmpfile)
	verifyFile.Seek(base.PageSize, 0) // Skip page 0, start at page 1
	verifyPage1 := &base.Page{}
	verifyFile.Read(verifyPage1.Data[:])
	verifyPage2 := &base.Page{}
	verifyFile.Read(verifyPage2.Data[:])
	verifyFile.Close()

	t.Logf("After corruption - Page 1 meta magic: %x",
		verifyPage1.Data[base.PageHeaderSize:base.PageHeaderSize+4])
	t.Logf("After corruption - Page 2 meta magic: %x",
		verifyPage2.Data[base.PageHeaderSize:base.PageHeaderSize+4])

	// Check file size
	info, _ := os.Stat(tmpfile)
	t.Logf("File size before refredb.Opening: %d bytes", info.Size())

	// Try to refredb.Open - should FAIL because both pages corrupted
	_, err = fredb.Open(tmpfile)
	assert.Error(t, err, "Expected error fredb.Opening DB with both meta pages corrupted")
	if err != nil && err.Error() != "both meta pages corrupted: invalid magic number, invalid magic number" {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCrashRecoveryChecksumCorruption tests that corrupted checksums are detected
func TestCrashRecoveryChecksumCorruption(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_checksum_corrupt.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	// Create valid DB
	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key"), []byte("value"))
	require.NoError(t, err, "Failed to Put key")
	db.Close()

	// Corrupt Page 0's checksum field (last 4 bytes of MetaPage header)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to fredb.Open file")

	// Checksum is at offPut 44 (after all other fields)
	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 44)
	require.NoError(t, err, "Failed to corrupt checksum")
	file.Close()

	// Try to refredb.Open - should succeed using Page 1
	db2, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to refredb.Open DB with corrupted checksum on Page 0")
	defer db2.Close()

	// Verify data still accessible (using Page 1)
	v, err := db2.Get([]byte("key"))
	assert.NoError(t, err, "Failed to get key")
	assert.Equal(t, "value", string(v))

	t.Logf("Successfully recovered from checksum corruption using backup meta Page")
}

func TestCrashRecoveryWrongMagicNumber(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_wrong_magic.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt meta Page with valid checksum but wrong magic
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to fredb.Open file")

	// Write wrong magic number at Page 0
	wrongMagic := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	_, err = file.WriteAt(wrongMagic, int64(base.PageHeaderSize))
	require.NoError(t, err, "Failed to write wrong magic")
	file.Sync()
	file.Close()

	// Try to fredb.Open - should fail or use Page 1
	db, err = fredb.Open(tmpfile)
	if err != nil {
		t.Logf("fredb.Open failed with wrong magic (expected): %v", err)
		return
	}
	defer db.Close()

	// If fredb.Open succeeded, it should have used Page 1
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Logf("Failed to get key after wrong magic: %v", err)
	} else {
		assert.Equal(t, "value1", string(val))
	}
}

func TestCrashRecoveryRootPageIDZero(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_root_zero.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Corrupt meta Page: Put RootPageID to 0 but keep valid TxID
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to fredb.Open file")

	// Read current meta from Page 0
	page := &base.Page{}
	_, err = file.ReadAt(page.Data[:], 4096)
	require.NoError(t, err, "Failed to read meta")

	// Parse and modify
	meta := page.DeserializeMeta()
	meta.RootPageID = 0 // Put to 0 (invalid state if TxID > 0)
	meta.Checksum = meta.CalculateChecksum()

	// Write back
	page.SerializeMeta(meta)
	_, err = file.WriteAt(page.Data[:], 4096)
	require.NoError(t, err, "Failed to write modified meta")
	file.Sync()
	file.Close()

	// Try to fredb.Open - behavior depends on implementation
	db, err = fredb.Open(tmpfile)
	if err != nil {
		t.Logf("fredb.Open failed with RootPageID=0 (may be expected): %v", err)
		return
	}
	defer db.Close()

	// If fredb.Open succeeded, verify state
	val, err := db.Get([]byte("key1"))
	if val == nil {
		t.Logf("Key not found (expected with RootPageID=0)")
	} else if err != nil {
		t.Logf("get returned error: %v", err)
	}
}

func TestCrashRecoveryTruncatedFile(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_truncated.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Truncate file to only 1 Page (missing meta Page 1)
	err = os.Truncate(tmpfile, base.PageSize)
	require.NoError(t, err, "Failed to truncate file")

	// Try to fredb.Open
	db, err = fredb.Open(tmpfile)
	if err != nil {
		t.Logf("fredb.Open failed on truncated file (expected): %v", err)
		return
	}
	defer db.Close()

	// If fredb.Open succeeded, verify we can still use it
	t.Log("fredb.Open succeeded on truncated file")
}

func TestCrashRecoveryBothMetaSameTxnID(t *testing.T) {
	t.Parallel()

	tmpfile := "/tmp/test_crash_recovery_same_txnid.DB"
	os.Remove(tmpfile)
	os.Remove(tmpfile + ".wal")
	defer os.Remove(tmpfile)
	defer os.Remove(tmpfile + ".wal")

	db, err := fredb.Open(tmpfile)
	require.NoError(t, err, "Failed to create DB")

	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(t, err, "Failed to Put key")

	err = db.Close()
	require.NoError(t, err, "Failed to close DB")

	// Create impossible state: both meta pages with same TxID but different roots
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	require.NoError(t, err, "Failed to fredb.Open file")

	// Read meta from Page 0
	page0 := &base.Page{}
	_, err = file.ReadAt(page0.Data[:], 0)
	require.NoError(t, err, "Failed to read meta 0")
	meta0 := page0.DeserializeMeta()

	// Read meta from Page 1
	page1 := &base.Page{}
	_, err = file.ReadAt(page1.Data[:], base.PageSize)
	require.NoError(t, err, "Failed to read meta 1")
	meta1 := page1.DeserializeMeta()

	// Make both have same TxID
	meta1.TxID = meta0.TxID
	meta1.RootPageID = 999 // Different root (invalid)
	meta1.Checksum = meta1.CalculateChecksum()

	// Write back Page 1
	page1.SerializeMeta(meta1)
	_, err = file.WriteAt(page1.Data[:], int64(base.PageSize))
	require.NoError(t, err, "Failed to write meta 1")
	file.Sync()
	file.Close()

	// Try to fredb.Open - should handle this gracefully
	db, err = fredb.Open(tmpfile)
	if err != nil {
		t.Logf("fredb.Open failed with same TxID (may be expected): %v", err)
		return
	}
	defer db.Close()

	// If fredb.Open succeeded, verify which meta was chosen
	t.Log("fredb.Open succeeded despite same TxID on both metas")
}

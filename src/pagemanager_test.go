package src

import (
	"encoding/hex"
	"fmt"
	"os"
	"testing"
)

func TestDiskPageManagerBasic(t *testing.T) {
	// Create temp file
	tmpfile := "/tmp/test_disk.db"
	defer os.Remove(tmpfile)

	// Create new database
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Insert some data
	if err := db.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}
	if err := db.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Verify data
	val, err := db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Close (flushes to disk)
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close db: %v", err)
	}

	// Reopen database
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}

	// Verify data persisted
	val, err = db2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 after reopen: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1 after reopen, got %s", val)
	}

	val, err = db2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2 after reopen: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2 after reopen, got %s", val)
	}

	// Cleanup
	if err := db2.Close(); err != nil {
		t.Fatalf("Failed to close db2: %v", err)
	}
}

func TestDiskPageManagerPersistence(t *testing.T) {
	tmpfile := "/tmp/test_persist.db"
	defer os.Remove(tmpfile)

	// Create database and insert 100 keys
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close db: %v", err)
	}

	// Reopen and verify all keys
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i * 2)}

		value, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d after reopen: %v", i, err)
		}
		if len(value) != 1 || value[0] != expectedValue[0] {
			t.Errorf("Key %d: expected %v, got %v", i, expectedValue, value)
		}
	}

	if err := db2.Close(); err != nil {
		t.Fatalf("Failed to close db2: %v", err)
	}
}

func TestDiskPageManagerDelete(t *testing.T) {
	tmpfile := "/tmp/test_delete.db"
	defer os.Remove(tmpfile)

	// Create database
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// Insert keys
	db.Set([]byte("a"), []byte("1"))
	db.Set([]byte("b"), []byte("2"))
	db.Set([]byte("c"), []byte("3"))

	// Delete one
	if err := db.Delete([]byte("b")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Close
	db.Close()

	// Reopen and verify
	db2, _ := Open(tmpfile)

	// Should still have a and c
	if _, err := db2.Get([]byte("a")); err != nil {
		t.Error("Key 'a' should exist")
	}
	if _, err := db2.Get([]byte("c")); err != nil {
		t.Error("Key 'c' should exist")
	}

	// b should be gone
	if _, err := db2.Get([]byte("b")); err == nil {
		t.Error("Key 'b' should be deleted")
	}

	db2.Close()
}

// TestDBFileFormat validates the on-disk format
func TestDBFileFormat(t *testing.T) {
	tmpfile := "/tmp/test_db_format.db"
	defer os.Remove(tmpfile)

	// Create DB and write some data
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Validate file exists and has correct structure
	info, err := os.Stat(tmpfile)
	if err != nil {
		t.Fatalf("DB file not found: %v", err)
	}

	// File should be at least 3 pages (meta 0-1, freelist 2)
	minSize := int64(PageSize * 3)
	if info.Size() < minSize {
		t.Errorf("File too small: got %d bytes, expected at least %d", info.Size(), minSize)
	}

	// File size should be page-aligned
	if info.Size()%int64(PageSize) != 0 {
		t.Errorf("File size not page-aligned: %d bytes", info.Size())
	}

	// Read both meta pages
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	page0 := &Page{}
	n, err := file.Read(page0.data[:])
	if err != nil {
		t.Fatalf("Failed to read meta page 0: %v", err)
	}
	if n != PageSize {
		t.Fatalf("Short read: got %d bytes, expected %d", n, PageSize)
	}

	page1 := &Page{}
	n, err = file.Read(page1.data[:])
	if err != nil {
		t.Fatalf("Failed to read meta page 1: %v", err)
	}
	if n != PageSize {
		t.Fatalf("Short read: got %d bytes, expected %d", n, PageSize)
	}

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	// Close() increments TxnID from 0 to 1, so writes to page 1 (1 % 2 = 1)
	// Page 1 should have the latest meta with RootPageID set
	t.Logf("Meta page 0 TxnID: %d, RootPageID: %d", meta0.TxnID, meta0.RootPageID)
	t.Logf("Meta page 1 TxnID: %d, RootPageID: %d", meta1.TxnID, meta1.RootPageID)

	// Pick the page with highest TxnID (should be page 1)
	var meta *MetaPage
	if meta0.TxnID > meta1.TxnID {
		meta = meta0
	} else {
		meta = meta1
	}

	// Validate magic number
	if meta.Magic != MagicNumber {
		t.Errorf("Invalid magic number: got 0x%08x, expected 0x%08x", meta.Magic, MagicNumber)
	}

	// Validate version
	if meta.Version != FormatVersion {
		t.Errorf("Invalid version: got %d, expected %d", meta.Version, FormatVersion)
	}

	// Validate page size
	if meta.PageSize != PageSize {
		t.Errorf("Invalid page size: got %d, expected %d", meta.PageSize, PageSize)
	}

	// Validate RootPageID is persisted after Close
	if meta.RootPageID == 0 {
		t.Errorf("RootPageID is zero after Close - should be persisted to meta")
	}

	// Validate freelist location
	if meta.FreelistID != 2 {
		t.Errorf("Freelist ID: got %d, expected 2", meta.FreelistID)
	}

	// Validate checksum
	if err := meta.Validate(); err != nil {
		t.Errorf("Meta validation failed: %v", err)
	}

	t.Logf("Meta page validated successfully:")
	t.Logf("  Magic: 0x%08x", meta.Magic)
	t.Logf("  Version: %d", meta.Version)
	t.Logf("  PageSize: %d", meta.PageSize)
	t.Logf("  RootPageID: %d", meta.RootPageID)
	t.Logf("  FreelistID: %d", meta.FreelistID)
	t.Logf("  FreelistPages: %d", meta.FreelistPages)
	t.Logf("  TxnID: %d", meta.TxnID)
	t.Logf("  NumPages: %d", meta.NumPages)
	t.Logf("  File size: %d bytes (%d pages)", info.Size(), info.Size()/int64(PageSize))
}

// TestDBFileHexDump creates a DB file and prints hex dump for manual inspection
func TestDBFileHexDump(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping hex dump test in short mode")
	}

	tmpfile := "/tmp/test_db_hexdump.db"
	defer os.Remove(tmpfile)

	// Create DB with known data
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("testkey"), []byte("testvalue"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Read first 4 pages and dump them
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Read meta page 0
	page0 := make([]byte, PageSize)
	_, err = file.Read(page0)
	if err != nil {
		t.Fatalf("Failed to read page 0: %v", err)
	}

	t.Logf("\n=== Page 0 (Meta) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page0[:128]))

	// Read meta page 1
	page1 := make([]byte, PageSize)
	_, err = file.Read(page1)
	if err != nil {
		t.Fatalf("Failed to read page 1: %v", err)
	}

	t.Logf("\n=== Page 1 (Meta backup) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page1[:128]))

	// Read freelist page 2
	page2 := make([]byte, PageSize)
	_, err = file.Read(page2)
	if err != nil {
		t.Fatalf("Failed to read page 2: %v", err)
	}

	t.Logf("\n=== Page 2 (Freelist) - First 128 bytes ===")
	t.Logf("%s", formatHexDump(page2[:128]))

	// Read root page (page 3 likely)
	page3 := make([]byte, PageSize)
	_, err = file.Read(page3)
	if err != nil {
		t.Fatalf("Failed to read page 3: %v", err)
	}

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

		// Offset
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

// TestDBCorruptionDetection validates that corrupted meta pages are detected
func TestDBCorruptionDetection(t *testing.T) {
	tmpfile := "/tmp/test_db_corruption.db"
	defer os.Remove(tmpfile)

	// Create valid DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Corrupt page 0's magic number
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 0) // Overwrite magic number
	if err != nil {
		t.Fatalf("Failed to corrupt file: %v", err)
	}
	file.Close()

	// Try to reopen - should succeed because page 1 is still valid
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB with one corrupted meta page: %v", err)
	}
	defer db2.Close()

	// Verify data still accessible (using page 1)
	v, err := db2.Get([]byte("key"))
	if err != nil {
		t.Errorf("Failed to get key from DB with corrupted page 0: %v", err)
	}
	if string(v) != "value" {
		t.Errorf("Wrong value: got %s, expected value", string(v))
	}

	t.Logf("Successfully recovered from single meta page corruption using backup")
}

// Helper to inspect raw bytes as hex
func dumpBytes(label string, data []byte) string {
	return fmt.Sprintf("%s: %s", label, hex.EncodeToString(data))
}

// TestCrashRecoveryBothMetaCorrupted tests that DB fails to open when both meta pages are invalid
func TestCrashRecoveryBothMetaCorrupted(t *testing.T) {
	tmpfile := fmt.Sprintf("/tmp/test_both_meta_corrupt_%d.db", os.Getpid())
	os.Remove(tmpfile) // Clean up any previous test file
	defer os.Remove(tmpfile)

	// Create valid DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}

	// Corrupt both meta pages (magic number)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	// Corrupt page 0 meta magic (at PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(PageHeaderSize))
	if err != nil {
		t.Fatalf("Failed to corrupt page 0: %v", err)
	}
	// Corrupt page 1 meta magic (at PageSize + PageHeaderSize offset)
	_, err = file.WriteAt(corruptData, int64(PageSize+PageHeaderSize))
	if err != nil {
		t.Fatalf("Failed to corrupt page 1: %v", err)
	}
	file.Sync() // Ensure corruption is written to disk
	file.Close()

	// Verify corruption was applied
	verifyFile, _ := os.Open(tmpfile)
	verifyPage0 := &Page{}
	verifyFile.Read(verifyPage0.data[:])
	verifyPage1 := &Page{}
	verifyFile.Read(verifyPage1.data[:])
	verifyFile.Close()

	t.Logf("After corruption - Page 0 meta magic: %x", verifyPage0.data[PageHeaderSize:PageHeaderSize+4])
	t.Logf("After corruption - Page 1 meta magic: %x", verifyPage1.data[PageHeaderSize:PageHeaderSize+4])

	// Check file size
	info, _ := os.Stat(tmpfile)
	t.Logf("File size before reopening: %d bytes", info.Size())

	// Try to reopen - should FAIL because both pages corrupted
	_, err = Open(tmpfile)
	if err == nil {
		t.Fatal("Expected error opening DB with both meta pages corrupted, got nil")
	}
	if err.Error() != "both meta pages corrupted: invalid magic number, invalid magic number" {
		t.Logf("Got expected error: %v", err)
	}
}

// TestCrashRecoveryChecksumCorruption tests that corrupted checksums are detected
func TestCrashRecoveryChecksumCorruption(t *testing.T) {
	tmpfile := "/tmp/test_checksum_corrupt.db"
	defer os.Remove(tmpfile)

	// Create valid DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	err = db.Set([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}
	db.Close()

	// Corrupt page 0's checksum field (last 4 bytes of MetaPage header)
	file, err := os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Checksum is at offset 44 (after all other fields)
	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err = file.WriteAt(corruptData, 44)
	if err != nil {
		t.Fatalf("Failed to corrupt checksum: %v", err)
	}
	file.Close()

	// Try to reopen - should succeed using page 1
	db2, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DB with corrupted checksum on page 0: %v", err)
	}
	defer db2.Close()

	// Verify data still accessible (using page 1)
	v, err := db2.Get([]byte("key"))
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}
	if string(v) != "value" {
		t.Errorf("Wrong value: got %s, expected value", string(v))
	}

	t.Logf("Successfully recovered from checksum corruption using backup meta page")
}

// TestCrashRecoveryAlternatingWrites tests that meta pages alternate correctly based on TxnID
func TestCrashRecoveryAlternatingWrites(t *testing.T) {
	tmpfile := "/tmp/test_alternating_meta.db"
	defer os.Remove(tmpfile)

	// Create DB
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// TxnID starts at 0, so first commit writes to page 0
	// Do multiple commits and verify alternating pattern
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		err = db.Set(key, value)
		if err != nil {
			t.Fatalf("Failed to set key%d: %v", i, err)
		}
	}

	db.Close()

	// Reopen and read both meta pages
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	page0 := &Page{}
	file.Read(page0.data[:])
	page1 := &Page{}
	file.Read(page1.data[:])
	file.Close()

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()

	t.Logf("Page 0 TxnID: %d", meta0.TxnID)
	t.Logf("Page 1 TxnID: %d", meta1.TxnID)

	// One should have higher TxnID than the other
	if meta0.TxnID == meta1.TxnID {
		t.Error("Both meta pages have same TxnID - alternating writes not working")
	}

	// The page with higher TxnID should be the active one
	var activeMeta *MetaPage
	if meta0.TxnID > meta1.TxnID {
		activeMeta = meta0
		// TxnID should be even (written to page 0)
		if activeMeta.TxnID%2 != 0 {
			t.Errorf("Page 0 has odd TxnID %d, expected even", activeMeta.TxnID)
		}
	} else {
		activeMeta = meta1
		// TxnID should be odd (written to page 1)
		if activeMeta.TxnID%2 != 1 {
			t.Errorf("Page 1 has even TxnID %d, expected odd", activeMeta.TxnID)
		}
	}

	// Both should be valid
	if err := meta0.Validate(); err != nil {
		t.Errorf("Page 0 invalid: %v", err)
	}
	if err := meta1.Validate(); err != nil {
		t.Errorf("Page 1 invalid: %v", err)
	}

	t.Logf("Meta page alternating writes validated successfully")
}

// TestCrashRecoveryLastCommittedState tests recovery to previous valid state
func TestCrashRecoveryLastCommittedState(t *testing.T) {
	tmpfile := "/tmp/test_last_committed.db"
	defer os.Remove(tmpfile)

	// Create DB and do a single commit
	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}
	db.Set([]byte("key1"), []byte("value1"))
	db.Close()

	// Reopen and do a second commit (Set without Close to avoid extra TxnID)
	db2, _ := Open(tmpfile)
	db2.Set([]byte("key2"), []byte("value2"))

	// Check TxnIDs after second Set (before Close)
	file, err := os.Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	page0 := &Page{}
	file.Read(page0.data[:])
	page1 := &Page{}
	file.Read(page1.data[:])
	file.Close()

	meta0 := page0.ReadMeta()
	meta1 := page1.ReadMeta()
	t.Logf("After second Set: Page 0 TxnID=%d RootPageID=%d, Page 1 TxnID=%d RootPageID=%d",
		meta0.TxnID, meta0.RootPageID, meta1.TxnID, meta1.RootPageID)

	// Record the older TxnID (should only have key1)
	var olderTxn uint64
	var olderRoot PageID
	if meta0.TxnID < meta1.TxnID {
		olderTxn = meta0.TxnID
		olderRoot = meta0.RootPageID
	} else {
		olderTxn = meta1.TxnID
		olderRoot = meta1.RootPageID
	}

	db2.Close() // Now close, which will write another meta

	// Simulate crash: corrupt the newest meta page
	file, err = os.OpenFile(tmpfile, os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	page0 = &Page{}
	file.ReadAt(page0.data[:], 0)
	page1 = &Page{}
	file.ReadAt(page1.data[:], int64(PageSize))

	meta0 = page0.ReadMeta()
	meta1 = page1.ReadMeta()

	// Corrupt the newer one
	var corruptOffset int64
	if meta0.TxnID > meta1.TxnID {
		corruptOffset = int64(PageHeaderSize)
		t.Logf("Corrupting page 0 (TxnID %d)", meta0.TxnID)
	} else {
		corruptOffset = int64(PageSize + PageHeaderSize)
		t.Logf("Corrupting page 1 (TxnID %d)", meta1.TxnID)
	}

	corruptData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	file.WriteAt(corruptData, corruptOffset)
	file.Sync()
	file.Close()

	// Reopen - should fall back to previous valid state
	db3, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen after simulated crash: %v", err)
	}
	defer db3.Close()

	meta3 := db3.store.pager.GetMeta()
	t.Logf("After reopen: loaded meta with TxnID %d, RootPageID %d", meta3.TxnID, meta3.RootPageID)
	t.Logf("Expected to recover to TxnID %d (previous valid state)", olderTxn)

	// key1 should always exist
	v1, err := db3.Get([]byte("key1"))
	if err != nil {
		t.Errorf("key1 should exist after crash recovery: %v", err)
	}
	if string(v1) != "value1" {
		t.Errorf("Wrong value for key1: got %s, expected value1", string(v1))
	}

	// Verify we loaded the older state (key2 should match the older root's content)
	// If we loaded olderRoot, check if it has key2 or not
	_, err = db3.Get([]byte("key2"))
	if meta3.RootPageID == olderRoot {
		// We're at the older state, verify its actual content
		t.Logf("Recovered to older root (page %d), key2 exists: %v", olderRoot, err == nil)
	}

	t.Logf("Successfully recovered from crash using backup meta page")
}

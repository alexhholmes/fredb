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
	dm, err := NewDiskPageManager(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DiskPageManager: %v", err)
	}

	// Create BTree
	btree, err := NewBTree(dm)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// Insert some data
	if err := btree.Set([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to set key1: %v", err)
	}
	if err := btree.Set([]byte("key2"), []byte("value2")); err != nil {
		t.Fatalf("Failed to set key2: %v", err)
	}

	// Verify data
	val, err := btree.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}

	// Close (flushes to disk and closes pager)
	if err := btree.Close(); err != nil {
		t.Fatalf("Failed to close btree: %v", err)
	}

	// Reopen database
	dm2, err := NewDiskPageManager(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DiskPageManager: %v", err)
	}

	btree2, err := NewBTree(dm2)
	if err != nil {
		t.Fatalf("Failed to reopen BTree: %v", err)
	}

	// Verify data persisted
	val, err = btree2.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1 after reopen: %v", err)
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1 after reopen, got %s", val)
	}

	val, err = btree2.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("Failed to get key2 after reopen: %v", err)
	}
	if string(val) != "value2" {
		t.Errorf("Expected value2 after reopen, got %s", val)
	}

	// Cleanup
	if err := btree2.Close(); err != nil {
		t.Fatalf("Failed to close btree2: %v", err)
	}
}

func TestDiskPageManagerPersistence(t *testing.T) {
	tmpfile := "/tmp/test_persist.db"
	defer os.Remove(tmpfile)

	// Create database and insert 100 keys
	dm, err := NewDiskPageManager(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DiskPageManager: %v", err)
	}

	btree, err := NewBTree(dm)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		if err := btree.Set(key, value); err != nil {
			t.Fatalf("Failed to set key %d: %v", i, err)
		}
	}

	if err := btree.Close(); err != nil {
		t.Fatalf("Failed to close btree: %v", err)
	}

	// Reopen and verify all keys
	dm2, err := NewDiskPageManager(tmpfile)
	if err != nil {
		t.Fatalf("Failed to reopen DiskPageManager: %v", err)
	}

	btree2, err := NewBTree(dm2)
	if err != nil {
		t.Fatalf("Failed to reopen BTree: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		expectedValue := []byte{byte(i * 2)}

		value, err := btree2.Get(key)
		if err != nil {
			t.Fatalf("Failed to get key %d after reopen: %v", i, err)
		}
		if len(value) != 1 || value[0] != expectedValue[0] {
			t.Errorf("Key %d: expected %v, got %v", i, expectedValue, value)
		}
	}

	if err := btree2.Close(); err != nil {
		t.Fatalf("Failed to close btree2: %v", err)
	}
}

func TestDiskPageManagerDelete(t *testing.T) {
	tmpfile := "/tmp/test_delete.db"
	defer os.Remove(tmpfile)

	// Create database
	dm, err := NewDiskPageManager(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DiskPageManager: %v", err)
	}

	btree, err := NewBTree(dm)
	if err != nil {
		t.Fatalf("Failed to create BTree: %v", err)
	}

	// Insert keys
	btree.Set([]byte("a"), []byte("1"))
	btree.Set([]byte("b"), []byte("2"))
	btree.Set([]byte("c"), []byte("3"))

	// Delete one
	if err := btree.Delete([]byte("b")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Close
	btree.Close()

	// Reopen and verify
	dm2, _ := NewDiskPageManager(tmpfile)
	btree2, _ := NewBTree(dm2)

	// Should still have a and c
	if _, err := btree2.Get([]byte("a")); err != nil {
		t.Error("Key 'a' should exist")
	}
	if _, err := btree2.Get([]byte("c")); err != nil {
		t.Error("Key 'c' should exist")
	}

	// b should be gone
	if _, err := btree2.Get([]byte("b")); err == nil {
		t.Error("Key 'b' should be deleted")
	}

	btree2.Close()
}

// TestDBFileFormat validates the on-disk format
func TestDBFileFormat(t *testing.T) {
	tmpfile := "/tmp/test_db_format.db"
	defer os.Remove(tmpfile)

	// Create DB and write some data
	db, err := NewDB(tmpfile)
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
	db, err := NewDB(tmpfile)
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
	db, err := NewDB(tmpfile)
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
	db2, err := NewDB(tmpfile)
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

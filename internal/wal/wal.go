package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"fredb/internal/base"
)

// WALSyncMode controls when the WAL is fsynced to disk.
type WALSyncMode int

const (
	// WALSyncEveryCommit fsyncs on every transaction commit (BoltDB-style).
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data, etcd or Raft
	WALSyncEveryCommit WALSyncMode = iota

	// WALSyncBytes fsyncs when bytesPerSync bytes have been written (RocksDB-style).
	// - Higher throughput than per-commit fsync
	// - Data loss window: up to bytesPerSync bytes on power failure
	// - Use for: Analytics, caches, high-throughput workloads
	WALSyncBytes

	// WALSyncOff disables fsync entirely (testing/bulk loads only).
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	WALSyncOff
)

// WAL implements Write-Ahead Logging for crash recovery and batched commits
type WAL struct {
	file   *os.File
	mu     sync.Mutex
	offset int64 // Current write position

	// Sync configuration
	syncMode       WALSyncMode
	bytesPerSync   int64
	bytesSinceSync int64 // Bytes written since last fsync

	// Page tracking - latches prevent stale disk reads
	Pages sync.Map // PageID -> uint64 (txnID) - Pages in WAL but not yet on disk
}

// Record types
const (
	WALRecordPage   uint8 = 1 // Page write
	WALRecordCommit uint8 = 2 // Commit marker
)

// Record format: [Type:1][TxnID:8][PageID:8][DataLen:4][Data:N]
const WALRecordHeaderSize = 1 + 8 + 8 + 4

// NewWAL opens or creates a WAL file with the specified sync mode
func NewWAL(path string, syncMode WALSyncMode, bytesPerSync int64) (*WAL, error) {
	// Open wal file with read/write, create if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}

	// get current file size to set offset
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &WAL{
		file:           file,
		offset:         info.Size(),
		syncMode:       syncMode,
		bytesPerSync:   bytesPerSync,
		bytesSinceSync: 0,
	}, nil
}

// AppendPage writes a Page record to the WAL
// Format: [WALRecordPage:1][TxnID:8][PageID:8][PageSize:4][Page.data:PageSize]
//
// KNOWN LIMITATION: Uses two separate write() calls (header + data).
// If crash occurs between writes, WAL will have partial record, causing
// recovery to fail. Future fix: buffer entire record for atomic write or add checksums.
func (w *WAL) AppendPage(txnID uint64, pageID base.PageID, page *base.Page) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build record header
	header := make([]byte, WALRecordHeaderSize)
	header[0] = WALRecordPage
	binary.LittleEndian.PutUint64(header[1:9], txnID)
	binary.LittleEndian.PutUint64(header[9:17], uint64(pageID))
	binary.LittleEndian.PutUint32(header[17:21], base.PageSize)

	// Write header
	if _, err := w.file.Write(header); err != nil {
		return err
	}

	// Write Page data
	if _, err := w.file.Write(page.Data[:]); err != nil {
		return err
	}

	// Update offset and track bytes since sync
	bytesWritten := int64(WALRecordHeaderSize + base.PageSize)
	w.offset += bytesWritten
	w.bytesSinceSync += bytesWritten

	// Set latch to prevent stale disk reads
	w.Pages.Store(pageID, txnID)

	return nil
}

// AppendCommit writes a commit marker to the WAL
// Format: [WALRecordCommit:1][TxnID:8][0:8][0:4]
func (w *WAL) AppendCommit(txnID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build commit record (no data payload)
	header := make([]byte, WALRecordHeaderSize)
	header[0] = WALRecordCommit
	binary.LittleEndian.PutUint64(header[1:9], txnID)
	binary.LittleEndian.PutUint64(header[9:17], 0)
	binary.LittleEndian.PutUint32(header[17:21], 0)

	// Write commit marker
	if _, err := w.file.Write(header); err != nil {
		return err
	}

	// Update offset and track bytes since sync
	bytesWritten := int64(WALRecordHeaderSize)
	w.offset += bytesWritten
	w.bytesSinceSync += bytesWritten

	return nil
}

// Sync conditionally fsyncs the WAL based on sync mode configuration
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.syncMode {
	case WALSyncEveryCommit:
		// Always sync
		return w.syncUnsafe()

	case WALSyncBytes:
		// Sync if we've exceeded the byte threshold
		if w.bytesSinceSync >= w.bytesPerSync {
			return w.syncUnsafe()
		}
		return nil

	case WALSyncOff:
		// Never sync
		return nil

	default:
		return fmt.Errorf("unknown wal sync mode: %d", w.syncMode)
	}
}

// ForceSync unconditionally fsyncs the WAL regardless of sync mode.
// Used during Close() and checkpoint to ensure durability.
func (w *WAL) ForceSync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.syncUnsafe()
}

// syncUnsafe performs fsync and resets the byte counter.
// Caller must hold w.mu.
func (w *WAL) syncUnsafe() error {
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.bytesSinceSync = 0
	return nil
}

// WALRecord represents a single WAL record
type WALRecord struct {
	Type   uint8
	TxnID  uint64
	PageID base.PageID
	Page   *base.Page
}

// Replay reads the WAL and applies all committed transactions after fromTxnID
func (w *WAL) Replay(fromTxnID uint64, applyFn func(base.PageID, *base.Page) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning of wal
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Track uncommitted transactions
	// Map: TxnID -> list of (PageID, Page) to apply if commit marker found
	uncommitted := make(map[uint64][]WALRecord)

	header := make([]byte, WALRecordHeaderSize)

	for {
		// Read record header
		n, err := w.file.Read(header)
		if err == io.EOF {
			break // End of wal
		}
		if err != nil {
			return fmt.Errorf("wal replay read error: %w", err)
		}
		if n != WALRecordHeaderSize {
			return fmt.Errorf("wal replay: short header read: %d bytes", n)
		}

		// Parse header
		recordType := header[0]
		txnID := binary.LittleEndian.Uint64(header[1:9])
		pageID := base.PageID(binary.LittleEndian.Uint64(header[9:17]))
		dataLen := binary.LittleEndian.Uint32(header[17:21])

		switch recordType {
		case WALRecordPage:
			// Read Page data
			if dataLen != base.PageSize {
				return fmt.Errorf("wal replay: invalid Page size: %d", dataLen)
			}

			page := &base.Page{}
			if _, err := w.file.Read(page.Data[:]); err != nil {
				return fmt.Errorf("wal replay: failed to read Page data: %w", err)
			}

			// store in uncommitted map
			uncommitted[txnID] = append(uncommitted[txnID], WALRecord{
				Type:   WALRecordPage,
				TxnID:  txnID,
				PageID: pageID,
				Page:   page,
			})

		case WALRecordCommit:
			// Transaction committed - apply all Pages if txnID > fromTxnID
			if txnID > fromTxnID {
				for _, record := range uncommitted[txnID] {
					if err := applyFn(record.PageID, record.Page); err != nil {
						return fmt.Errorf("wal replay: failed to apply Page %d: %w", record.PageID, err)
					}
				}
			}

			// Remove from uncommitted
			delete(uncommitted, txnID)

		default:
			return fmt.Errorf("wal replay: unknown record type: %d", recordType)
		}
	}

	// Seek back to end for future appends
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
}

// Truncate removes all WAL records up to and including the given TxnID
// SAFETY: Only call this after meta.CheckpointTxnID has been updated to upToTxnID
// to ensure we don't lose uncheckpointed data.
func (w *WAL) Truncate(upToTxnID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Find the offset where we should truncate
	// Scan wal and find the first commit record with TxnID > upToTxnID

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := make([]byte, WALRecordHeaderSize)
	truncateOffset := int64(0)

	for {
		currentOffset, _ := w.file.Seek(0, io.SeekCurrent)

		// Read record header
		n, err := w.file.Read(header)
		if err == io.EOF {
			// Reached end - truncate entire wal
			truncateOffset = 0
			break
		}
		if err != nil {
			return fmt.Errorf("wal truncate read error: %w", err)
		}
		if n != WALRecordHeaderSize {
			return fmt.Errorf("wal truncate: short header read")
		}

		recordType := header[0]
		txnID := binary.LittleEndian.Uint64(header[1:9])
		dataLen := binary.LittleEndian.Uint32(header[17:21])

		// Skip data section if present
		if recordType == WALRecordPage {
			if _, err := w.file.Seek(int64(dataLen), io.SeekCurrent); err != nil {
				return err
			}
		}

		// Check if this commit is beyond our checkpoint
		if recordType == WALRecordCommit && txnID > upToTxnID {
			// Found first commit beyond checkpoint - truncate here
			truncateOffset = currentOffset
			break
		}
	}

	// Truncate file
	if err := w.file.Truncate(truncateOffset); err != nil {
		return err
	}

	// Seek to end for future appends
	newSize, err := w.file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	w.offset = newSize

	return nil
}

// CleanupLatch removes WAL Pages latches for Pages that have been checkpointed
// AND are visible to all active readers (txnID < minReaderTxn).
func (w *WAL) CleanupLatch(checkpointTxn, minReaderTxn uint64) {
	w.Pages.Range(func(key, value interface{}) bool {
		pageID := key.(base.PageID)
		txnID := value.(uint64)

		// Only remove latch if BOTH conditions true:
		// 1. Page is checkpointed (written to disk)
		// 2. All active readers can see this version (no reader needs older version)
		if txnID <= checkpointTxn && txnID < minReaderTxn {
			w.Pages.Delete(pageID)
		}
		return true // Continue iteration
	})
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.file.Close()
}

package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"fredb/internal/base"
	"fredb/internal/directio"
)

// SyncMode controls when the WAL is fsynced to disk.
type SyncMode int

const (
	// SyncEveryCommit fsyncs on every transaction commit (BoltDB-style).
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data, etcd or Raft
	SyncEveryCommit SyncMode = iota

	// SyncBytes fsyncs when bytesPerSync bytes have been written (RocksDB-style).
	// - Higher throughput than per-commit fsync
	// - Data loss window: up to bytesPerSync bytes on power failure
	// - Use for: Analytics, caches, high-throughput workloads
	SyncBytes

	// SyncOff disables fsync entirely (testing/bulk loads only).
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	SyncOff
)

// WAL implements Write-Ahead Logging for crash recovery and batched commits
type WAL struct {
	file   *os.File
	mu     sync.Mutex
	offset int64 // Current write position

	// Sync configuration
	syncMode       SyncMode
	bytesPerSync   int
	bytesSinceSync int // Bytes written since last fsync

	// Direct I/O buffer pool
	bufPool *sync.Pool

	// Page tracking - latches prevent stale disk reads
	Pages sync.Map // PageID -> uint64 (txnID) - Pages in WAL but not yet on disk
}

// Record represents a single WAL record
type Record struct {
	Type   uint8
	TxnID  uint64
	PageID base.PageID
	Page   *base.Page
}

// Record types
const (
	RecordPage   uint8 = 1 // Page write
	RecordCommit uint8 = 2 // Commit marker
)

// RecordHeaderSize Record format: [Type:1][TxnID:8][PageID:8][DataLen:4][Data:N]
const RecordHeaderSize = 1 + 8 + 8 + 4

// NewWAL opens or creates a WAL file with the specified sync mode
func NewWAL(path string, syncMode SyncMode, bytesPerSync int) (*WAL, error) {
	// Open wal file with DirectIO support
	file, err := directio.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
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
		file:   file,
		offset: info.Size(),
		bufPool: &sync.Pool{
			New: func() interface{} {
				return directio.AlignedBlock(directio.BlockSize * 2)
			},
		},
		syncMode:       syncMode,
		bytesPerSync:   bytesPerSync,
		bytesSinceSync: 0,
	}, nil
}

// AppendPage writes a Page record to the WAL
// Format: [RecordPage:1][TxnID:8][PageID:8][PageSize:4][Page.data:PageSize][Padding]
// Records are padded to BlockSize*2 (8192) for DirectIO alignment
func (w *WAL) AppendPage(txnID uint64, pageID base.PageID, page *base.Page) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get aligned buffer from pool
	buf := w.bufPool.Get().([]byte)
	defer w.bufPool.Put(buf)

	// Build record header
	buf[0] = RecordPage
	binary.LittleEndian.PutUint64(buf[1:9], txnID)
	binary.LittleEndian.PutUint64(buf[9:17], uint64(pageID))
	binary.LittleEndian.PutUint32(buf[17:21], base.PageSize)

	// Copy page data
	copy(buf[RecordHeaderSize:RecordHeaderSize+base.PageSize], page.Data[:])

	// Write entire aligned buffer (header + page + padding)
	writeSize := directio.BlockSize * 2
	if _, err := w.file.Write(buf[:writeSize]); err != nil {
		return err
	}

	// Update offset and track bytes since sync
	w.offset += int64(writeSize)
	w.bytesSinceSync += writeSize

	// Set latch to prevent stale disk reads
	w.Pages.Store(pageID, txnID)

	return nil
}

// AppendCommit writes a commit marker to the WAL
// Format: [RecordCommit:1][TxnID:8][0:8][0:4][Padding]
// Records are padded to BlockSize (4096) for DirectIO alignment
func (w *WAL) AppendCommit(txnID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get aligned buffer from pool
	buf := w.bufPool.Get().([]byte)
	defer w.bufPool.Put(buf)

	// Build commit record (no data payload)
	buf[0] = RecordCommit
	binary.LittleEndian.PutUint64(buf[1:9], txnID)
	binary.LittleEndian.PutUint64(buf[9:17], 0)
	binary.LittleEndian.PutUint32(buf[17:21], 0)

	// Write aligned buffer (header + padding)
	writeSize := directio.BlockSize
	if _, err := w.file.Write(buf[:writeSize]); err != nil {
		return err
	}

	// Update offset and track bytes since sync
	w.offset += int64(writeSize)
	w.bytesSinceSync += writeSize

	return nil
}

// Sync conditionally fsyncs the WAL based on sync mode configuration
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.syncMode {
	case SyncEveryCommit:
		// Always sync
		return w.syncUnsafe()

	case SyncBytes:
		// Sync if we've exceeded the byte threshold
		if w.bytesSinceSync >= w.bytesPerSync {
			return w.syncUnsafe()
		}
		return nil

	case SyncOff:
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
	uncommitted := make(map[uint64][]Record)

	header := make([]byte, RecordHeaderSize)

	for {
		// Read record header
		n, err := w.file.Read(header)
		if err == io.EOF {
			break // End of wal
		}
		if err != nil {
			return fmt.Errorf("wal replay read error: %w", err)
		}
		if n != RecordHeaderSize {
			return fmt.Errorf("wal replay: short header read: %d bytes", n)
		}

		// Parse header
		recordType := header[0]
		txnID := binary.LittleEndian.Uint64(header[1:9])
		pageID := base.PageID(binary.LittleEndian.Uint64(header[9:17]))
		dataLen := binary.LittleEndian.Uint32(header[17:21])

		switch recordType {
		case RecordPage:
			// Read Page data
			if dataLen != base.PageSize {
				return fmt.Errorf("wal replay: invalid Page size: %d", dataLen)
			}

			page := &base.Page{}
			if _, err := w.file.Read(page.Data[:]); err != nil {
				return fmt.Errorf("wal replay: failed to read Page data: %w", err)
			}

			// Skip padding (BlockSize*2 - RecordHeaderSize - PageSize)
			paddingSize := directio.BlockSize*2 - RecordHeaderSize - base.PageSize
			if _, err := w.file.Seek(int64(paddingSize), io.SeekCurrent); err != nil {
				return fmt.Errorf("wal replay: failed to skip padding: %w", err)
			}

			// store in uncommitted map
			uncommitted[txnID] = append(uncommitted[txnID], Record{
				Type:   RecordPage,
				TxnID:  txnID,
				PageID: pageID,
				Page:   page,
			})

		case RecordCommit:
			// Skip padding (BlockSize - RecordHeaderSize)
			paddingSize := directio.BlockSize - RecordHeaderSize
			if _, err := w.file.Seek(int64(paddingSize), io.SeekCurrent); err != nil {
				return fmt.Errorf("wal replay: failed to skip padding: %w", err)
			}

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

	header := make([]byte, RecordHeaderSize)
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
		if n != RecordHeaderSize {
			return fmt.Errorf("wal truncate: short header read")
		}

		recordType := header[0]
		txnID := binary.LittleEndian.Uint64(header[1:9])

		// Skip data section and padding based on record type
		var skipSize int64
		if recordType == RecordPage {
			// Skip: PageData + Padding = (BlockSize*2 - RecordHeaderSize)
			skipSize = int64(directio.BlockSize*2 - RecordHeaderSize)
		} else if recordType == RecordCommit {
			// Skip: Padding = (BlockSize - RecordHeaderSize)
			skipSize = int64(directio.BlockSize - RecordHeaderSize)
		}

		if skipSize > 0 {
			if _, err := w.file.Seek(skipSize, io.SeekCurrent); err != nil {
				return err
			}
		}

		// Check if this commit is beyond our checkpoint
		if recordType == RecordCommit && txnID > upToTxnID {
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

// CleanupLatch removes WAL Pages latches for Pages that have been checkpoint
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

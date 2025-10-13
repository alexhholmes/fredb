package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"fredb/internal/base"
	"fredb/internal/directio"
)

// Storage provides file I/O for pages
type Storage struct {
	mu   sync.Mutex // Protects file access
	file *os.File
	pool *sync.Pool // Pool of aligned []byte buffers for direct I/O

	// Stats
	reads   atomic.Uint64 // Total disk reads
	writes  atomic.Uint64 // Total disk writes
	read    atomic.Uint64 // Total bytes read
	written atomic.Uint64 // Total bytes written
}

// NewStorage opens or creates a database file
func NewStorage(path string) (*Storage, error) {
	// Use directio.OpenFile - falls back to regular I/O on unsupported platforms
	file, err := directio.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &Storage{
		file: file,
		pool: &sync.Pool{
			New: func() interface{} {
				return directio.AlignedBlock(base.PageSize)
			},
		},
	}, nil
}

// ReadPage reads a page from disk
func (s *Storage) ReadPage(id base.PageID) (*base.Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	offset := int64(id) * base.PageSize
	page := &base.Page{}

	// Get aligned buffer from pool
	buf := s.pool.Get().([]byte)
	defer s.pool.Put(buf)

	s.reads.Add(1)
	n, err := s.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	s.read.Add(uint64(n))
	if n != base.PageSize {
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, base.PageSize)
	}

	// Copy from aligned buffer to page
	copy(page.Data[:], buf)

	return page, nil
}

// WritePage writes a page to disk
func (s *Storage) WritePage(id base.PageID, page *base.Page) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)
	if directio.IsAligned(buf) {
		defer s.pool.Put(buf)
	} else {
		// Buffer was not allocated from the storage aligned buffer pool.
		// We need to copy it to an aligned buffer before writing.
		aligned := s.pool.Get().([]byte)
		copy(aligned, buf)
		buf = aligned
		defer s.pool.Put(aligned)
	}

	offset := int64(id) * base.PageSize
	s.writes.Add(1)
	n, err := s.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	s.written.Add(uint64(n))
	if n != base.PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, base.PageSize)
	}

	return nil
}

func (s *Storage) GetBuffer() []byte {
	return s.pool.Get().([]byte)
}

// Sync buffered writes to disk
func (s *Storage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Sync()
}

// Empty checks if the file is empty (size 0)
func (s *Storage) Empty() (bool, error) {
	info, err := s.file.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() == 0, nil
}

// Stats returns disk I/O statistics
func (s *Storage) Stats() Stats {
	return Stats{
		Reads:   s.reads.Load(),
		Writes:  s.writes.Load(),
		Read:    s.read.Load(),
		Written: s.written.Load(),
	}
}

type Stats struct {
	Reads   uint64
	Writes  uint64
	Read    uint64
	Written uint64
}

// Close closes the file
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

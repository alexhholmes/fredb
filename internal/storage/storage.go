package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/directio"
)

// Storage implements page storage using direct I/O with aligned buffers
type Storage struct {
	file    *os.File
	bufPool sync.Pool

	// Stats counters
	reads   atomic.Uint64
	writes  atomic.Uint64
	read    atomic.Uint64
	written atomic.Uint64
}

// New creates a new storage backend
func New(path string) (*Storage, error) {
	file, err := directio.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &Storage{
		file: file,
		bufPool: sync.Pool{
			New: func() any {
				return directio.AlignedBlock(base.PageSize)
			},
		},
	}, nil
}

// ReadPage reads a page using direct I/O
func (s *Storage) ReadPage(id base.PageID) (*base.Page, error) {
	offset := int64(id) * base.PageSize
	buf := s.bufPool.Get().([]byte)

	s.reads.Add(1)
	n, err := s.file.ReadAt(buf, offset)
	if err != nil {
		s.bufPool.Put(buf)
		return nil, err
	}
	s.read.Add(uint64(n))
	if n != base.PageSize {
		s.bufPool.Put(buf)
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, base.PageSize)
	}

	// Zero-copy: cast aligned buffer to Page
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	return page, nil
}

// WritePage writes a page using direct I/O
func (s *Storage) WritePage(id base.PageID, page *base.Page) error {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)

	if !directio.IsAligned(buf) {
		// Buffer not aligned - copy to aligned buffer
		aligned := directio.AlignedBlock(base.PageSize)
		copy(aligned, buf)
		buf = aligned
	}

	offset := int64(id) * base.PageSize
	s.writes.Add(1)

	n, err := s.file.WriteAt(buf, offset)
	defer s.written.Add(uint64(n))
	if err != nil {
		return err
	}
	if n != base.PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, base.PageSize)
	}

	return nil
}

// WriteAt writes multiple contiguous pages at once
func (s *Storage) WriteAt(id base.PageID, data []byte) error {
	// Check that data is a multiple of PageSize
	if len(data)%base.PageSize != 0 {
		return fmt.Errorf("data size %d is not a multiple of page size %d", len(data), base.PageSize)
	}

	if !directio.IsAligned(data) {
		// Buffer not aligned - copy to aligned buffer
		aligned := directio.AlignedBlock(base.PageSize)
		copy(aligned, data)
		data = aligned
	}

	offset := int64(id) * base.PageSize
	s.writes.Add(1)

	n, err := s.file.WriteAt(data, int64(offset))
	defer s.written.Add(uint64(n))
	if err != nil {
		return err
	}
	if n != len(data) {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, len(data))
	}

	return nil
}

// Sync flushes buffered writes to disk
func (s *Storage) Sync() error {
	return s.file.Sync()
}

// Empty returns whether the file is empty
func (s *Storage) Empty() (bool, error) {
	info, err := s.file.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() == 0, nil
}

// Close closes the file
func (s *Storage) Close() error {
	return s.file.Close()
}

// GetBuffer gets an aligned buffer from the pool
func (s *Storage) GetBuffer() []byte {
	return s.bufPool.Get().([]byte)
}

// PutBuffer returns an aligned buffer to the pool
func (s *Storage) PutBuffer(buf []byte) {
	s.bufPool.Put(buf)
}

// Stats holds I/O statistics
type Stats struct {
	Reads   uint64
	Writes  uint64
	Read    uint64
	Written uint64
}

// Stats returns I/O statistics
func (s *Storage) Stats() Stats {
	return Stats{
		Reads:   s.reads.Load(),
		Writes:  s.writes.Load(),
		Read:    s.read.Load(),
		Written: s.written.Load(),
	}
}

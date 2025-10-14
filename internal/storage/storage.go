package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

	"fredb/internal/base"
	"fredb/internal/directio"
)

type Mode int

const (
	DirectIO Mode = iota
	MMap
)

// Storage provides file I/O for pages
type Storage struct {
	mode    Mode
	file    *os.File
	bufPool sync.Pool

	// Mmap fields
	empty    bool
	mmapData []byte
	mmapSize int64

	// Stats
	reads   atomic.Uint64 // Total disk reads
	writes  atomic.Uint64 // Total disk writes
	read    atomic.Uint64 // Total bytes read
	written atomic.Uint64 // Total bytes written
}

// NewStorage opens or creates a database file
func NewStorage(path string, mode Mode) (*Storage, error) {
	switch mode {
	case DirectIO:
		file, err := directio.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}
		return &Storage{
			file: file,
			mode: mode,
			bufPool: sync.Pool{
				New: func() any {
					return directio.AlignedBlock(base.PageSize)
				},
			},
		}, nil

	case MMap:
		file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return nil, err
		}

		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}

		var empty bool
		size := info.Size()
		if size == 0 {
			// Initialize with 1GB (sparse file)
			size = 1024 * 1024 * 1024
			if err := file.Truncate(size); err != nil {
				file.Close()
				return nil, err
			}
			empty = true
		}

		data, err := syscall.Mmap(int(file.Fd()), 0, int(size),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			file.Close()
			return nil, err
		}

		return &Storage{
			file:     file,
			mode:     mode,
			empty:    empty,
			mmapData: data,
			mmapSize: size,
			bufPool: sync.Pool{
				New: func() any {
					return make([]byte, base.PageSize)
				},
			},
		}, nil
	}

	return nil, fmt.Errorf("unknown storage mode: %d", mode)
}

// ReadPage reads a page from disk
func (s *Storage) ReadPage(id base.PageID) (*base.Page, error) {
	if s.mode == DirectIO {
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

		// Zero-copy the data into the Page struct
		page := (*base.Page)(unsafe.Pointer(&buf[0]))

		return page, nil
	}

	// MMap
	if s.mmapData == nil {
		return nil, fmt.Errorf("storage closed")
	}

	offset := int64(id) * base.PageSize
	if offset+base.PageSize > s.mmapSize {
		return nil, fmt.Errorf("page %d beyond mapped region", id)
	}

	s.reads.Add(1)
	s.read.Add(base.PageSize)

	// Copy from mmap to avoid pointer invalidation on remap
	buf := make([]byte, base.PageSize)
	copy(buf, s.mmapData[offset:offset+base.PageSize])
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	return page, nil
}

// WritePage writes a page to disk
func (s *Storage) WritePage(id base.PageID, page *base.Page) error {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)

	if s.mode == MMap {
		if s.mmapData == nil {
			return fmt.Errorf("storage closed")
		}

		offset := int64(id) * base.PageSize
		if offset+base.PageSize > s.mmapSize {
			if err := s.growMMap(offset + base.PageSize); err != nil {
				return err
			}
		}

		s.writes.Add(1)
		copy(s.mmapData[offset:], buf)
		s.written.Add(base.PageSize)
		return nil
	}

	// DirectIO path
	if !directio.IsAligned(buf) {
		// Buffer was not allocated from the storage aligned buffer pool.
		// We need to copy it to an aligned buffer before writing.
		aligned := directio.AlignedBlock(base.PageSize)
		copy(aligned, buf)
		buf = aligned
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

// growMMap expands the memory-mapped region
func (s *Storage) growMMap(minSize int64) error {
	// Round up to 1GB chunks to reduce remap frequency
	const growthSize = 1024 * 1024 * 1024 // 1GB
	newSize := ((minSize + growthSize - 1) / growthSize) * growthSize

	// Start async flush to reduce munmap blocking time
	_ = unix.Msync(s.mmapData, unix.MS_ASYNC)

	// Unmap old region
	if err := syscall.Munmap(s.mmapData); err != nil {
		return err
	}

	// Grow file (sparse allocation)
	if err := s.file.Truncate(newSize); err != nil {
		return err
	}

	// Remap with new size
	data, err := syscall.Mmap(int(s.file.Fd()), 0, int(newSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return err
	}

	s.mmapData = data
	s.mmapSize = newSize
	return nil
}

// Sync buffered writes to disk
func (s *Storage) Sync() error {
	if s.mode == MMap {
		// For mmap, sync the mapped region to disk
		if err := unix.Msync(s.mmapData, unix.MS_SYNC); err != nil {
			return err
		}
	}
	return s.file.Sync()
}

// Empty checks if the file is empty or if mmap size is zero
func (s *Storage) Empty() (bool, error) {
	if s.mode == MMap {
		return s.empty, nil
	}
	info, err := s.file.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() == 0, nil
}

func (s *Storage) GetBuffer() []byte {
	return s.bufPool.Get().([]byte)
}

func (s *Storage) PutBuffer(buf []byte) {
	s.bufPool.Put(buf)
}

// GetMode returns the storage mode
func (s *Storage) GetMode() Mode {
	return s.mode
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
	if s.mode == MMap && s.mmapData != nil {
		if err := syscall.Munmap(s.mmapData); err != nil {
			return err
		}
		s.mmapData = nil
	}
	return s.file.Close()
}

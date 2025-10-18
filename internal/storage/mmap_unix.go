// mmap_unix.go
//go:build linux || darwin

package storage

import (
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

	"fredb/internal/base"
)

// MMap implements Storage using memory-mapped I/O
type MMap struct {
	file     *os.File
	mmapData []byte
	mmapSize int64
	empty    bool

	// Stats counters
	reads   atomic.Uint64
	writes  atomic.Uint64
	read    atomic.Uint64
	written atomic.Uint64
}

// NewMMap creates a new memory-mapped storage backend
func NewMMap(path string) (*MMap, error) {
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

	return &MMap{
		file:     file,
		mmapData: data,
		mmapSize: size,
		empty:    empty,
	}, nil
}

// ReadPage reads a page from the memory-mapped region
func (m *MMap) ReadPage(id base.PageID) (*base.Page, error) {
	if m.mmapData == nil {
		return nil, fmt.Errorf("storage closed")
	}

	offset := int64(id) * base.PageSize
	if offset+base.PageSize > m.mmapSize {
		return nil, fmt.Errorf("page %d beyond mapped region", id)
	}

	m.reads.Add(1)
	m.read.Add(base.PageSize)

	// Copy from mmap to avoid pointer invalidation on remap
	buf := make([]byte, base.PageSize)
	copy(buf, m.mmapData[offset:offset+base.PageSize])
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	return page, nil
}

// WritePage writes a page to the memory-mapped region
func (m *MMap) WritePage(id base.PageID, page *base.Page) error {
	if m.mmapData == nil {
		return fmt.Errorf("storage closed")
	}

	buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)

	offset := int64(id) * base.PageSize
	if offset+base.PageSize > m.mmapSize {
		// Grow mmap region
		minSize := offset + base.PageSize

		// Round up to 1GB chunks to reduce remap frequency
		const growthSize = 1024 * 1024 * 1024 // 1GB
		newSize := ((minSize + growthSize - 1) / growthSize) * growthSize

		// Start async flush to reduce munmap blocking time
		_ = unix.Msync(m.mmapData, unix.MS_ASYNC)

		// Unmap old region
		if err := syscall.Munmap(m.mmapData); err != nil {
			return err
		}

		// Grow file (sparse allocation)
		if err := m.file.Truncate(newSize); err != nil {
			return err
		}

		// Remap with new size
		data, err := syscall.Mmap(int(m.file.Fd()), 0, int(newSize),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return err
		}

		m.mmapData = data
		m.mmapSize = newSize
	}

	m.writes.Add(1)
	copy(m.mmapData[offset:], buf)
	m.written.Add(base.PageSize)
	return nil
}

// WritePageRange writes a contiguous range of pages to the memory-mapped region
func (m *MMap) WritePageRange(startID base.PageID, buffer []byte) error {
	if m.mmapData == nil {
		return fmt.Errorf("storage closed")
	}

	if len(buffer)%base.PageSize != 0 {
		return fmt.Errorf("buffer size %d not multiple of page size %d", len(buffer), base.PageSize)
	}

	offset := int64(startID) * base.PageSize
	endOffset := offset + int64(len(buffer))

	// Check if we need to grow the mmap region
	if endOffset > m.mmapSize {
		minSize := endOffset

		// Round up to 1GB chunks to reduce remap frequency
		const growthSize = 1024 * 1024 * 1024 // 1GB
		newSize := ((minSize + growthSize - 1) / growthSize) * growthSize

		// Start async flush to reduce munmap blocking time
		_ = unix.Msync(m.mmapData, unix.MS_ASYNC)

		// Unmap old region
		if err := syscall.Munmap(m.mmapData); err != nil {
			return err
		}

		// Grow file (sparse allocation)
		if err := m.file.Truncate(newSize); err != nil {
			return err
		}

		// Remap with new size
		data, err := syscall.Mmap(int(m.file.Fd()), 0, int(newSize),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err != nil {
			return err
		}

		m.mmapData = data
		m.mmapSize = newSize
	}

	pageCount := len(buffer) / base.PageSize
	m.writes.Add(uint64(pageCount))
	copy(m.mmapData[offset:], buffer)
	m.written.Add(uint64(len(buffer)))

	return nil
}

// Sync flushes the memory-mapped region to disk
func (m *MMap) Sync() error {
	if err := unix.Msync(m.mmapData, unix.MS_SYNC); err != nil {
		return err
	}
	return m.file.Sync()
}

// Empty returns whether this is a newly created database
func (m *MMap) Empty() (bool, error) {
	return m.empty, nil
}

// Stats returns I/O statistics
func (m *MMap) Stats() Stats {
	return Stats{
		Reads:   m.reads.Load(),
		Writes:  m.writes.Load(),
		Read:    m.read.Load(),
		Written: m.written.Load(),
	}
}

// Close unmaps the region and closes the file
func (m *MMap) Close() error {
	if m.mmapData != nil {
		if err := syscall.Munmap(m.mmapData); err != nil {
			return err
		}
		m.mmapData = nil
	}
	return m.file.Close()
}

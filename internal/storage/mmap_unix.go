// mmap_unix.go
//go:build linux || darwin

package storage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/alexhholmes/fredb/internal/base"
)

// MMap implements Storage using memory-mapped I/O
type MMap struct {
	mu       sync.RWMutex
	file     *os.File
	mmapData []byte // PROT_READ only
	fileSize int64  // actual file size
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
		// Initialize with 4 pages (2 meta + freelist + root)
		size = 4 * base.PageSize
		if err := file.Truncate(size); err != nil {
			file.Close()
			return nil, err
		}
		empty = true
	}

	// Mmap read-only
	data, err := syscall.Mmap(int(file.Fd()), 0, int(size),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Advise the kernel that the mmap is accessed randomly.
	err = unix.Madvise(data, syscall.MADV_RANDOM)
	if err != nil && !errors.Is(err, syscall.ENOSYS) {
		// Ignore not implemented error in kernel because it still works.
		return nil, fmt.Errorf("madvise: %s", err)
	}

	return &MMap{
		file:     file,
		mmapData: data,
		fileSize: size,
		empty:    empty,
	}, nil
}

// ReadPage reads a page from the memory-mapped region
func (m *MMap) ReadPage(id base.PageID) (*base.Page, error) {
	if m.mmapData == nil {
		return nil, fmt.Errorf("storage closed")
	}

	offset := int64(id) * base.PageSize
	if offset+base.PageSize > m.fileSize {
		return nil, fmt.Errorf("page %d beyond mapped region", id)
	}

	m.reads.Add(1)
	m.read.Add(base.PageSize)

	// Copy from mmap to avoid pointer invalidation on remap
	buf := make([]byte, base.PageSize)

	m.mu.RLock()
	copy(buf, m.mmapData[offset:offset+base.PageSize])
	m.mu.RUnlock()
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	return page, nil
}

// WritePage writes a page via WriteAt syscall
func (m *MMap) WritePage(id base.PageID, page *base.Page) error {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)
	offset := int64(id) * base.PageSize

	m.mu.Lock()
	defer m.mu.Unlock()

	// Grow file if needed
	if offset+base.PageSize > m.fileSize {
		newSize := m.growSize(offset + base.PageSize)
		if err := m.file.Truncate(newSize); err != nil {
			return err
		}
		m.fileSize = newSize

		// Remap read-only with new size
		if err := m.remapReadOnly(newSize); err != nil {
			return err
		}
	}

	// Write via syscall, not mmap
	m.writes.Add(1)
	_, err := m.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	m.written.Add(base.PageSize)
	return nil
}

// growSize calculates new file size using BBolt's strategy
func (m *MMap) growSize(minSize int64) int64 {
	// Powers of 2: 32KB → 1GB
	size := int64(32 * 1024)
	for size < minSize && size < 1024*1024*1024 {
		size *= 2
	}
	// Then 1GB chunks
	if size < minSize {
		const gb = 1024 * 1024 * 1024
		size = ((minSize + gb - 1) / gb) * gb
	}
	return size
}

// remapReadOnly unmaps and remaps the region with new size
func (m *MMap) remapReadOnly(newSize int64) error {
	if err := syscall.Munmap(m.mmapData); err != nil {
		return err
	}
	data, err := syscall.Mmap(int(m.file.Fd()), 0, int(newSize),
		syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return err
	}
	m.mmapData = data
	return nil
}

// Sync flushes writes to disk
func (m *MMap) Sync() error {
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

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

// DirectIO implements Storage using direct I/O with aligned buffers
type DirectIO struct {
	file    *os.File
	bufPool sync.Pool

	// Stats counters
	reads   atomic.Uint64
	writes  atomic.Uint64
	read    atomic.Uint64
	written atomic.Uint64
}

// NewDirectIO creates a new direct I/O storage backend
func NewDirectIO(path string) (*DirectIO, error) {
	file, err := directio.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}

	return &DirectIO{
		file: file,
		bufPool: sync.Pool{
			New: func() any {
				return directio.AlignedBlock(base.PageSize)
			},
		},
	}, nil
}

// ReadPage reads a page using direct I/O
func (d *DirectIO) ReadPage(id base.PageID) (*base.Page, error) {
	offset := int64(id) * base.PageSize
	buf := d.bufPool.Get().([]byte)

	d.reads.Add(1)
	n, err := d.file.ReadAt(buf, offset)
	if err != nil {
		d.bufPool.Put(buf)
		return nil, err
	}
	d.read.Add(uint64(n))
	if n != base.PageSize {
		d.bufPool.Put(buf)
		return nil, fmt.Errorf("short read: got %d bytes, expected %d", n, base.PageSize)
	}

	// Zero-copy: cast aligned buffer to Page
	page := (*base.Page)(unsafe.Pointer(&buf[0]))
	return page, nil
}

// WritePage writes a page using direct I/O
func (d *DirectIO) WritePage(id base.PageID, page *base.Page) error {
	buf := unsafe.Slice((*byte)(unsafe.Pointer(page)), base.PageSize)

	if !directio.IsAligned(buf) {
		// Buffer not aligned - copy to aligned buffer
		aligned := directio.AlignedBlock(base.PageSize)
		copy(aligned, buf)
		buf = aligned
	}

	offset := int64(id) * base.PageSize
	d.writes.Add(1)
	n, err := d.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	d.written.Add(uint64(n))
	if n != base.PageSize {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, base.PageSize)
	}

	return nil
}

// WritePageRange writes a contiguous range of pages in a single syscall
func (d *DirectIO) WritePageRange(startID base.PageID, buffer []byte) error {
	if len(buffer)%base.PageSize != 0 {
		return fmt.Errorf("buffer size %d not multiple of page size %d", len(buffer), base.PageSize)
	}

	// Ensure buffer is aligned for direct I/O (only required on platforms with alignment requirements)
	alignedBuf := buffer
	if !directio.IsAligned(buffer) {
		// Buffer not aligned - copy to aligned buffer
		alignedBuf = directio.AlignedBlock(len(buffer))
		copy(alignedBuf, buffer)
	}

	offset := int64(startID) * base.PageSize
	pageCount := len(buffer) / base.PageSize

	d.writes.Add(uint64(pageCount))
	n, err := d.file.WriteAt(alignedBuf, offset)
	if err != nil {
		return err
	}
	d.written.Add(uint64(n))

	if n != len(buffer) {
		return fmt.Errorf("short write: wrote %d bytes, expected %d", n, len(buffer))
	}

	return nil
}

// Sync flushes buffered writes to disk
func (d *DirectIO) Sync() error {
	return d.file.Sync()
}

// Empty returns whether the file is empty
func (d *DirectIO) Empty() (bool, error) {
	info, err := d.file.Stat()
	if err != nil {
		return false, err
	}
	return info.Size() == 0, nil
}

// Stats returns I/O statistics
func (d *DirectIO) Stats() Stats {
	return Stats{
		Reads:   d.reads.Load(),
		Writes:  d.writes.Load(),
		Read:    d.read.Load(),
		Written: d.written.Load(),
	}
}

// Close closes the file
func (d *DirectIO) Close() error {
	return d.file.Close()
}

func (d *DirectIO) GetBuffer() []byte {
	return d.bufPool.Get().([]byte)
}

func (d *DirectIO) PutBuffer(buf []byte) {
	d.bufPool.Put(buf)
}

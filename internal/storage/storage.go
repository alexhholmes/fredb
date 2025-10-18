package storage

import (
	"github.com/alexhholmes/fredb/internal/base"
)

// Storage provides page-based I/O interface
type Storage interface {
	ReadPage(id base.PageID) (*base.Page, error)
	WritePage(id base.PageID, page *base.Page) error
	Sync() error
	Empty() (bool, error)
	Close() error
	Stats() Stats
}

// Stats tracks disk I/O metrics
type Stats struct {
	Reads   uint64 // Total read operations
	Writes  uint64 // Total write operations
	Read    uint64 // Total bytes read
	Written uint64 // Total bytes written
}

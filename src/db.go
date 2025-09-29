package src

import (
	"errors"
	"sync"
)

var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrPageOverflow       = errors.New("page overflow: serialized data exceeds page size")
	ErrInvalidOffset      = errors.New("invalid offset: out of bounds")
	ErrInvalidMagicNumber = errors.New("invalid magic number")
	ErrInvalidVersion     = errors.New("invalid format version")
	ErrInvalidPageSize    = errors.New("invalid page size")
	ErrInvalidChecksum    = errors.New("invalid checksum")
)

type DB interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	Close() error
}

var _ DB = (*db)(nil)

type db struct {
	mu    sync.RWMutex
	store *BTree
}

func NewDB(path string) (DB, error) {
	pager, err := NewDiskPageManager(path)
	if err != nil {
		return nil, err
	}

	btree, err := NewBTree(pager)
	if err != nil {
		return nil, err
	}

	return &db{
		store: btree,
	}, nil
}

func (d *db) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.store.Get(key)
}

func (d *db) Set(key, value []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.store.Set(key, value)
}

func (d *db) Delete(key []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.store.Delete(key)
}

func (d *db) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.store.Close()
}

package src

import (
	"errors"
	"sync"
)

var ErrKeyNotFound = errors.New("key not found")

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
	// For now, use in-memory pager (path ignored)
	pager := NewInMemoryPageManager()

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

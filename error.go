package fredb

import (
	"errors"

	"fredb/internal/base"
)

//goland:noinspection GoUnusedGlobalVariable
var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrDatabaseClosed = errors.New("database is closed")
	ErrKeyEmpty       = errors.New("key cannot be empty")
	ErrKeyTooLarge    = errors.New("key too large")
	ErrValueTooLarge  = errors.New("value too large")
	ErrCorruption     = errors.New("data corruption detected")

	ErrTxNotWritable = errors.New("transaction is read-only")
	ErrTxInProgress  = errors.New("write transaction already in progress")
	ErrTxDone        = errors.New("transaction has been committed or rolled back")
	ErrNoActiveTx    = errors.New("no active write transaction")

	ErrBucketExists   = errors.New("bucket already exists")
	ErrBucketNotFound = errors.New("bucket not found")

	ErrKeysUnsorted          = errors.New("keys must be inserted in strictly ascending order")
	ErrBulkLoaderEmpty       = errors.New("bulk loader is empty")
	ErrBulkLoaderMultipleRoots = errors.New("failed to build tree: multiple roots remaining")

	ErrPageOverflow       = base.ErrPageOverflow
	ErrInvalidOffset      = base.ErrInvalidOffset
	ErrInvalidMagicNumber = base.ErrInvalidMagicNumber
	ErrInvalidVersion     = base.ErrInvalidVersion
	ErrInvalidPageSize    = base.ErrInvalidPageSize
	ErrInvalidChecksum    = base.ErrInvalidChecksum
)

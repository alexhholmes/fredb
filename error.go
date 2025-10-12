package fredb

import (
	"errors"

	"fredb/internal/base"
)

//goland:noinspection GoUnusedGlobalVariable
var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrDatabaseClosed = errors.New("database is closed")
	ErrKeyTooLarge    = errors.New("key too large")
	ErrValueTooLarge  = errors.New("value too large")
	ErrCorruption     = errors.New("data corruption detected")

	ErrTxNotWritable = errors.New("transaction is read-only")
	ErrTxInProgress  = errors.New("write transaction already in progress")
	ErrTxDone        = errors.New("transaction has been committed or rolled back")

	ErrPageOverflow       = base.ErrPageOverflow
	ErrInvalidOffset      = base.ErrInvalidOffset
	ErrInvalidMagicNumber = base.ErrInvalidMagicNumber
	ErrInvalidVersion     = base.ErrInvalidVersion
	ErrInvalidPageSize    = base.ErrInvalidPageSize
	ErrInvalidChecksum    = base.ErrInvalidChecksum

	ErrBucketExists   = errors.New("bucket already exists")
	ErrBucketNotFound = errors.New("bucket not found")
)

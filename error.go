package fredb

import (
	"errors"

	"github.com/alexhholmes/fredb/internal/base"
	"github.com/alexhholmes/fredb/internal/lifecycle"
)

//goland:noinspection GoUnusedGlobalVariable
var (
	ErrKeyNotFound       = errors.New("key not found")
	ErrDatabaseClosed    = errors.New("database is closed")
	ErrDatabaseExists    = errors.New("database already exists at path")
	ErrKeyTooLarge       = errors.New("key too large")
	ErrValueTooLarge     = errors.New("value too large")
	ErrCorruption        = errors.New("data corruption detected")
	ErrInvalidMaxReaders = errors.New("invalid max readers, must be greater than zero")

	ErrTxNotWritable = errors.New("transaction is read-only")
	ErrTxInProgress  = errors.New("write transaction already in progress")
	ErrTxDone        = errors.New("transaction has been committed or rolled back")

	ErrBucketExists   = errors.New("bucket already exists")
	ErrBucketNotFound = errors.New("bucket not found")

	ErrTooManyReaders = lifecycle.ErrTooManyReaders

	ErrPageOverflow       = base.ErrPageOverflow
	ErrInvalidOffset      = base.ErrInvalidOffset
	ErrInvalidMagicNumber = base.ErrInvalidMagicNumber
	ErrInvalidVersion     = base.ErrInvalidVersion
	ErrInvalidPageSize    = base.ErrInvalidPageSize
	ErrInvalidChecksum    = base.ErrInvalidChecksum
)

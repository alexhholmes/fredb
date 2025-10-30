package base

import "errors"

var (
	ErrInvalidOffset      = errors.New("invalid offset: out of bounds")
	ErrInvalidMagicNumber = errors.New("invalid magic number")
	ErrInvalidVersion     = errors.New("invalid format version")
	ErrInvalidPageSize    = errors.New("invalid Page Size")
	ErrInvalidChecksum    = errors.New("invalid checksum")
	ErrPageOverflow       = errors.New("page overflow")
)

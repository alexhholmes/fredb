// Package directio provides open file functions that bypass the OS buffer.
// This is adapted from https://github.com/ncw/directio.
package directio

import (
	"log"
	"unsafe"
)

// IsAligned checks whether passed byte slice is aligned
func IsAligned(block []byte) bool {
	return alignment(block, AlignSize) == 0
}

// AlignedBlock returns []byte of size BlockSize aligned to a multiple
// of AlignSize in memory (must be power of two)
func AlignedBlock(BlockSize int) []byte {
	block := make([]byte, BlockSize+AlignSize)
	if AlignSize == 0 {
		return block
	}
	a := alignment(block, AlignSize)
	offset := 0
	if a != 0 {
		offset = AlignSize - a
	}
	block = block[offset : offset+BlockSize]
	// Can't check alignment of a zero sized block
	if BlockSize != 0 {
		if !IsAligned(block) {
			log.Fatal("Failed to align block")
		}
	}
	return block
}

// alignment returns alignment of the block in memory
// with reference to AlignSize
//
// Can't check alignment of a zero sized block as &block[0] is invalid
func alignment(block []byte, AlignSize int) int {
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(AlignSize-1))
}

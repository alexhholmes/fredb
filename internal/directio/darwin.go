//go:build darwin

package directio

import (
	"fmt"
	"os"
	"syscall"
)

const (
	AlignSize = 0
	BlockSize = 4096
	DirectIO  = true
)

func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	file, err = os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}

	// Insert F_NOCACHE to avoid OS caching
	_, _, e1 := syscall.Syscall(syscall.SYS_FCNTL, uintptr(file.Fd()), syscall.F_NOCACHE, 1)
	if e1 != 0 {
		err = fmt.Errorf("Failed to set F_NOCACHE: %s", e1)
		file.Close()
		file = nil
	}

	return
}

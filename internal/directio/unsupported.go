//go:build windows || openbsd || plan9

package directio

const (
	AlignSize = 0
	BlockSize = 4096
	DirectIO  = false
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT.
func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, flag, perm)
}

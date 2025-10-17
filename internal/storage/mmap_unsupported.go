// mmap_unsupported.go
//go:build !linux && !darwin

package storage

// On unsupported platforms, MMap falls back to DirectIO
type MMap struct {
	*DirectIO
}

func NewMMap(path string) (*MMap, error) {
	dio, err := NewDirectIO(path)
	if err != nil {
		return nil, err
	}
	return &MMap{DirectIO: dio}, nil
}

// All methods automatically delegate to DirectIO

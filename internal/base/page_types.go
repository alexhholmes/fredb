package base

import (
	"bytes"
)

const (
	// Page type flags - reuse existing flags
	LeafPageFlag     uint16 = 0x01
	BranchPageFlag   uint16 = 0x02
	OverflowPageFlag uint16 = 0x04
)

// Clone creates a shallow copy of LeafPage for copy-on-write
func (p *LeafPage) Clone() *LeafPage {
	cloned := NewLeafPage()
	cloned.Header = p.Header
	cloned.Header.PageID = 0 // New page needs ID allocation
	cloned.Header.NumKeys = uint16(len(p.Keys)) // Sync with actual Keys length
	cloned.Elements = append([]LeafElement{}, p.Elements...)
	cloned.Keys = append([][]byte{}, p.Keys...)
	cloned.Values = append([][]byte{}, p.Values...)
	cloned.RebuildIndirectSlices() // Pack into new buffer
	return cloned
}

// Clone creates a shallow copy of BranchPage for copy-on-write
func (p *BranchPage) Clone() *BranchPage {
	cloned := NewBranchPage()
	cloned.Header = p.Header
	cloned.Header.PageID = 0 // New page needs ID allocation
	cloned.Header.NumKeys = uint16(len(p.Keys)) // Sync with actual Keys length
	cloned.Elements = append([]BranchElement{}, p.Elements...)
	cloned.Keys = append([][]byte{}, p.Keys...)
	cloned.FirstChild = p.FirstChild
	cloned.RebuildIndirectSlices() // Pack into new buffer
	return cloned
}

// Clone creates a shallow copy of OverflowPage for copy-on-write
func (p *OverflowPage) Clone() *OverflowPage {
	cloned := NewOverflowPage()
	cloned.Header = p.Header
	cloned.Header.PageID = 0 // New page needs ID allocation
	cloned.Data = append([]byte{}, p.Data...)
	return cloned
}

// Children returns all child page IDs for BranchPage
func (p *BranchPage) Children() []PageID {
	children := make([]PageID, p.Header.NumKeys+1)
	children[0] = p.FirstChild
	for i := 0; i < int(p.Header.NumKeys); i++ {
		children[i+1] = p.Elements[i].ChildID
	}
	return children
}

// FindKey returns the index of key in page, or -1 if not found
func FindKey(keys [][]byte, key []byte) int {
	for i := 0; i < len(keys); i++ {
		cmp := bytes.Compare(key, keys[i])
		if cmp == 0 {
			return i
		}
		if cmp < 0 {
			return -1
		}
	}
	return -1
}

// IsFull checks if a LeafPage would overflow with new key-value pair
func (p *LeafPage) IsFull(key, value []byte) bool {
	currentSize := PageHeaderSize + len(p.Elements)*LeafElementSize
	for _, k := range p.Keys {
		currentSize += len(k)
	}
	for _, v := range p.Values {
		currentSize += len(v)
	}
	projectedSize := currentSize + LeafElementSize + len(key) + len(value)
	return projectedSize > PageSize
}

// IsFull checks if a BranchPage would overflow with new key
func (p *BranchPage) IsFull(key []byte) bool {
	currentSize := PageHeaderSize + len(p.Elements)*BranchElementSize + 8 // +8 for FirstChild
	for _, k := range p.Keys {
		currentSize += len(k)
	}
	projectedSize := currentSize + BranchElementSize + len(key)
	return projectedSize > PageSize
}

// IsUnderflow checks if page has insufficient fill ratio (doesn't apply to root)
func (p *LeafPage) IsUnderflow() bool {
	minSize := int(float64(PageSize) * MinFillRatio)
	currentSize := PageHeaderSize + len(p.Elements)*LeafElementSize
	for _, k := range p.Keys {
		currentSize += len(k)
	}
	for _, v := range p.Values {
		currentSize += len(v)
	}
	return currentSize < minSize
}

// IsUnderflow checks if page has insufficient fill ratio (doesn't apply to root)
func (p *BranchPage) IsUnderflow() bool {
	minSize := int(float64(PageSize) * MinFillRatio)
	currentSize := PageHeaderSize + len(p.Elements)*BranchElementSize + 8
	for _, k := range p.Keys {
		currentSize += len(k)
	}
	return currentSize < minSize
}

// Size calculates the size of the serialized LeafPage
func (p *LeafPage) Size() int {
	size := PageHeaderSize + len(p.Elements)*LeafElementSize
	for _, k := range p.Keys {
		size += len(k)
	}
	for _, v := range p.Values {
		size += len(v)
	}
	return size
}

// Size calculates the size of the serialized BranchPage
func (p *BranchPage) Size() int {
	size := PageHeaderSize + len(p.Elements)*BranchElementSize + 8
	for _, k := range p.Keys {
		size += len(k)
	}
	return size
}

// CheckOverflow validates page doesn't exceed PageSize
func (p *LeafPage) CheckOverflow() error {
	if p.Size() > PageSize {
		return ErrPageOverflow
	}
	return nil
}

// CheckOverflow validates page doesn't exceed PageSize
func (p *BranchPage) CheckOverflow() error {
	if p.Size() > PageSize {
		return ErrPageOverflow
	}
	return nil
}
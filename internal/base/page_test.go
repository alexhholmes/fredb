package base

import (
	"flag"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = flag.Bool("slow", false, "run slow tests")

func TestPageHeaderAlignment(t *testing.T) {
	t.Parallel()

	// Verify struct sizes match expectations (no padding)
	assert.Equal(t, uintptr(8), unsafe.Sizeof(PageID(0)), "PageID Size")
	assert.Equal(t, uintptr(40), unsafe.Sizeof(PageHeader{}), "PageHeader Size")

	// Verify field offsets (no padding needed)
	var h PageHeader
	assert.Equal(t, uintptr(0), unsafe.Offsetof(h.PageID), "PageID offset")
	assert.Equal(t, uintptr(8), unsafe.Offsetof(h.Flags), "Flags offset")
	assert.Equal(t, uintptr(10), unsafe.Offsetof(h.NumKeys), "NumKeys offset")
	assert.Equal(t, uintptr(12), unsafe.Offsetof(h.Padding), "Padding offset")
	assert.Equal(t, uintptr(16), unsafe.Offsetof(h.TxnID), "TxID offset")
	assert.Equal(t, uintptr(24), unsafe.Offsetof(h._NextLeaf), "_NextLeaf offset")
	assert.Equal(t, uintptr(32), unsafe.Offsetof(h._PrevLeaf), "_PrevLeaf offset")
}

func TestPageHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header
	writeHdr := PageHeader{
		PageID:  42,
		Flags:   LeafPageFlag,
		NumKeys: 10,
		Padding: 99,
		TxnID:   123,
	}
	page.WriteHeader(&writeHdr)

	// Read back using unsafe pointer
	readHdr := page.Header()

	assert.Equal(t, writeHdr.PageID, readHdr.PageID, "PageID")
	assert.Equal(t, writeHdr.Flags, readHdr.Flags, "Flags")
	assert.Equal(t, writeHdr.NumKeys, readHdr.NumKeys, "NumKeys")
	assert.Equal(t, writeHdr.Padding, readHdr.Padding, "Padding")
	assert.Equal(t, writeHdr.TxnID, readHdr.TxnID, "TxID")
}

func TestPageHeaderByteLayout(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header with known Values
	hdr := PageHeader{
		PageID:    0x0123456789ABCDEF, // 8 bytes
		Flags:     0x1234,             // 2 bytes
		NumKeys:   0x5678,             // 2 bytes
		Padding:   0x9ABCDEF0,         // 4 bytes
		TxnID:     0x1122334455667788, // 8 bytes
		_NextLeaf: 0xFEDCBA9876543210, // 8 bytes
		_PrevLeaf: 0x0011223344556677, // 8 bytes
	}
	page.WriteHeader(&hdr)

	// Verify actual byte layout (little-endian, no padding)
	expected := []byte{
		// PageID (8 bytes, little-endian)
		0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01,
		// Flags (2 bytes, little-endian)
		0x34, 0x12,
		// NumKeys (2 bytes, little-endian)
		0x78, 0x56,
		// Padding (4 bytes, little-endian)
		0xF0, 0xDE, 0xBC, 0x9A,
		// TxID (8 bytes, little-endian)
		0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
		// _NextLeaf (8 bytes, little-endian)
		0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE,
		// _PrevLeaf (8 bytes, little-endian)
		0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
	}

	for i, expectedByte := range expected {
		assert.Equal(t, expectedByte, page.Data[i], "byte[%d]", i)
	}
}

func TestLeafElementAlignment(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uintptr(12), unsafe.Sizeof(LeafElement{}), "LeafElement Size")
}

func TestLeafElementByteLayout(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header first (need valid Header for element writes)
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	// Write leaf element with known Values
	elem := LeafElement{
		KeyOffset:   0x1234,     // 2 bytes
		KeySize:     0x5678,     // 2 bytes
		ValueOffset: 0x9ABC,     // 2 bytes
		ValueSize:   0xDEF0,     // 2 bytes
		Reserved:    0x12345678, // 4 bytes
	}
	page.WriteLeafElement(0, &elem)

	// Verify actual byte layout starting at PageHeaderSize (little-endian)
	offset := PageHeaderSize
	expected := []byte{
		// KeyOffset (2 bytes, little-endian)
		0x34, 0x12,
		// KeySize (2 bytes, little-endian)
		0x78, 0x56,
		// ValueOffset (2 bytes, little-endian)
		0xBC, 0x9A,
		// ValueSize (2 bytes, little-endian)
		0xF0, 0xDE,
		// Reserved (4 bytes, little-endian)
		0x78, 0x56, 0x34, 0x12,
	}

	for i, expectedByte := range expected {
		assert.Equal(t, expectedByte, page.Data[offset+i], "byte[%d]", offset+i)
	}
}

func TestLeafElementRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header first
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 2,
	}
	page.WriteHeader(&header)

	// Write leaf elements
	elem1 := LeafElement{
		KeyOffset:   0,
		KeySize:     5,
		ValueOffset: 5,
		ValueSize:   10,
		Reserved:    0,
	}
	page.WriteLeafElement(0, &elem1)

	elem2 := LeafElement{
		KeyOffset:   15,
		KeySize:     3,
		ValueOffset: 18,
		ValueSize:   7,
		Reserved:    0,
	}
	page.WriteLeafElement(1, &elem2)

	// Read back using unsafe pointer
	elements := page.LeafElements()

	require.Len(t, elements, 2)

	assert.Equal(t, elem1.KeyOffset, elements[0].KeyOffset, "elem[0].KeyOffset")
	assert.Equal(t, elem1.KeySize, elements[0].KeySize, "elem[0].KeySize")
	assert.Equal(t, elem1.ValueOffset, elements[0].ValueOffset, "elem[0].ValueOffset")
	assert.Equal(t, elem1.ValueSize, elements[0].ValueSize, "elem[0].ValueSize")

	assert.Equal(t, elem2.KeyOffset, elements[1].KeyOffset, "elem[1].KeyOffset")
	assert.Equal(t, elem2.KeySize, elements[1].KeySize, "elem[1].KeySize")
}

func TestBranchElementAlignment(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uintptr(16), unsafe.Sizeof(BranchElement{}), "BranchElement Size")
}

func TestBranchElementByteLayout(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header first (need valid Header for element writes)
	header := PageHeader{
		PageID:  1,
		Flags:   BranchPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	// Write branch element with known Values (B+ tree: no Values in branches)
	elem := BranchElement{
		KeyOffset: 0x1234,             // 2 bytes
		KeySize:   0x5678,             // 2 bytes
		Reserved:  0x9ABCDEF0,         // 4 bytes
		ChildID:   0x0123456789ABCDEF, // 8 bytes
	}
	page.WriteBranchElement(0, &elem)

	// Verify actual byte layout starting at PageHeaderSize (little-endian)
	offset := PageHeaderSize
	expected := []byte{
		// KeyOffset (2 bytes, little-endian)
		0x34, 0x12,
		// KeySize (2 bytes, little-endian)
		0x78, 0x56,
		// Reserved (4 bytes, little-endian)
		0xF0, 0xDE, 0xBC, 0x9A,
		// ChildID (8 bytes, little-endian)
		0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01,
	}

	for i, expectedByte := range expected {
		assert.Equal(t, expectedByte, page.Data[offset+i], "byte[%d]", offset+i)
	}
}

func TestBranchElementRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header first
	header := PageHeader{
		PageID:  1,
		Flags:   BranchPageFlag,
		NumKeys: 2,
	}
	page.WriteHeader(&header)

	// Write branch elements (B+ tree: no Values in branches)
	elem1 := BranchElement{
		KeyOffset: 8,
		KeySize:   5,
		Reserved:  0,
		ChildID:   100,
	}
	page.WriteBranchElement(0, &elem1)

	elem2 := BranchElement{
		KeyOffset: 13,
		KeySize:   3,
		Reserved:  0,
		ChildID:   200,
	}
	page.WriteBranchElement(1, &elem2)

	// Read back using unsafe pointer
	elements := page.BranchElements()

	require.Len(t, elements, 2)

	assert.Equal(t, elem1.KeyOffset, elements[0].KeyOffset, "elem[0].KeyOffset")
	assert.Equal(t, elem1.KeySize, elements[0].KeySize, "elem[0].KeySize")
	assert.Equal(t, elem1.ChildID, elements[0].ChildID, "elem[0].ChildID")

	assert.Equal(t, elem2.KeyOffset, elements[1].KeyOffset, "elem[1].KeyOffset")
	assert.Equal(t, elem2.KeySize, elements[1].KeySize, "elem[1].KeySize")
	assert.Equal(t, elem2.ChildID, elements[1].ChildID, "elem[1].ChildID")
}

func TestBranchFirstChildRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header
	header := PageHeader{
		PageID:  1,
		Flags:   BranchPageFlag,
		NumKeys: 0,
	}
	page.WriteHeader(&header)

	// Write first child
	childID := PageID(42)
	page.WriteBranchFirstChild(childID)

	// Read back
	readChildID := page.ReadBranchFirstChild()

	assert.Equal(t, childID, readChildID, "ChildID")
}

func TestDataAreaStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		flags    uint16
		numKeys  uint16
		expected int
	}{
		{
			name:     "leaf with 0 Keys",
			flags:    LeafPageFlag,
			numKeys:  0,
			expected: PageHeaderSize,
		},
		{
			name:     "leaf with 10 Keys",
			flags:    LeafPageFlag,
			numKeys:  10,
			expected: PageHeaderSize + 10*LeafElementSize,
		},
		{
			name:     "branch with 0 Keys",
			flags:    BranchPageFlag,
			numKeys:  0,
			expected: PageHeaderSize,
		},
		{
			name:     "branch with 5 Keys",
			flags:    BranchPageFlag,
			numKeys:  5,
			expected: PageHeaderSize + 5*BranchElementSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var page Page
			header := PageHeader{
				PageID:  1,
				Flags:   tt.flags,
				NumKeys: tt.numKeys,
			}
			page.WriteHeader(&header)

			dataStart := page.dataAreaStart()
			assert.Equal(t, tt.expected, dataStart, "dataAreaStart()")
		})
	}
}

func TestGetKeyValue(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header for leaf with 1 key
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	dataStart := page.dataAreaStart()

	// Write leaf element with absolute offsets
	elem := LeafElement{
		KeyOffset:   uint16(dataStart),
		KeySize:     5,
		ValueOffset: uint16(dataStart + 5),
		ValueSize:   10,
	}
	page.WriteLeafElement(0, &elem)

	// Write actual key/value data in data area
	key := []byte("hello")
	value := []byte("world12345")
	copy(page.Data[dataStart:], key)
	copy(page.Data[dataStart+5:], value)

	// Read back using getKey/getValue
	readKey, err := page.GetKey(elem.KeyOffset, elem.KeySize)
	require.NoError(t, err, "getKey()")
	readValue, err := page.GetValue(elem.ValueOffset, elem.ValueSize)
	require.NoError(t, err, "getValue()")

	assert.Equal(t, string(key), string(readKey), "getKey()")
	assert.Equal(t, string(value), string(readValue), "getValue()")
}

func TestGetKeyValueBoundsChecking(t *testing.T) {
	t.Parallel()

	var page Page

	// Write Header for leaf with 1 key
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	dataStart := page.dataAreaStart()

	tests := []struct {
		name    string
		offset  uint16
		size    uint16
		wantErr bool
	}{
		{
			name:    "valid key at data start",
			offset:  uint16(dataStart),
			size:    10,
			wantErr: false,
		},
		{
			name:    "key extends beyond Page",
			offset:  uint16(PageSize - 5),
			size:    10,
			wantErr: true,
		},
		{
			name:    "offset too large",
			offset:  uint16(PageSize),
			size:    1,
			wantErr: true,
		},
		{
			name:    "zero Size at data start",
			offset:  uint16(dataStart),
			size:    0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := page.GetKey(tt.offset, tt.size)
			if tt.wantErr {
				assert.Error(t, err, "getKey()")
			} else {
				assert.NoError(t, err, "getKey()")
			}
		})
	}
}

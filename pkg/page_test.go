package pkg

import (
	"testing"
	"unsafe"
)

func TestPageHeaderAlignment(t *testing.T) {
	t.Parallel()

	// Verify struct sizes match expectations (no padding)
	if size := unsafe.Sizeof(PageID(0)); size != 8 {
		t.Errorf("PageID size = %d bytes, expected 8", size)
	}

	if size := unsafe.Sizeof(PageHeader{}); size != 40 {
		t.Errorf("PageHeader size = %d bytes, expected 40", size)
	}

	// Verify field offsets (no padding needed)
	var h PageHeader
	if offset := unsafe.Offsetof(h.PageID); offset != 0 {
		t.Errorf("PageID offset = %d, expected 0", offset)
	}
	if offset := unsafe.Offsetof(h.Flags); offset != 8 {
		t.Errorf("Flags offset = %d, expected 8", offset)
	}
	if offset := unsafe.Offsetof(h.NumKeys); offset != 10 {
		t.Errorf("NumKeys offset = %d, expected 10", offset)
	}
	if offset := unsafe.Offsetof(h.Padding); offset != 12 {
		t.Errorf("Padding offset = %d, expected 12", offset)
	}
	if offset := unsafe.Offsetof(h.TxnID); offset != 16 {
		t.Errorf("TxnID offset = %d, expected 16", offset)
	}
	if offset := unsafe.Offsetof(h._NextLeaf); offset != 24 {
		t.Errorf("_NextLeaf offset = %d, expected 24", offset)
	}
	if offset := unsafe.Offsetof(h._PrevLeaf); offset != 32 {
		t.Errorf("_PrevLeaf offset = %d, expected 32", offset)
	}
}

func TestPageHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header
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

	if readHdr.PageID != writeHdr.PageID {
		t.Errorf("PageID: got %d, want %d", readHdr.PageID, writeHdr.PageID)
	}
	if readHdr.Flags != writeHdr.Flags {
		t.Errorf("Flags: got %d, want %d", readHdr.Flags, writeHdr.Flags)
	}
	if readHdr.NumKeys != writeHdr.NumKeys {
		t.Errorf("NumKeys: got %d, want %d", readHdr.NumKeys, writeHdr.NumKeys)
	}
	if readHdr.Padding != writeHdr.Padding {
		t.Errorf("Padding: got %d, want %d", readHdr.Padding, writeHdr.Padding)
	}
	if readHdr.TxnID != writeHdr.TxnID {
		t.Errorf("TxnID: got %d, want %d", readHdr.TxnID, writeHdr.TxnID)
	}
}

func TestPageHeaderByteLayout(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header with known values
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
		// TxnID (8 bytes, little-endian)
		0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
		// _NextLeaf (8 bytes, little-endian)
		0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE,
		// _PrevLeaf (8 bytes, little-endian)
		0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x00,
	}

	for i, expectedByte := range expected {
		if page.data[i] != expectedByte {
			t.Errorf("byte[%d]: got 0x%02X, want 0x%02X", i, page.data[i], expectedByte)
		}
	}
}

func TestLeafElementAlignment(t *testing.T) {
	t.Parallel()

	if size := unsafe.Sizeof(LeafElement{}); size != 12 {
		t.Errorf("LeafElement size = %d bytes, expected 12", size)
	}
}

func TestLeafElementByteLayout(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header first (need valid header for element writes)
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	// Write leaf element with known values
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
		if page.data[offset+i] != expectedByte {
			t.Errorf("byte[%d]: got 0x%02X, want 0x%02X", offset+i, page.data[offset+i], expectedByte)
		}
	}
}

func TestLeafElementRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header first
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

	if len(elements) != 2 {
		t.Fatalf("got %d elements, want 2", len(elements))
	}

	if elements[0].KeyOffset != elem1.KeyOffset {
		t.Errorf("elem[0].KeyOffset: got %d, want %d", elements[0].KeyOffset, elem1.KeyOffset)
	}
	if elements[0].KeySize != elem1.KeySize {
		t.Errorf("elem[0].KeySize: got %d, want %d", elements[0].KeySize, elem1.KeySize)
	}
	if elements[0].ValueOffset != elem1.ValueOffset {
		t.Errorf("elem[0].ValueOffset: got %d, want %d", elements[0].ValueOffset, elem1.ValueOffset)
	}
	if elements[0].ValueSize != elem1.ValueSize {
		t.Errorf("elem[0].ValueSize: got %d, want %d", elements[0].ValueSize, elem1.ValueSize)
	}

	if elements[1].KeyOffset != elem2.KeyOffset {
		t.Errorf("elem[1].KeyOffset: got %d, want %d", elements[1].KeyOffset, elem2.KeyOffset)
	}
	if elements[1].KeySize != elem2.KeySize {
		t.Errorf("elem[1].KeySize: got %d, want %d", elements[1].KeySize, elem2.KeySize)
	}
}

func TestBranchElementAlignment(t *testing.T) {
	t.Parallel()

	if size := unsafe.Sizeof(BranchElement{}); size != 16 {
		t.Errorf("BranchElement size = %d bytes, expected 16", size)
	}
}

func TestBranchElementByteLayout(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header first (need valid header for element writes)
	header := PageHeader{
		PageID:  1,
		Flags:   BranchPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	// Write branch element with known values (B+ tree: no values in branches)
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
		if page.data[offset+i] != expectedByte {
			t.Errorf("byte[%d]: got 0x%02X, want 0x%02X", offset+i, page.data[offset+i], expectedByte)
		}
	}
}

func TestBranchElementRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header first
	header := PageHeader{
		PageID:  1,
		Flags:   BranchPageFlag,
		NumKeys: 2,
	}
	page.WriteHeader(&header)

	// Write branch elements (B+ tree: no values in branches)
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

	if len(elements) != 2 {
		t.Fatalf("got %d elements, want 2", len(elements))
	}

	if elements[0].KeyOffset != elem1.KeyOffset {
		t.Errorf("elem[0].KeyOffset: got %d, want %d", elements[0].KeyOffset, elem1.KeyOffset)
	}
	if elements[0].KeySize != elem1.KeySize {
		t.Errorf("elem[0].KeySize: got %d, want %d", elements[0].KeySize, elem1.KeySize)
	}
	if elements[0].ChildID != elem1.ChildID {
		t.Errorf("elem[0].ChildID: got %d, want %d", elements[0].ChildID, elem1.ChildID)
	}

	if elements[1].KeyOffset != elem2.KeyOffset {
		t.Errorf("elem[1].KeyOffset: got %d, want %d", elements[1].KeyOffset, elem2.KeyOffset)
	}
	if elements[1].KeySize != elem2.KeySize {
		t.Errorf("elem[1].KeySize: got %d, want %d", elements[1].KeySize, elem2.KeySize)
	}
	if elements[1].ChildID != elem2.ChildID {
		t.Errorf("elem[1].ChildID: got %d, want %d", elements[1].ChildID, elem2.ChildID)
	}
}

func TestBranchFirstChildRoundTrip(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header
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

	if readChildID != childID {
		t.Errorf("ChildID: got %d, want %d", readChildID, childID)
	}
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
			name:     "leaf with 0 keys",
			flags:    LeafPageFlag,
			numKeys:  0,
			expected: PageHeaderSize,
		},
		{
			name:     "leaf with 10 keys",
			flags:    LeafPageFlag,
			numKeys:  10,
			expected: PageHeaderSize + 10*LeafElementSize,
		},
		{
			name:     "branch with 0 keys",
			flags:    BranchPageFlag,
			numKeys:  0,
			expected: PageHeaderSize,
		},
		{
			name:     "branch with 5 keys",
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

			dataStart := page.DataAreaStart()
			if dataStart != tt.expected {
				t.Errorf("DataAreaStart() = %d, want %d", dataStart, tt.expected)
			}
		})
	}
}

func TestGetKeyValue(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header for leaf with 1 key
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	dataStart := page.DataAreaStart()

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
	copy(page.data[dataStart:], key)
	copy(page.data[dataStart+5:], value)

	// Read back using GetKey/GetValue
	readKey, err := page.GetKey(elem.KeyOffset, elem.KeySize)
	if err != nil {
		t.Fatalf("GetKey() error = %v", err)
	}
	readValue, err := page.GetValue(elem.ValueOffset, elem.ValueSize)
	if err != nil {
		t.Fatalf("GetValue() error = %v", err)
	}

	if string(readKey) != string(key) {
		t.Errorf("GetKey() = %q, want %q", readKey, key)
	}
	if string(readValue) != string(value) {
		t.Errorf("GetValue() = %q, want %q", readValue, value)
	}
}

func TestGetKeyValueBoundsChecking(t *testing.T) {
	t.Parallel()

	var page Page

	// Write header for leaf with 1 key
	header := PageHeader{
		PageID:  1,
		Flags:   LeafPageFlag,
		NumKeys: 1,
	}
	page.WriteHeader(&header)

	dataStart := page.DataAreaStart()

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
			name:    "key extends beyond page",
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
			name:    "zero size at data start",
			offset:  uint16(dataStart),
			size:    0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := page.GetKey(tt.offset, tt.size)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

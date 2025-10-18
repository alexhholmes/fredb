package writebuf

// Entry represents a buffered write operation
type Entry struct {
	Value   []byte
	Deleted bool // Tombstone flag for deletions
}

// Buffer manages batched write operations
type Buffer struct {
	entries   map[string]*Entry
	size      int
	threshold int
	ops       int
}

// NewBuffer creates a new write buffer with given threshold (in bytes)
func NewBuffer(threshold int) *Buffer {
	return &Buffer{
		entries:   make(map[string]*Entry),
		size:      0,
		threshold: threshold,
		ops:       0,
	}
}

// Set adds or updates a key-value pair in the buffer
// Returns (shouldBypass=true if single op, shouldFlush=true if threshold exceeded)
func (b *Buffer) Set(key string, value []byte) (shouldBypass, shouldFlush bool) {
	// Track size delta and check if new operation
	oldSize := 0
	old, exists := b.entries[key]
	if exists {
		oldSize = len(old.Value)
	}

	// Track operation count
	if !exists {
		b.ops++
	}

	// Single operation? Signal caller to bypass buffering
	if b.ops == 1 && !exists {
		return true, false
	}

	// Buffer the write
	b.entries[key] = &Entry{
		Value:   append([]byte(nil), value...), // Defensive copy
		Deleted: false,
	}

	b.size += len(key) + len(value) - oldSize

	// Check if flush needed
	shouldFlush = b.size >= b.threshold
	return false, shouldFlush
}

// Delete adds a tombstone for the given key
// Returns (shouldBypass=true if single op, shouldFlush=true if threshold exceeded)
func (b *Buffer) Delete(key string) (shouldBypass, shouldFlush bool) {
	e, exists := b.entries[key]
	if exists {
		// Key already buffered - update to tombstone
		oldSize := len(e.Value)
		e.Deleted = true
		e.Value = nil
		b.size -= oldSize
	} else {
		// Track new operation
		b.ops++

		// Single operation? Signal caller to bypass buffering
		if b.ops == 1 {
			return true, false
		}

		// Add new tombstone to buffer
		b.entries[key] = &Entry{
			Value:   nil,
			Deleted: true,
		}
		b.size += len(key)
	}

	// Check if flush needed
	shouldFlush = b.size >= b.threshold
	return false, shouldFlush
}

// Get retrieves an entry from the buffer
// Returns (value, deleted, found)
func (b *Buffer) Get(key string) ([]byte, bool, bool) {
	entry, found := b.entries[key]
	if !found {
		return nil, false, false
	}

	if entry.Deleted {
		return nil, true, true
	}

	// Return defensive copy
	result := make([]byte, len(entry.Value))
	copy(result, entry.Value)
	return result, false, true
}

// Len returns the number of buffered operations
func (b *Buffer) Len() int {
	return len(b.entries)
}

// SortedKeys returns all buffered keys in sorted order
func (b *Buffer) SortedKeys() []string {
	keys := make([]string, 0, len(b.entries))
	for k := range b.entries {
		keys = append(keys, k)
	}
	// Note: caller must sort this (we don't import "sort" to keep dependencies minimal)
	return keys
}

// GetEntry returns the entry for a key (for iteration during flush)
func (b *Buffer) GetEntry(key string) (*Entry, bool) {
	entry, found := b.entries[key]
	return entry, found
}

// Remove deletes a specific key from the buffer
func (b *Buffer) Remove(key string) {
	if entry, exists := b.entries[key]; exists {
		b.size -= len(key) + len(entry.Value)
		delete(b.entries, key)
		b.ops--
	}
}

// Clear empties the buffer
func (b *Buffer) Clear() {
	b.entries = make(map[string]*Entry)
	b.size = 0
	b.ops = 0
}

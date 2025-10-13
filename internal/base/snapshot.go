package base

import "sync/atomic"

// Snapshot bundles metadata and root pointer for atomic visibility with reference counting
type Snapshot struct {
	Meta MetaPage
	Root *Node
	Refs atomic.Int32 // Reference count for tracking active readers
}

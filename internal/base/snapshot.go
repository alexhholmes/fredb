package base

// Snapshot bundles metadata and root pointer for atomic visibility with reference counting
type Snapshot struct {
	Meta MetaPage
	Root *Node
}

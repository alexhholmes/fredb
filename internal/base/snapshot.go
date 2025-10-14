package base

// Snapshot bundles metadata and root pointer for atomic visibility
type Snapshot struct {
	Meta MetaPage
	Root *Node
}

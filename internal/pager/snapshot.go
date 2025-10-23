package pager

import "github.com/alexhholmes/fredb/internal/base"

// Snapshot bundles metadata and root pointer for atomic visibility with reference counting
type Snapshot struct {
	Meta base.MetaPage
	Root base.PageData
}

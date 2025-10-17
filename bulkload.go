package fredb

import (
	"bytes"
	"errors"

	"fredb/internal/base"
)

type BulkLoader struct {
	tx     *Tx
	bucket *Bucket

	// All nodes: leaves + internal branches
	nodes map[base.PageID]*base.Node

	// Leaf chain for bottom-up tree construction
	leaves      []*base.Node
	currentLeaf *base.Node
	lastKey     []byte // Enforce sorted order

	shadowRoot *base.Node
}

// Set adds a key-value pair to the bulk loader.
// Keys must be inserted in strictly ascending sorted order.
func (l *BulkLoader) Set(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}
	if len(key) > MaxKeySize {
		return errors.New("key too large")
	}
	if len(value) > MaxValueSize {
		return errors.New("value too large")
	}

	// Enforce sorted order
	if l.lastKey != nil && bytes.Compare(key, l.lastKey) <= 0 {
		return errors.New("keys must be inserted in strictly ascending order")
	}

	// Check if current leaf needs to be finalized
	if l.currentLeaf != nil {
		estimatedSize := l.currentLeaf.Size() + len(key) + len(value) + base.LeafElementSize
		if estimatedSize > int(base.PageSize*95/100) {
			// Current leaf is full, allocate a new one
			l.currentLeaf = nil
		}
	}

	// Allocate new leaf if needed
	if l.currentLeaf == nil {
		pageID, _, err := l.tx.allocatePage()
		if err != nil {
			return err
		}
		l.currentLeaf = &base.Node{
			PageID:   pageID,
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}
		l.nodes[pageID] = l.currentLeaf
		l.leaves = append(l.leaves, l.currentLeaf)
	}

	// Append key-value pair directly (no search, no COW)
	l.currentLeaf.Keys = append(l.currentLeaf.Keys, key)
	l.currentLeaf.Values = append(l.currentLeaf.Values, value)
	l.currentLeaf.NumKeys++

	// Update lastKey for sorted order enforcement
	l.lastKey = make([]byte, len(key))
	copy(l.lastKey, key)

	return nil
}

// finalize builds the tree bottom-up from the leaf chain.
func (l *BulkLoader) finalize() error {
	if len(l.leaves) == 0 {
		return errors.New("bulk loader is empty")
	}

	// Single leaf case: set as shadowRoot
	if len(l.leaves) == 1 {
		l.shadowRoot = l.leaves[0]
		return nil
	}

	// Build internal nodes level by level
	currentLevel := make([]*base.Node, len(l.leaves))
	copy(currentLevel, l.leaves)

	for len(currentLevel) > 1 {
		nextLevel := []*base.Node{}

		var currentBranch *base.Node
		threshold := int(base.PageSize * 95 / 100)

		for _, child := range currentLevel {
			// Allocate new branch if needed
			if currentBranch == nil {
				pageID, _, err := l.tx.allocatePage()
				if err != nil {
					return err
				}
				currentBranch = &base.Node{
					PageID:   pageID,
					Dirty:    true,
					NumKeys:  0,
					Keys:     make([][]byte, 0),
					Values:   nil,
					Children: make([]base.PageID, 0),
				}
				l.nodes[pageID] = currentBranch
			}

			// Calculate cost of adding this child
			var cost int
			if len(currentBranch.Children) == 0 {
				// First child: only pageID (8 bytes)
				cost = 8
			} else {
				// Subsequent children: separator key + pageID
				separatorKey := child.Keys[0]
				cost = base.BranchElementSize + len(separatorKey)
			}

			// Check if adding this child would exceed threshold
			if len(currentBranch.Children) > 0 && currentBranch.Size()+cost > threshold {
				// Finalize current branch and start a new one
				nextLevel = append(nextLevel, currentBranch)

				pageID, _, err := l.tx.allocatePage()
				if err != nil {
					return err
				}
				currentBranch = &base.Node{
					PageID:   pageID,
					Dirty:    true,
					NumKeys:  0,
					Keys:     make([][]byte, 0),
					Values:   nil,
					Children: make([]base.PageID, 0),
				}
				l.nodes[pageID] = currentBranch

				// Recalculate cost for first child in new branch
				cost = 8
			}

			// Add child to current branch
			// In B+ tree: first child has no separator key, subsequent children do
			if len(currentBranch.Children) == 0 {
				// First child
				currentBranch.Children = append(currentBranch.Children, child.PageID)
			} else {
				// Subsequent children: add separator key
				separatorKey := child.Keys[0]
				currentBranch.Keys = append(currentBranch.Keys, separatorKey)
				currentBranch.Children = append(currentBranch.Children, child.PageID)
				currentBranch.NumKeys++
			}
		}

		// Add the last branch to next level
		if currentBranch != nil && len(currentBranch.Children) > 0 {
			nextLevel = append(nextLevel, currentBranch)
		}

		currentLevel = nextLevel
	}

	// Set the root
	if len(currentLevel) == 1 {
		l.shadowRoot = currentLevel[0]
	} else {
		return errors.New("failed to build tree: multiple roots remaining")
	}

	return nil
}

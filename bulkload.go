package fredb

import (
	"bytes"
	"unsafe"

	"fredb/internal/base"
	"fredb/internal/directio"
)

const leafBatchSize = 1024 // 1024 pages = 4MB batches

type leafRef struct {
	pageID   base.PageID
	firstKey []byte
}

type BulkLoader struct {
	tx     *Tx
	bucket *Bucket

	// Leaf references: pageID + firstKey for branch construction
	leafRefs    []leafRef
	currentLeaf *base.Node
	lastKey     []byte // Enforce sorted order

	shadowRoot *base.Node

	// Cache of leaf/branch nodes keyed by PageID (for single-leaf case and tree structure)
	nodeCache map[base.PageID]*base.Node

	// Batch writes: accumulate leaves/branches before writing
	leafBatch   []*base.Node
	branchBatch []*base.Node

	// Stats tracking
	keysInserted int
}

// Set adds a key-value pair to the bulk loader.
// Keys MUST be inserted in strictly ascending sorted ordeTr.
func (l *BulkLoader) Set(key, value []byte) error {
	if len(key) == 0 {
		return ErrKeyEmpty
	}
	if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Enforce sorted order
	if l.lastKey != nil && bytes.Compare(key, l.lastKey) <= 0 {
		return ErrKeysUnsorted
	}

	// Check if current leaf needs to be finalized
	if l.currentLeaf != nil {
		estimatedSize := l.currentLeaf.Size() + len(key) + len(value) + base.LeafElementSize
		if estimatedSize > int(base.PageSize*95/100) {
			// Current leaf is full - write to disk and keep ref
			if err := l.flushLeaf(); err != nil {
				return err
			}
			l.currentLeaf = nil
		}
	}

	// Allocate new leaf if needed
	if l.currentLeaf == nil {
		// Don't assign PageID yet - will be assigned during batch flush
		l.currentLeaf = &base.Node{
			PageID:   0, // Assigned later in flushBatch
			Dirty:    true,
			NumKeys:  0,
			Keys:     make([][]byte, 0),
			Values:   make([][]byte, 0),
			Children: nil,
		}
	}

	// Append key-value pair directly (no search, no COW)
	l.currentLeaf.Keys = append(l.currentLeaf.Keys, key)
	l.currentLeaf.Values = append(l.currentLeaf.Values, value)
	l.currentLeaf.NumKeys++
	l.keysInserted++ // Track progress

	// Update lastKey for sorted order enforcement
	l.lastKey = make([]byte, len(key))
	copy(l.lastKey, key)

	return nil
}

// finalize builds the tree bottom-up from the leaf refs.
func (l *BulkLoader) finalize() error {
	// Flush any remaining in-memory leaf (even if empty, for consistency)
	if l.currentLeaf != nil {
		if l.currentLeaf.NumKeys > 0 {
			if err := l.flushLeaf(); err != nil {
				return err
			}
		} else {
			// Even empty leaf needs to be added to tx.pages to ensure tree structure
			l.tx.pages[l.currentLeaf.PageID] = l.currentLeaf
			l.nodeCache[l.currentLeaf.PageID] = l.currentLeaf

			// Save ref to leafRefs so we have at least one leaf
			if len(l.leafRefs) == 0 {
				l.leafRefs = append(l.leafRefs, leafRef{
					pageID:   l.currentLeaf.PageID,
					firstKey: nil, // Empty leaf has no first key
				})
			}
		}
		l.currentLeaf = nil
	}

	// Flush any remaining batched leaves
	if err := l.flushBatch(); err != nil {
		return err
	}

	if len(l.leafRefs) == 0 {
		return ErrBulkLoaderEmpty
	}

	// Single leaf case: set as shadowRoot
	if len(l.leafRefs) == 1 {
		ref := l.leafRefs[0]
		leaf := l.nodeCache[ref.pageID]
		if leaf == nil {
			return ErrBulkLoaderEmpty
		}
		l.shadowRoot = leaf
		return nil
	}

	// Build internal nodes level by level using refs
	currentLevel := make([]leafRef, len(l.leafRefs))
	copy(currentLevel, l.leafRefs)

	for len(currentLevel) > 1 {
		// Track branch metadata before PageID assignment
		type branchMeta struct {
			branch   *base.Node
			firstKey []byte
		}
		var branchMetas []branchMeta

		var currentBranch *base.Node
		var branchFirstKey []byte
		threshold := base.PageSize * 95 / 100

		for _, ref := range currentLevel {
			// Allocate new branch if needed
			if currentBranch == nil {
				currentBranch = &base.Node{
					PageID:   0, // Assigned later in flushBranchBatch
					Dirty:    true,
					NumKeys:  0,
					Keys:     make([][]byte, 0),
					Values:   nil,
					Children: make([]base.PageID, 0),
				}
				branchFirstKey = ref.firstKey
			}

			// Calculate cost of adding this child
			var cost int
			if len(currentBranch.Children) == 0 {
				// First child: only pageID (8 bytes)
				cost = 8
			} else {
				// Subsequent children: separator key + pageID
				cost = base.BranchElementSize + len(ref.firstKey)
			}

			// Check if adding this child would exceed threshold
			if len(currentBranch.Children) > 0 && currentBranch.Size()+cost > threshold {
				// Save branch metadata before writing
				branchMetas = append(branchMetas, branchMeta{
					branch:   currentBranch,
					firstKey: branchFirstKey,
				})
				if err := l.writeBranch(currentBranch); err != nil {
					return err
				}

				// Start new branch
				currentBranch = &base.Node{
					PageID:   0, // Assigned later in flushBranchBatch
					Dirty:    true,
					NumKeys:  0,
					Keys:     make([][]byte, 0),
					Values:   nil,
					Children: make([]base.PageID, 0),
				}
				branchFirstKey = ref.firstKey

				// Recalculate cost for first child in new branch
				cost = 8
			}

			// Add child to current branch
			// In B+ tree: first child has no separator key, subsequent children do
			if len(currentBranch.Children) == 0 {
				// First child
				currentBranch.Children = append(currentBranch.Children, ref.pageID)
			} else {
				// Subsequent children: add separator key
				currentBranch.Keys = append(currentBranch.Keys, ref.firstKey)
				currentBranch.Children = append(currentBranch.Children, ref.pageID)
				currentBranch.NumKeys++
			}
		}

		// Handle last branch
		if currentBranch != nil && len(currentBranch.Children) > 0 {
			if len(branchMetas) > 0 {
				// More branches exist, write this one to batch
				branchMetas = append(branchMetas, branchMeta{
					branch:   currentBranch,
					firstKey: branchFirstKey,
				})
				if err := l.writeBranch(currentBranch); err != nil {
					return err
				}
			} else {
				// This is the only branch at this level, it will be the root
				l.shadowRoot = currentBranch
				return nil
			}
		}

		// Flush all branches for this level (assigns PageIDs)
		if err := l.flushBranchBatch(); err != nil {
			return err
		}

		// Build nextLevel refs AFTER PageIDs are assigned
		var nextLevel []leafRef
		for _, meta := range branchMetas {
			nextLevel = append(nextLevel, leafRef{
				pageID:   meta.branch.PageID,
				firstKey: meta.firstKey,
			})
		}

		currentLevel = nextLevel
	}

	// Should not reach here, but set root if we do
	if len(currentLevel) == 1 {
		root, err := l.tx.loadNode(currentLevel[0].pageID)
		if err != nil {
			return err
		}
		l.shadowRoot = root
	} else if len(currentLevel) == 0 {
		return ErrBulkLoaderEmpty
	} else {
		return ErrBulkLoaderMultipleRoots
	}

	return nil
}

// BulkLoaderStats contains progress information about a completed bulk load operation.
type BulkLoaderStats struct {
	KeysInserted           int     // Total number of keys inserted so far
	LeavesCreated          int     // Number of leaf nodes created
	CurrentLeafUtilization float64 // Current leaf fullness (0.0 to 1.0)
}

// Stats returns current progress statistics for this bulk load operation.
// Useful for monitoring progress during long-running bulk loads.
func (l *BulkLoader) Stats() BulkLoaderStats {
	utilization := 0.0
	if l.currentLeaf != nil {
		utilization = float64(l.currentLeaf.Size()) / float64(base.PageSize)
	}

	return BulkLoaderStats{
		KeysInserted:           l.keysInserted,
		LeavesCreated:          len(l.leafRefs),
		CurrentLeafUtilization: utilization,
	}
}

// flushLeaf adds current leaf to batch and flushes batch if full
func (l *BulkLoader) flushLeaf() error {
	if l.currentLeaf == nil || l.currentLeaf.NumKeys == 0 {
		return nil
	}

	// Add to batch
	l.leafBatch = append(l.leafBatch, l.currentLeaf)

	// Flush batch if full
	if len(l.leafBatch) >= leafBatchSize {
		return l.flushBatch()
	}

	return nil
}

// flushBatch writes accumulated leaves in a single batch write
func (l *BulkLoader) flushBatch() error {
	if len(l.leafBatch) == 0 {
		return nil
	}

	batchSize := len(l.leafBatch)

	// Allocate sequential page IDs for entire batch
	startID, _ := l.tx.db.pager.AssignPageRange(batchSize)

	// Allocate aligned buffer for batch
	buffer := directio.AlignedBlock(batchSize * base.PageSize)

	// Serialize all leaves into contiguous buffer
	for i, leaf := range l.leafBatch {
		// Assign PageID from range
		leaf.PageID = base.PageID(int(startID) + i)

		// Serialize into buffer slice
		pageSlice := buffer[i*base.PageSize : (i+1)*base.PageSize]
		page := (*base.Page)(unsafe.Pointer(&pageSlice[0]))
		if err := leaf.Serialize(l.tx.txID, page); err != nil {
			return err
		}

		// Save reference for branch construction
		firstKey := make([]byte, len(leaf.Keys[0]))
		copy(firstKey, leaf.Keys[0])
		l.leafRefs = append(l.leafRefs, leafRef{
			pageID:   leaf.PageID,
			firstKey: firstKey,
		})

		// Cache ONLY first leaf (for single-leaf case)
		if len(l.leafRefs) == 1 {
			l.nodeCache[leaf.PageID] = leaf
		}
	}

	// Write entire batch in one syscall
	if err := l.tx.db.store.WritePageRange(startID, buffer); err != nil {
		return err
	}

	// Clear batch - nil out references to allow GC
	for i := range l.leafBatch {
		l.leafBatch[i] = nil
	}
	l.leafBatch = l.leafBatch[:0]

	return nil
}

// writeBranch adds branch to batch and flushes if full
func (l *BulkLoader) writeBranch(branch *base.Node) error {
	// Add to batch (PageID assigned later in flushBranchBatch)
	l.branchBatch = append(l.branchBatch, branch)

	// Flush batch if full (4MB batches)
	if len(l.branchBatch) >= leafBatchSize {
		return l.flushBranchBatch()
	}

	return nil
}

// flushBranchBatch writes accumulated branches in a single batch write
func (l *BulkLoader) flushBranchBatch() error {
	if len(l.branchBatch) == 0 {
		return nil
	}

	batchSize := len(l.branchBatch)

	// Allocate sequential page IDs for exact count (no waste)
	startID, _ := l.tx.db.pager.AssignPageRange(batchSize)

	// Allocate aligned buffer for batch
	buffer := directio.AlignedBlock(batchSize * base.PageSize)

	// Serialize all branches into contiguous buffer
	for i, branch := range l.branchBatch {
		// Assign PageID from range
		branch.PageID = base.PageID(int(startID) + i)

		// Serialize into buffer slice
		pageSlice := buffer[i*base.PageSize : (i+1)*base.PageSize]
		page := (*base.Page)(unsafe.Pointer(&pageSlice[0]))
		if err := branch.Serialize(l.tx.txID, page); err != nil {
			return err
		}
	}

	// Write entire batch in one syscall
	if err := l.tx.db.store.WritePageRange(startID, buffer); err != nil {
		return err
	}

	// Clear batch - nil out references to allow GC
	for i := range l.branchBatch {
		l.branchBatch[i] = nil
	}
	l.branchBatch = l.branchBatch[:0]

	return nil
}

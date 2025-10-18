package prefetch

import "fredb/internal/base"

// Prefetcher manages async loading of upcoming leaf pages
type Prefetcher struct {
	direction int         // 1=forward, -1=backward, 0=none
	distance  int         // how many leaves ahead to keep loaded
	current   base.PageID // track what's currently being prefetched
}

// LoadNodeFunc is a callback for loading nodes by page ID
type LoadNodeFunc func(base.PageID) (*base.Node, error)

// Trigger starts async prefetching of leaf pages in the specified direction
// dir: 1=forward, -1=backward
// loadNode: callback function to load nodes (typically tx.loadNode)
func (p *Prefetcher) Trigger(startPgid base.PageID, dir int, loadNode LoadNodeFunc) {
	if startPgid == 0 {
		return
	}

	// Direction change: Reset distance
	if p.direction != 0 && p.direction != dir {
		p.distance = 2
	}

	// First time or continuing same direction
	if p.direction == 0 {
		p.distance = 2
	} else if p.direction == dir && p.distance < 8 {
		// Adaptive: increase prefetch distance for sustained sequential access
		p.distance++
	}

	p.direction = dir

	// Already prefetching this chain
	if p.current == startPgid {
		return
	}
	p.current = startPgid

	// Async prefetch
	go func() {
		pgid := startPgid
		for i := 0; i < p.distance && pgid != 0; i++ {
			node, err := loadNode(pgid)
			if err != nil || node == nil {
				break
			}

			// Follow chain based on direction
			if dir == 1 {
				pgid = node.NextLeaf
			} else {
				pgid = node.PrevLeaf
			}
		}
	}()
}

// Reset clears Prefetcher state (called on Seek)
func (p *Prefetcher) Reset() {
	p.direction = 0
	p.distance = 2
	p.current = 0
}

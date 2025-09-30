package src

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDebugSearch(t *testing.T) {
	db := setupTestDB(t)

	// Insert 6 keys to trigger a split (MaxKeysPerNode=4)
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		value := []byte(fmt.Sprintf("value%d", i))

		fmt.Printf("Setting %s\n", key)
		if err := db.Set(key, value); err != nil {
			t.Fatalf("Failed to set: %v", err)
		}
	}

	// Check the root structure after inserts
	fmt.Printf("\n=== Root structure after inserts ===\n")
	root := db.store.root
	fmt.Printf("Root: pageID=%d, isLeaf=%v, numKeys=%d\n",
		root.pageID, root.isLeaf, root.numKeys)

	if !root.isLeaf {
		fmt.Printf("Root keys: ")
		for i := 0; i < int(root.numKeys); i++ {
			fmt.Printf("%s ", root.keys[i])
		}
		fmt.Printf("\nRoot children: %v\n", root.children)
	}

	// Now search for each key with debug info
	fmt.Println("\n=== Searching for all keys ===")
	for i := 0; i < 6; i++ {
		key := []byte(fmt.Sprintf("key%08d", i))
		fmt.Printf("\nSearching for %s:\n", key)

		// Create a read transaction
		tx, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}

		// The transaction should see the current root
		txRoot := tx.root
		fmt.Printf("  Tx root: pageID=%d, isLeaf=%v, numKeys=%d\n",
			txRoot.pageID, txRoot.isLeaf, txRoot.numKeys)

		// Manual search to debug
		node := txRoot
		for !node.isLeaf {
			// Find which child to descend to
			i := 0
			for i < int(node.numKeys) && bytes.Compare(key, node.keys[i]) > 0 {
				i++
			}
			fmt.Printf("  Descending to child %d (pageID=%d)\n", i, node.children[i])

			// Load child
			child, err := db.store.loadNode(node.children[i])
			if err != nil {
				fmt.Printf("  ERROR loading child: %v\n", err)
				break
			}

			fmt.Printf("  Child loaded: pageID=%d, isLeaf=%v, numKeys=%d\n",
				child.pageID, child.isLeaf, child.numKeys)
			node = child
		}

		// If we reached a leaf, check if key is there
		if node.isLeaf {
			fmt.Printf("  Leaf keys: ")
			for j := 0; j < int(node.numKeys); j++ {
				fmt.Printf("%s ", node.keys[j])
			}
			fmt.Println()

			// Search in leaf
			found := false
			for j := 0; j < int(node.numKeys); j++ {
				if bytes.Equal(node.keys[j], key) {
					fmt.Printf("  FOUND at index %d\n", j)
					found = true
					break
				}
			}
			if !found {
				fmt.Printf("  NOT FOUND in leaf\n")
			}
		}

		// Now try the actual Get
		val, err := tx.Get(key)
		if err != nil {
			fmt.Printf("  Get() failed: %v\n", err)
		} else {
			fmt.Printf("  Get() succeeded: %s\n", val)
		}

		tx.Rollback()
	}
}
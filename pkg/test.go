package pkg

import (
	"flag"
	"fmt"
	"os"
	"testing"
)

// Use -slow flag to run longer tests
var slow = flag.Bool("slow", false, "run slow tests")

// Helper to create a temporary test database
func setupTestDB(t *testing.T) *db {
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.db", t.Name())
	_ = os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
	})

	return db
}

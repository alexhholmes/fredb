// Package test provides integration tests for fredb.
package test

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/alexhholmes/fredb"
)

// setup creates a temporary fredb database for testing.
func setup(t *testing.T) (*fredb.DB, string) {
	tmpfile := fmt.Sprintf("/tmp/test_btree_%s.db", t.Name())
	_ = os.Remove(tmpfile)
	_ = os.Remove(tmpfile + ".wal") // Also remove wal file

	db, err := fredb.Open(tmpfile, fredb.WithCacheSizeMB(0))
	require.NoError(t, err, "Failed to create DB")

	// Type assert to get concrete type for tests
	t.Cleanup(func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
		_ = os.Remove(tmpfile + ".wal") // Cleanup wal file
	})

	return db, tmpfile
}

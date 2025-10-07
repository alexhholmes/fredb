package fredb

import "fredb/internal/wal"

// DBOptions configures database behavior.
type DBOptions struct {
	walSyncMode     wal.SyncMode
	walBytesPerSync int64 // Used when walSyncMode == SyncBytes
}

// defaultDBOptions returns safe default configuration.
func defaultDBOptions() DBOptions {
	return DBOptions{
		walSyncMode:     wal.SyncEveryCommit, // Maximum durability by default
		walBytesPerSync: 1024 * 1024,         // 1MB
	}
}

// DBOption configures database options using the functional options pattern.
type DBOption func(*DBOptions)

// WithWALSyncEveryCommit configures the database to fsync wal on every commit.
// This provides maximum durability (zero data loss) but lower throughput.
//
//goland:noinspection GoUnusedExportedFunction
func WithWALSyncEveryCommit() DBOption {
	return func(opts *DBOptions) {
		opts.walSyncMode = wal.SyncEveryCommit
	}
}

// WithWALSyncBytes configures the database to fsync wal every N bytes written.
// This provides higher throughput with bounded data loss window.
// The bytes parameter determines the maximum amount of data that could be lost on crash.
//
//goland:noinspection GoUnusedExportedFunction
func WithWALSyncBytes(bytes int64) DBOption {
	return func(opts *DBOptions) {
		opts.walSyncMode = wal.SyncBytes
		opts.walBytesPerSync = bytes
	}
}

// WithWALSyncOff disables wal fsync entirely.
// This provides maximum throughput but all unflushed data is lost on crash.
// Only use for testing or bulk loads where data can be reconstructed.
//
//goland:noinspection GoUnusedExportedFunction
func WithWALSyncOff() DBOption {
	return func(opts *DBOptions) {
		opts.walSyncMode = wal.SyncOff
	}
}

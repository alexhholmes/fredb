package fredb

import "fredb/internal/wal"

// DBOptions configures database behavior.
type DBOptions struct {
	walSyncMode     wal.SyncMode
	walBytesPerSync int // Used when walSyncMode == SyncBytes
	maxCacheSizeMB  int // Maximum size of in-memory cache in MB. 0 means no limit.
}

// defaultDBOptions returns safe default configuration.
func defaultDBOptions() DBOptions {
	return DBOptions{
		walSyncMode:     wal.SyncBytes,
		walBytesPerSync: 1024 * 1024, // 1MB
		maxCacheSizeMB:  512,         // 512MB
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
// Note: It is optimal to align this value to the filesystem block size (typically 4096 bytes).
//
//goland:noinspection GoUnusedExportedFunction
func WithWALSyncBytes(bytes int) DBOption {
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

// WithMaxCacheSizeMB sets the maximum size of in-memory cache in MB.
// When the cache exceeds this size, the least recently used items are evicted.
//
//goland:noinspection GoUnusedExportedFunction
func WithMaxCacheSizeMB(mb int) DBOption {
	return func(opts *DBOptions) {
		opts.maxCacheSizeMB = mb
	}
}

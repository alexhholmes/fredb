package fredb

// WALSyncMode controls when the WAL is fsynced to disk.
type WALSyncMode int

const (
	// WALSyncEveryCommit fsyncs on every transaction commit (BoltDB-style).
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data, etcd or Raft
	WALSyncEveryCommit WALSyncMode = iota

	// WALSyncBytes fsyncs when bytesPerSync bytes have been written (RocksDB-style).
	// - Higher throughput than per-commit fsync
	// - Data loss window: up to bytesPerSync bytes on power failure
	// - Use for: Analytics, caches, high-throughput workloads
	WALSyncBytes

	// WALSyncOff disables fsync entirely (testing/bulk loads only).
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	WALSyncOff
)

// DBOptions configures database behavior.
type DBOptions struct {
	walSyncMode     WALSyncMode
	walBytesPerSync int64 // Used when walSyncMode == WALSyncBytes
}

// defaultDBOptions returns safe default configuration.
func defaultDBOptions() DBOptions {
	return DBOptions{
		walSyncMode:     WALSyncEveryCommit, // Maximum durability by default
		walBytesPerSync: 1024 * 1024,        // 1MB
	}
}

// DBOption configures database options using the functional options pattern.
type DBOption func(*DBOptions)

// WithWALSyncEveryCommit configures the database to fsync WAL on every commit.
// This provides maximum durability (zero data loss) but lower throughput.
func WithWALSyncEveryCommit() DBOption {
	return func(opts *DBOptions) {
		opts.walSyncMode = WALSyncEveryCommit
	}
}

// WithWALSyncBytes configures the database to fsync WAL every N bytes written.
// This provides higher throughput with bounded data loss window.
// The bytes parameter determines the maximum amount of data that could be lost on crash.
func WithWALSyncBytes(bytes int64) DBOption {
	return func(opts *DBOptions) {
		opts.walSyncMode = WALSyncBytes
		opts.walBytesPerSync = bytes
	}
}

// WithWALSyncOff disables WAL fsync entirely.
// This provides maximum throughput but all unflushed data is lost on crash.
// Only use for testing or bulk loads where data can be reconstructed.
func WithWALSyncOff() DBOption {
	return func(opts *DBOptions) {
		opts.walSyncMode = WALSyncOff
	}
}

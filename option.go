package fredb

// SyncMode controls when database writes are fsynced to disk
type SyncMode int

const (
	// SyncEveryCommit fsyncs on every transaction commit. Uses direct I/O.
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data
	SyncEveryCommit SyncMode = iota

	// SyncBytes fsyncs when at least N bytes have been written since the last
	// fsync. Uses mmap I/O.
	// - Balances durability and performance
	// - Some data loss possible on crash (up to N bytes)
	// - Use for: General purpose applications
	SyncBytes

	// SyncOff disables fsync entirely (testing/bulk loads only). Uses mmap
	// I/O.
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	SyncOff
)

// DBOptions configures database behavior.
type DBOptions struct {
	syncMode       SyncMode
	syncBytes      uint // Number of bytes to write before fsync when SyncMode is SyncBytes.
	maxCacheSizeMB int  // Maximum size of in-memory cache in MB. 0 means no limit.
}

// DefaultDBOptions returns safe default configuration.
//
// goland:noinspection GoUnusedExportedFunction
func DefaultDBOptions() DBOptions {
	return DBOptions{
		syncMode:       SyncEveryCommit,
		syncBytes:      1024 * 1024, // 1MB
		maxCacheSizeMB: 512,         // 512MB
	}
}

// DBOption configures database options using the functional options pattern.
type DBOption func(*DBOptions)

// WithSyncEveryCommit configures the database to fsync on every commit.
// This provides maximum durability (zero data loss) but lower throughput.
//
//goland:noinspection GoUnusedExportedFunction
func WithSyncEveryCommit() DBOption {
	return func(opts *DBOptions) {
		opts.syncMode = SyncEveryCommit
	}
}

// WithSyncOff disables fsync entirely.
// This provides maximum throughput but all unflushed data is lost on crash.
// Only use for testing or bulk loads where data can be reconstructed.
//
//goland:noinspection GoUnusedExportedFunction
func WithSyncOff() DBOption {
	return func(opts *DBOptions) {
		opts.syncMode = SyncOff
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

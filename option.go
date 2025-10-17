package fredb

// SyncMode controls when database writes are fsynced to disk
type SyncMode int

const (
	// SyncEveryCommit fsyncs on every transaction commit. Uses direct I/O.
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data
	SyncEveryCommit SyncMode = iota

	// SyncOff disables fsync entirely (testing/bulk loads only). Uses mmap
	// I/O.
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	SyncOff
)

// Options configures database behavior.
type Options struct {
	syncMode        SyncMode
	maxCacheSizeMB  int // Maximum size of in-memory cache in MB. 0 means no limit.
	writeBufferSize int // Write buffer size in bytes
}

// DefaultDBOptions returns safe default configuration.
//
// goland:noinspection GoUnusedExportedFunction
func DefaultDBOptions() Options {
	return Options{
		syncMode:        SyncEveryCommit,
		maxCacheSizeMB:  512,             // 512MB
		writeBufferSize: 1 * 1024 * 1024, // 1MB
	}
}

// DBOption configures database options using the functional options pattern.
type DBOption func(*Options)

// WithSyncEveryCommit configures the database to fsync on every commit.
// This provides maximum durability (zero data loss) but lower throughput.
//
//goland:noinspection GoUnusedExportedFunction
func WithSyncEveryCommit() DBOption {
	return func(opts *Options) {
		opts.syncMode = SyncEveryCommit
	}
}

// WithSyncOff disables fsync entirely.
// This provides maximum throughput but all unflushed data is lost on crash.
// Only use for testing or bulk loads where data can be reconstructed.
//
//goland:noinspection GoUnusedExportedFunction
func WithSyncOff() DBOption {
	return func(opts *Options) {
		opts.syncMode = SyncOff
	}
}

// WithMaxCacheSizeMB sets the maximum size of in-memory cache in MB.
// When the cache exceeds this size, the least recently used items are evicted.
//
//goland:noinspection GoUnusedExportedFunction
func WithMaxCacheSizeMB(mb int) DBOption {
	return func(opts *Options) {
		opts.maxCacheSizeMB = mb
	}
}

// WithWriteBufferSize sets the write buffer size in bytes. This is used within
// a write transaction to keep track of uncommitted changes before writing to
// the in-memory btree with sorted writes (improving btree localization).
// Larger sizes can improve write throughput at the cost of higher memory usage.
// Defaults to 1MB.
func WithWriteBufferSize(size int) DBOption {
	return func(opts *Options) {
		opts.writeBufferSize = size
	}
}

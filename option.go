package fredb

// SyncMode controls when database writes are fsynced to disk
type SyncMode int

const (
	// SyncEveryCommit fsyncs on every transaction commit. Uses direct I/O.
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data
	SyncEveryCommit SyncMode = iota

	// SyncOff disables fsync entirely (testing/bulk loads only).
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	SyncOff
)

// Options configures database behavior.
type Options struct {
	SyncMode        SyncMode
	MaxCacheSizeMB  int // Maximum size of in-memory cache in MB. 0 means no limit.
	WriteBufferSize int // Write buffer size in bytes
	MaxReaders      int // Maximum number of concurrent readers
}

// DefaultDBOptions returns safe default configuration.
//
// goland:noinspection GoUnusedExportedFunction
func DefaultDBOptions() Options {
	return Options{
		SyncMode:        SyncEveryCommit,
		MaxCacheSizeMB:  512,             // 512MB
		WriteBufferSize: 1 * 1024 * 1024, // 1MB
		MaxReaders:      256,
	}
}

// Option configures database options using the functional options pattern.
type Option func(*Options)

// WithSyncEveryCommit configures the database to fsync on every commit.
// This provides maximum durability (zero data loss) but lower throughput.
//
//goland:noinspection GoUnusedExportedFunction
func WithSyncEveryCommit() Option {
	return func(opts *Options) {
		opts.SyncMode = SyncEveryCommit
	}
}

// WithSyncOff disables fsync entirely.
// This provides maximum throughput but all unflushed data is lost on crash.
// Only use for testing or bulk loads where data can be reconstructed.
//
//goland:noinspection GoUnusedExportedFunction
func WithSyncOff() Option {
	return func(opts *Options) {
		opts.SyncMode = SyncOff
	}
}

// WithCacheSizeMB sets the maximum size of in-memory cache in MB.
// When the cache exceeds this size, the least recently used items are evicted.
//
//goland:noinspection GoUnusedExportedFunction
func WithCacheSizeMB(mb int) Option {
	return func(opts *Options) {
		opts.MaxCacheSizeMB = mb
	}
}

// WithWriteBufferSize sets the write buffer size in bytes. This is used within
// a write transaction to keep track of uncommitted changes before writing to
// the in-memory btree with sorted writes (improving btree localization).
// Larger sizes can improve write throughput at the cost of higher memory usage.
// Defaults to 1MB.
//
//goland:noinspection GoUnusedExportedFunction
func WithWriteBufferSize(size int) Option {
	return func(opts *Options) {
		opts.WriteBufferSize = size
	}
}

// WithMaxReaders sets the maximum number of concurrent readers.
// Higher values allow more concurrent read transactions at the cost of
// increased memory usage for tracking readers and slightly slower
// writes due to reader management overhead. Defaults to 256.
//
//goland:noinspection GoUnusedExportedFunction
func WithMaxReaders(n int) Option {
	return func(opts *Options) {
		opts.MaxReaders = n
	}
}

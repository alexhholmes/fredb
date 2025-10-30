package fredb

import "github.com/alexhholmes/fredb/internal/pager"

// SyncMode controls when database writes are fsynced to disk
type SyncMode pager.SyncMode

const (
	// SyncEveryCommit fsyncs on every transaction commit. Uses direct I/O.
	// - Guarantees zero data loss on power failure
	// - Limited by fsync latency (typically 1-10ms per commit)
	// - Use for: Financial transactions, critical data
	SyncEveryCommit = SyncMode(pager.SyncEveryCommit)

	// SyncOff disables fsync entirely (testing/bulk loads only).
	// - Maximum throughput
	// - All unflushed data lost on crash
	// - Use for: Testing, bulk imports with external durability
	SyncOff = SyncMode(pager.SyncOff)
)

// Options configures database behavior.
type Options struct {
	SyncMode       SyncMode
	MaxReaders     int // Maximum number of concurrent readers
	MaxCacheSizeMB int // Maximum size of in-memory cache in MB. 0 means no limit.
	Logger         Logger
}

// Option configures database options using the functional options pattern.
type Option func(*Options)

// DefaultOptions returns safe default configuration.
//
// goland:noinspection GoUnusedExportedFunction
func DefaultOptions() Option {
	return func(opts *Options) {
		opts.SyncMode = SyncEveryCommit
		opts.MaxReaders = 256
		opts.MaxCacheSizeMB = 1024
		opts.Logger = DiscardLogger{}
	}
}

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

// WithCacheSizeMB sets the maximum size of in-memory cache in MB.
// When the cache exceeds this size, the least recently used items are evicted.
//
//goland:noinspection GoUnusedExportedFunction
func WithCacheSizeMB(mb int) Option {
	return func(opts *Options) {
		opts.MaxCacheSizeMB = mb
	}
}

// WithLogger sets the default log for this fredb instance. See the log
// subpackage for log library adapters for the Logger interface.
func WithLogger(logger Logger) Option {
	return func(opts *Options) {
		opts.Logger = logger
	}
}

# fredb

Embedded B-tree key-value store in Go.

## Status

**In Development** - Core B-tree operations, disk persistence, MVCC, complete. WAL, direct I/O, and optimizations still in-progress.

## Features

- **B+ tree storage**: Full keys in branch nodes, efficient range scans
- **MVCC transactions**: Snapshot isolation with copy-on-write
- **ACID guarantees**: Atomic commits, durable writes, isolated reads
- **Crash recovery**: Dual meta pages with CRC32 checksums
- **Cursor API**: Forward/reverse iteration with seek support
- **Concurrent access**: Multiple readers + single writer

## Usage

```go
import "github.com/alexhholmes/fredb/pkg"

// Open database
db, _ := pkg.Open("data.db")
defer db.Close()

// Simple operations
db.Set([]byte("key"), []byte("value"))
value, _ := db.Get([]byte("key"))
db.Delete([]byte("key"))

// Transactions (MVCC with snapshot isolation)
db.View(func(tx *pkg.Tx) error {
    value, err := tx.Get([]byte("key"))
    // Read-only transaction
    return err
})

db.Update(func(tx *pkg.Tx) error {
    tx.Set([]byte("key"), []byte("value"))
    tx.Delete([]byte("old-key"))
    // Auto-commit on success, rollback on error
    return nil
})

// Manual transactions
tx, _ := db.Begin(true) // writable
defer tx.Rollback()
tx.Set([]byte("key"), []byte("value"))
tx.Commit()

// Cursor iteration
tx, _ := db.Begin(false) // read-only
defer tx.Rollback()
cursor := tx.Cursor()
for cursor.Seek([]byte("key")); cursor.Valid(); cursor.Next() {
    key, value := cursor.Key(), cursor.Value()
    // Process key-value pairs
}
```

### Options

#### WAL Sync Modes

WALSyncEveryCommit Mode

- ✅ Zero data loss on power failure
- ✅ tx.Commit() returns only after durable
- ❌ Slow throughput (~200-500 TPS, 4ms/op fsync latency)

WALSyncBytes Mode

- ✅ High throughput
- ⚠️ Data loss window: last bytesPerSync bytes
- ⚠️ tx.Commit() returns before durable
- Recovery: WAL replay stops at first invalid/incomplete record 

WALSyncOff Mode

- ✅ Maximum throughput
- ❌ All unfsynced data lost on crash
- Use case: Testing, bulk loads with external backup

```go
// Maximum durability (default) - BoltDB-style
db, _ := Open("data.db")

// High throughput - RocksDB-style with 1MB sync threshold
db, _ := Open("data.db", WithWALSyncBytes(1024*1024))

// Testing/bulk loads only - no fsync
db, _ := Open("data.db", WithWALSyncOff())
```

## Architecture

```
  ┌───────────────────────────────────────────────────────┐
  │                   PUBLIC API LAYER                    │
  │                    (fredb package)                    │
  │                                                       │
  │  ┌──────────────┐        ┌──────────┐  ┌──────────┐   │
  │  │      DB      │────────│    Tx    │──│  Cursor  │   │
  │  │              │ owns   │          │  │          │   │
  │  │ Entry point, │        │ Snapshot │  │ Iterator │   │
  │  │ MVCC coord.  │        └──────────┘  └──────────┘   │
  │  └──────┬───────┘                                     │
  └─────────┼─────────────────────────────────────────────┘
            │
            │ owns
            │
  ┌─────────┼─────────────────────────────────────────────┐
  │         │         PRIVATE LAYER                       │
  │         │     (fredb package, private)                │
  │         │                                             │
  │         │                     ┌───────────┐           │
  │         │────────────────────►│   btree   │           │
  │         │                     │           │           │
  │         │                     │ B+ tree   │           │
  │         │                     │  logic    │           │
  │         │                     └─────┬─────┘           │
  └─────────│───────────────────────────┼─────────────────┘
            │                           │ owns
            │                           │
  ┌─────────┼───────────────────────────┼─────────────────┐
  │         │       INTERNAL LAYER      │                 │
  │         │    (internal/* packages)  │─────────┐       │
  │         │                           │         │       │
  │         │                   ┌───────▼──────┐  │       │
  │         │                   │  PageCache   │  │       │
  │         │                   │   (cache/)   │  │       │
  │         │                   │ Versioned    │  │       │
  │         │                   │ LRU + MVCC   │  │       │
  │         │                   └───────┬──────┘  │       │
  │         │                           │         │       │
  │         │                           │ uses    │       │
  │         ▼                           ▼         ▼       │
  │  ┌───────────┐     ┌──────────────────────────────┐   │
  │  │    WAL    │────►│       PageManager            │   │
  │  │           │     │       (storage/)             │   │
  │  │ Durability│     │ Disk I/O • FreeList •        │   │
  │  │           │     │              VersionMap      │   │
  │  └───────────┘     └──────────────┬───────────────┘   │
  └───────────────────────────────────┼───────────────────┘
                                      │ uses
  ══════════════════════════════════════════════════════════
  FOUNDATION: internal/base
  ┌───────────────────────────────────────────────────────┐
  │  Node • Page • PageID • MetaPage • PageHeader         │
  │  Wire format types used by ALL layers                 │
  └───────────────────────────────────────────────────────┘
```

## Disk Format

```
Page 0-1:  Meta pages (dual, CRC32 checksums)
Page 2:   Free list (dynamic growth)
Page 3+:  {Free list pages} ⊕ {B-tree data pages}, allocation order preserved
```

## Testing

```bash
go test ./fredb -v
```

Currently: 49 tests passing
- B-tree ops: insert, delete, split, merge, rebalance
- Wire format: byte-level serialization tests
- Concurrency: parallel reads/writes

## Roadmap

- [x] B-tree implementation
- [x] Node serialization
- [x] Meta page format
- [x] Free list design
- [x] Disk page manager
- [x] Cache eviction
- [x] WAL for durability
- [x] MVCC transactions
- [x] Direct I/O support
- [x] Benchmarks

## Inspiration

- **bbolt**: Page layout, unsafe.Pointer serialization
- **BoltDB**: Original design philosophy
- **SQLite**: Robustness patterns

# fredb

A simple embedded B-tree key-value store in Go.

Designed for up to 1TB+, read-heavy, local KV storage for edge, containers, and 
infrastructure.

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

#### Sync Modes

SyncEveryCommit Mode

- ✅ Zero data loss on power failure
- ✅ tx.Commit() returns only after durable
- ❌ Slow throughput (~200-500 TPS, 4ms/op fsync latency)

SyncBytes Mode

- ✅ High throughput
- ⚠️ Data loss window: last bytesPerSync bytes
- ⚠️ tx.Commit() returns before durable
- Recovery: WAL replay stops at first invalid/incomplete record 

SyncOff Mode

- ✅ Maximum throughput
- ❌ All unfsynced data lost on crash
- Use case: Testing, bulk loads with external backup

```go
// Maximum durability (default) - BoltDB-style
db, _ := Open("data.db")

// High throughput - RocksDB-style with 1MB sync threshold
db, _ := Open("data.db", WithSyncBytes(1024*1024))

// Testing/bulk loads only - no fsync
db, _ := Open("data.db", WithSyncOff())
```

## Testing

```bash
go test ./fredb -v
```

- B-tree ops: insert, delete, split, merge, rebalance
- Wire format: byte-level serialization tests
- Concurrency: parallel reads/writes

## Inspiration

- **bbolt**
- **BadgerDB**

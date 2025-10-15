# fredb

A simple embedded B-tree key-value store in Go.

Designed for up to 1TB+, read-heavy, local KV storage for edge, containers, and 
infrastructure.

## Status

**In Development** - Core B-tree operations, disk persistence, MVCC, API, and direct 
I/O are complete. Optimizations still in-progress.

## Features

- **B+ tree storage**: Full keys in branch nodes, efficient range scans
- **MVCC transactions**: Snapshot isolation with copy-on-write
- **ACID guarantees**: Atomic commits, durable writes, isolated reads
- **Crash recovery**: Dual meta pages with CRC32 checksums
- **Cursor API**: Forward/reverse iteration with seek support
- **Concurrent access**: Multiple readers + single writer

## Usage

```go
import "fredb"

// Open database
db, _ := fredb.Open("data.db")
defer db.Close()

// Simple operations (default bucket)
db.Set([]byte("key"), []byte("value"))
value, _ := db.Get([]byte("key"))
db.Delete([]byte("key"))

// Transactions (MVCC with snapshot isolation)
db.View(func(tx *fredb.Tx) error {
    value, _ := tx.Get([]byte("key"))
    // Read-only transaction, auto-rollback
    return nil
})

db.Update(func(tx *fredb.Tx) error {
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

// Buckets (namespaces with separate B+trees)
db.Update(func(tx *fredb.Tx) error {
    // Create bucket
    bucket, _ := tx.CreateBucket([]byte("users"))
    bucket.Put([]byte("alice"), []byte("data"))

    // Get bucket
    bucket = tx.Bucket([]byte("users"))
    value := bucket.Get([]byte("alice"))

    // Delete bucket and all its data
    tx.DeleteBucket([]byte("users"))
    return nil
})

// Cursor iteration
db.View(func(tx *fredb.Tx) error {
    c := tx.Cursor()

    // First to last
    for k, v := c.First(); k != nil; k, v = c.Next() {
        // Process key-value pairs
    }

    // Last to first
    for k, v := c.Last(); k != nil; k, v = c.Prev() {
        // Reverse iteration
    }

    // Seek to key >= "start"
    for k, v := c.Seek([]byte("start")); k != nil; k, v = c.Next() {
        if bytes.Compare(k, []byte("end")) > 0 {
            break
        }
    }

    return nil
})

// ForEach iteration
db.View(func(tx *fredb.Tx) error {
    // Iterate all keys
    tx.ForEach(func(k, v []byte) error {
        // Process each key-value pair
        return nil
    })

    // Iterate keys with prefix
    tx.ForEachPrefix([]byte("user:"), func(k, v []byte) error {
        // Process all keys starting with "user:"
        return nil
    })

    return nil
})

// Bucket sequences (auto-increment IDs)
db.Update(func(tx *fredb.Tx) error {
    bucket := tx.Bucket([]byte("users"))

    id, _ := bucket.NextSequence() // 1
    id, _ = bucket.NextSequence()  // 2

    current := bucket.Sequence()   // 2
    bucket.SetSequence(100)        // Reset to 100

    return nil
})

// Iterate over all buckets
db.View(func(tx *fredb.Tx) error {
    tx.ForEachBucket(func(name []byte, b *fredb.Bucket) error {
        // Process each bucket
        return nil
    })
    return nil
})
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

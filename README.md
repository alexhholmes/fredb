# fredb

Embedded B-tree key-value store in Go.

## Status

**In Development** - Core B-tree operations complete, disk persistence in progress.

## Features

- **B-tree storage**: Order-256 B-tree with efficient splits/merges
- **Simple API**: Get/Set/Delete operations
- **ACID transactions**: (planned) Multi-version concurrency control
- **Crash recovery**: (in progress) WAL + dual meta pages
- **Concurrent**: Read-write mutex for multi-threaded access

## Usage

```go
import "github.com/alexhholmes/fredb"

// Create database (in-memory for now)
db, _ := fredb.NewDB("data.db")
defer db.Close()

// Basic operations
db.Set([]byte("key"), []byte("value"))
value, _ := db.Get([]byte("key"))
db.Delete([]byte("key"))
```

## Architecture

```
┌─────────┐
│   DB    │  Public API + concurrency
└────┬────┘
     │
┌────▼────┐
│  BTree  │  B-tree logic + node cache
└────┬────┘
     │
┌────▼────────┐
│ PageManager │  Disk I/O + free list
└─────────────┘
```

## Disk Format

```
Page 0-1:  Meta pages (dual, CRC32 checksums)
Page 2+:   Free list (dynamic growth)
Rest:      B-tree data pages (4KB each)
```

## Testing

```bash
go test ./src -v
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
- [ ] Cache eviction
- [ ] WAL for durability
- [ ] MVCC transactions
- [ ] Benchmarks

## Inspiration

- **bbolt**: Page layout, unsafe.Pointer serialization
- **BoltDB**: Original design philosophy
- **SQLite**: Robustness patterns
package fredb_test

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/cockroachdb/pebble"
	bolt "go.etcd.io/bbolt"
	_ "modernc.org/sqlite"

	"fredb"
)

const (
	benchValueSize  = 1024
	benchNumRecords = 10000
)

// Write Benchmarks

func BenchmarkSequentialWrite(b *testing.B) {
	b.Run("Fredb/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_seq_write_fredb_sync.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithSyncEveryCommit())
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Update(func(tx *fredb.Tx) error {
				return tx.Set(key, value)
			})
		}
	})

	b.Run("Fredb/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_seq_write_fredb_nosync.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithSyncOff())
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Update(func(tx *fredb.Tx) error {
				return tx.Set(key, value)
			})
		}
	})

	b.Run("Bbolt/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_seq_write_bbolt_sync.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: false})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("test")).Put(key, value)
			})
		}
	})

	b.Run("Bbolt/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_seq_write_bbolt_nosync.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: true})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket([]byte("test")).Put(key, value)
			})
		}
	})

	b.Run("Badger/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_seq_write_badger_sync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithSyncWrites(true).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			})
		}
	})

	b.Run("Badger/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_seq_write_badger_nosync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithSyncWrites(false).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, value)
			})
		}
	})

	b.Run("Pebble/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_seq_write_pebble_sync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Set(key, value, pebble.Sync)
		}
	})

	b.Run("Pebble/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_seq_write_pebble_nosync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Set(key, value, pebble.NoSync)
		}
	})

	b.Run("SQLite/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_seq_write_sqlite_sync.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA synchronous=FULL")
		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
		}
	})

	b.Run("SQLite/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_seq_write_sqlite_nosync.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA synchronous=OFF")
		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i))
			db.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
		}
	})
}

func BenchmarkBatchWrite(b *testing.B) {
	batchSize := 1000

	b.Run("Fredb/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_fredb_sync.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithSyncEveryCommit())
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
					if err := tx.Set(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})

	b.Run("Fredb/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_fredb_nosync.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithSyncOff())
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
					if err := tx.Set(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})

	b.Run("Bbolt/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_bbolt_sync.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: false})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("test"))
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
					if err := bucket.Put(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})

	b.Run("Bbolt/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_bbolt_nosync.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: true})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("test"))
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
					if err := bucket.Put(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})

	b.Run("Badger/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_badger_sync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithSyncWrites(true).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			db.Update(func(txn *badger.Txn) error {
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
					if err := txn.Set(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})

	b.Run("Badger/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_badger_nosync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithSyncWrites(false).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			db.Update(func(txn *badger.Txn) error {
				for j := 0; j < batchSize; j++ {
					key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
					if err := txn.Set(key, value); err != nil {
						return err
					}
				}
				return nil
			})
		}
	})

	b.Run("Pebble/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_pebble_sync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			batch := db.NewBatch()
			for j := 0; j < batchSize; j++ {
				key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
				batch.Set(key, value, nil)
			}
			db.Apply(batch, pebble.Sync)
			batch.Close()
		}
	})

	b.Run("Pebble/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_pebble_nosync.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			batch := db.NewBatch()
			for j := 0; j < batchSize; j++ {
				key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
				batch.Set(key, value, nil)
			}
			db.Apply(batch, pebble.NoSync)
			batch.Close()
		}
	})

	b.Run("SQLite/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_sqlite_sync.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA synchronous=FULL")
		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			tx, _ := db.Begin()
			for j := 0; j < batchSize; j++ {
				key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
				tx.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
			}
			tx.Commit()
		}
	})

	b.Run("SQLite/SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_sqlite_nosync.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA synchronous=OFF")
		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batchStart := i * batchSize
			tx, _ := db.Begin()
			for j := 0; j < batchSize; j++ {
				key := []byte(fmt.Sprintf("key-%020d", batchStart+j))
				tx.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
			}
			tx.Commit()
		}
	})
}

// Read Benchmarks

func BenchmarkSequentialRead(b *testing.B) {
	b.Run("Fredb", func(b *testing.B) {
		path := "/tmp/bench_seq_read_fredb.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithMaxCacheSizeMB(1024))
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					tx.Set(key, value)
				}
				return nil
			})
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i%benchNumRecords))
			db.View(func(tx *fredb.Tx) error {
				tx.Get(key)
				return nil
			})
		}
	})

	b.Run("Bbolt", func(b *testing.B) {
		path := "/tmp/bench_seq_read_bbolt.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, nil)
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("test"))
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					bucket.Put(key, value)
				}
				return nil
			})
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i%benchNumRecords))
			db.View(func(tx *bolt.Tx) error {
				tx.Bucket([]byte("test")).Get(key)
				return nil
			})
		}
	})

	b.Run("Badger", func(b *testing.B) {
		path := "/tmp/bench_seq_read_badger.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(txn *badger.Txn) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					txn.Set(key, value)
				}
				return nil
			})
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i%benchNumRecords))
			db.View(func(txn *badger.Txn) error {
				txn.Get(key)
				return nil
			})
		}
	})

	b.Run("Pebble", func(b *testing.B) {
		path := "/tmp/bench_seq_read_pebble.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			batch := db.NewBatch()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				batch.Set(key, value, nil)
			}
			db.Apply(batch, pebble.Sync)
			batch.Close()
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i%benchNumRecords))
			db.Get(key)
		}
	})

	b.Run("SQLite", func(b *testing.B) {
		path := "/tmp/bench_seq_read_sqlite.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			tx, _ := db.Begin()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				tx.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", key, value)
			}
			tx.Commit()
		}

		stmt, _ := db.Prepare("SELECT value FROM kv WHERE key = ?")
		defer stmt.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("key-%020d", i%benchNumRecords))
			stmt.Query(key)
		}
	})
}

func BenchmarkRandomRead(b *testing.B) {
	b.Run("Fredb", func(b *testing.B) {
		path := "/tmp/bench_rand_read_fredb.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithMaxCacheSizeMB(1024))
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					tx.Set(key, value)
				}
				return nil
			})
		}

		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			idx := rng.Intn(benchNumRecords)
			key := []byte(fmt.Sprintf("key-%020d", idx))
			db.View(func(tx *fredb.Tx) error {
				tx.Get(key)
				return nil
			})
		}
	})

	b.Run("Bbolt", func(b *testing.B) {
		path := "/tmp/bench_rand_read_bbolt.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, nil)
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("test"))
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					bucket.Put(key, value)
				}
				return nil
			})
		}

		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			idx := rng.Intn(benchNumRecords)
			key := []byte(fmt.Sprintf("key-%020d", idx))
			db.View(func(tx *bolt.Tx) error {
				tx.Bucket([]byte("test")).Get(key)
				return nil
			})
		}
	})

	b.Run("Badger", func(b *testing.B) {
		path := "/tmp/bench_rand_read_badger.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(txn *badger.Txn) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					txn.Set(key, value)
				}
				return nil
			})
		}

		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			idx := rng.Intn(benchNumRecords)
			key := []byte(fmt.Sprintf("key-%020d", idx))
			db.View(func(txn *badger.Txn) error {
				txn.Get(key)
				return nil
			})
		}
	})

	b.Run("Pebble", func(b *testing.B) {
		path := "/tmp/bench_rand_read_pebble.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			batch := db.NewBatch()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				batch.Set(key, value, nil)
			}
			db.Apply(batch, pebble.Sync)
			batch.Close()
		}

		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			idx := rng.Intn(benchNumRecords)
			key := []byte(fmt.Sprintf("key-%020d", idx))
			db.Get(key)
		}
	})

	b.Run("SQLite", func(b *testing.B) {
		path := "/tmp/bench_rand_read_sqlite.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			tx, _ := db.Begin()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				tx.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", key, value)
			}
			tx.Commit()
		}

		stmt, _ := db.Prepare("SELECT value FROM kv WHERE key = ?")
		defer stmt.Close()

		rng := rand.New(rand.NewSource(42))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			idx := rng.Intn(benchNumRecords)
			key := []byte(fmt.Sprintf("key-%020d", idx))
			stmt.Query(key)
		}
	})
}

func BenchmarkConcurrentRead(b *testing.B) {
	b.Run("Fredb", func(b *testing.B) {
		path := "/tmp/bench_conc_read_fredb.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithMaxCacheSizeMB(1024))
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					tx.Set(key, value)
				}
				return nil
			})
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))
				db.View(func(tx *fredb.Tx) error {
					tx.Get(key)
					return nil
				})
			}
		})
	})

	b.Run("Bbolt", func(b *testing.B) {
		path := "/tmp/bench_conc_read_bbolt.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, nil)
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("test"))
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					bucket.Put(key, value)
				}
				return nil
			})
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))
				db.View(func(tx *bolt.Tx) error {
					tx.Bucket([]byte("test")).Get(key)
					return nil
				})
			}
		})
	})

	b.Run("Badger", func(b *testing.B) {
		path := "/tmp/bench_conc_read_badger.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(txn *badger.Txn) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					txn.Set(key, value)
				}
				return nil
			})
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))
				db.View(func(txn *badger.Txn) error {
					txn.Get(key)
					return nil
				})
			}
		})
	})

	b.Run("Pebble", func(b *testing.B) {
		path := "/tmp/bench_conc_read_pebble.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			batch := db.NewBatch()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				batch.Set(key, value, nil)
			}
			db.Apply(batch, pebble.Sync)
			batch.Close()
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))
				db.Get(key)
			}
		})
	})

	b.Run("SQLite", func(b *testing.B) {
		path := "/tmp/bench_conc_read_sqlite.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			tx, _ := db.Begin()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				tx.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", key, value)
			}
			tx.Commit()
		}

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			stmt, _ := db.Prepare("SELECT value FROM kv WHERE key = ?")
			defer stmt.Close()
			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))
				stmt.Query(key)
			}
		})
	})
}

func BenchmarkReadWriteMix(b *testing.B) {
	b.Run("Fredb", func(b *testing.B) {
		path := "/tmp/bench_rw_mix_fredb.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithMaxCacheSizeMB(1024), fredb.WithSyncEveryCommit())
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					tx.Set(key, value)
				}
				return nil
			})
		}

		var totalWriteNs int64
		var writeCount int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			writeCounter := 0
			localWriteNs := int64(0)
			localWriteCount := int64(0)

			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))

				// 80% reads, 20% writes
				if writeCounter%5 == 0 {
					start := time.Now().UnixNano()
					db.Update(func(tx *fredb.Tx) error {
						return tx.Set(key, value)
					})
					localWriteNs += time.Now().UnixNano() - start
					localWriteCount++
				} else {
					db.View(func(tx *fredb.Tx) error {
						tx.Get(key)
						return nil
					})
				}
				writeCounter++
			}

			atomic.AddInt64(&totalWriteNs, localWriteNs)
			atomic.AddInt64(&writeCount, localWriteCount)
		})

		if writeCount > 0 {
			b.ReportMetric(float64(totalWriteNs)/float64(writeCount), "write-ns/op")
			b.ReportMetric(float64(writeCount), "writes")
		}
	})

	b.Run("Bbolt", func(b *testing.B) {
		path := "/tmp/bench_rw_mix_bbolt.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: false})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(tx *bolt.Tx) error {
				bucket := tx.Bucket([]byte("test"))
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					bucket.Put(key, value)
				}
				return nil
			})
		}

		var totalWriteNs int64
		var writeCount int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			writeCounter := 0
			localWriteNs := int64(0)
			localWriteCount := int64(0)

			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))

				// 80% reads, 20% writes
				if writeCounter%5 == 0 {
					start := time.Now().UnixNano()
					db.Update(func(tx *bolt.Tx) error {
						return tx.Bucket([]byte("test")).Put(key, value)
					})
					localWriteNs += time.Now().UnixNano() - start
					localWriteCount++
				} else {
					db.View(func(tx *bolt.Tx) error {
						tx.Bucket([]byte("test")).Get(key)
						return nil
					})
				}
				writeCounter++
			}

			atomic.AddInt64(&totalWriteNs, localWriteNs)
			atomic.AddInt64(&writeCount, localWriteCount)
		})

		if writeCount > 0 {
			b.ReportMetric(float64(totalWriteNs)/float64(writeCount), "write-ns/op")
			b.ReportMetric(float64(writeCount), "writes")
		}
	})

	b.Run("Badger", func(b *testing.B) {
		path := "/tmp/bench_rw_mix_badger.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		opts := badger.DefaultOptions(path).WithSyncWrites(true).WithLogger(nil)
		db, _ := badger.Open(opts)
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			db.Update(func(txn *badger.Txn) error {
				for j := 0; j < 100 && i+j < benchNumRecords; j++ {
					key := []byte(fmt.Sprintf("key-%020d", i+j))
					txn.Set(key, value)
				}
				return nil
			})
		}

		var totalWriteNs int64
		var writeCount int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			writeCounter := 0
			localWriteNs := int64(0)
			localWriteCount := int64(0)

			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))

				// 80% reads, 20% writes
				if writeCounter%5 == 0 {
					start := time.Now().UnixNano()
					db.Update(func(txn *badger.Txn) error {
						return txn.Set(key, value)
					})
					localWriteNs += time.Now().UnixNano() - start
					localWriteCount++
				} else {
					db.View(func(txn *badger.Txn) error {
						txn.Get(key)
						return nil
					})
				}
				writeCounter++
			}

			atomic.AddInt64(&totalWriteNs, localWriteNs)
			atomic.AddInt64(&writeCount, localWriteCount)
		})

		if writeCount > 0 {
			b.ReportMetric(float64(totalWriteNs)/float64(writeCount), "write-ns/op")
			b.ReportMetric(float64(writeCount), "writes")
		}
	})

	b.Run("Pebble", func(b *testing.B) {
		path := "/tmp/bench_rw_mix_pebble.db"
		os.RemoveAll(path)
		defer os.RemoveAll(path)

		db, _ := pebble.Open(path, &pebble.Options{Logger: nil})
		defer db.Close()

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			batch := db.NewBatch()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				batch.Set(key, value, nil)
			}
			db.Apply(batch, pebble.Sync)
			batch.Close()
		}

		var totalWriteNs int64
		var writeCount int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			writeCounter := 0
			localWriteNs := int64(0)
			localWriteCount := int64(0)

			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))

				// 80% reads, 20% writes
				if writeCounter%5 == 0 {
					start := time.Now().UnixNano()
					db.Set(key, value, pebble.Sync)
					localWriteNs += time.Now().UnixNano() - start
					localWriteCount++
				} else {
					db.Get(key)
				}
				writeCounter++
			}

			atomic.AddInt64(&totalWriteNs, localWriteNs)
			atomic.AddInt64(&writeCount, localWriteCount)
		})

		if writeCount > 0 {
			b.ReportMetric(float64(totalWriteNs)/float64(writeCount), "write-ns/op")
			b.ReportMetric(float64(writeCount), "writes")
		}
	})

	b.Run("SQLite", func(b *testing.B) {
		path := "/tmp/bench_rw_mix_sqlite.db"
		os.Remove(path)
		defer os.Remove(path)

		db, _ := sql.Open("sqlite", path)
		defer db.Close()

		db.Exec("PRAGMA synchronous=FULL")
		db.Exec("PRAGMA journal_mode=WAL")
		db.Exec("CREATE TABLE kv (key BLOB PRIMARY KEY, value BLOB)")

		value := make([]byte, benchValueSize)
		for i := 0; i < benchNumRecords; i += 100 {
			tx, _ := db.Begin()
			for j := 0; j < 100 && i+j < benchNumRecords; j++ {
				key := []byte(fmt.Sprintf("key-%020d", i+j))
				tx.Exec("INSERT INTO kv (key, value) VALUES (?, ?)", key, value)
			}
			tx.Commit()
		}

		var totalWriteNs int64
		var writeCount int64

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			rng := rand.New(rand.NewSource(42))
			writeCounter := 0
			localWriteNs := int64(0)
			localWriteCount := int64(0)
			stmt, _ := db.Prepare("SELECT value FROM kv WHERE key = ?")
			defer stmt.Close()

			for pb.Next() {
				idx := rng.Intn(benchNumRecords)
				key := []byte(fmt.Sprintf("key-%020d", idx))

				// 80% reads, 20% writes
				if writeCounter%5 == 0 {
					start := time.Now().UnixNano()
					tx, _ := db.Begin()
					tx.Exec("INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)", key, value)
					tx.Commit()
					localWriteNs += time.Now().UnixNano() - start
					localWriteCount++
				} else {
					stmt.Query(key)
				}
				writeCounter++
			}

			atomic.AddInt64(&totalWriteNs, localWriteNs)
			atomic.AddInt64(&writeCount, localWriteCount)
		})

		if writeCount > 0 {
			b.ReportMetric(float64(totalWriteNs)/float64(writeCount), "write-ns/op")
			b.ReportMetric(float64(writeCount), "writes")
		}
	})
}
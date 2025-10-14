package fredb_test

import (
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"fredb"
)

const (
	benchValueSize  = 1024
	benchNumRecords = 10000
)

// Write Benchmarks

func BenchmarkSequentialWrite_Fredb(b *testing.B) {
	b.Run("SyncOn", func(b *testing.B) {
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

	b.Run("SyncOff", func(b *testing.B) {
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
}

func BenchmarkSequentialWrite_Bbolt(b *testing.B) {
	b.Run("SyncOn", func(b *testing.B) {
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

	b.Run("SyncOff", func(b *testing.B) {
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
}

func BenchmarkBatchWrite_Fredb(b *testing.B) {
	b.Run("SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_fredb_sync.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithSyncEveryCommit())
		defer db.Close()

		value := make([]byte, benchValueSize)
		batchSize := 1000
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

	b.Run("SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_fredb_nosync.db"
		defer os.Remove(path)

		db, _ := fredb.Open(path, fredb.WithSyncOff())
		defer db.Close()

		value := make([]byte, benchValueSize)
		batchSize := 1000
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
}

func BenchmarkBatchWrite_Bbolt(b *testing.B) {
	b.Run("SyncOn", func(b *testing.B) {
		path := "/tmp/bench_batch_write_bbolt_sync.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: false})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		batchSize := 1000
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

	b.Run("SyncOff", func(b *testing.B) {
		path := "/tmp/bench_batch_write_bbolt_nosync.db"
		defer os.Remove(path)

		db, _ := bolt.Open(path, 0600, &bolt.Options{NoSync: true})
		defer db.Close()

		db.Update(func(tx *bolt.Tx) error {
			tx.CreateBucket([]byte("test"))
			return nil
		})

		value := make([]byte, benchValueSize)
		batchSize := 1000
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
}

// Read Benchmarks

func BenchmarkSequentialRead_Fredb(b *testing.B) {
	path := "/tmp/bench_seq_read_fredb.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkSequentialRead_Bbolt(b *testing.B) {
	path := "/tmp/bench_seq_read_bbolt.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkRandomRead_Fredb(b *testing.B) {
	path := "/tmp/bench_rand_read_fredb.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkRandomRead_Bbolt(b *testing.B) {
	path := "/tmp/bench_rand_read_bbolt.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkConcurrentRead_Fredb(b *testing.B) {
	path := "/tmp/bench_conc_read_fredb.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkConcurrentRead_Bbolt(b *testing.B) {
	path := "/tmp/bench_conc_read_bbolt.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkReadWriteMix_Fredb(b *testing.B) {
	path := "/tmp/bench_rw_mix_fredb.db"
	defer os.Remove(path)

	// Populate
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
}

func BenchmarkReadWriteMix_Bbolt(b *testing.B) {
	path := "/tmp/bench_rw_mix_bbolt.db"
	defer os.Remove(path)

	// Populate
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
}

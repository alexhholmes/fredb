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

// 10GB Stress Test - Mixed Read/Write Workload
// Target: ~10GB of data
// Workload: 70% reads, 30% writes

const (
	stressTestBatchSize = 10_000           // Insert in batches
	stressTestDuration  = 60 * time.Second // Run for 60 seconds
)

var stressTestSizes = []struct {
	name       string
	valueSize  int
	numRecords int
}{
	{"256B", 256, 40_000_000}, // 40M × 256B ≈ 10GB
	// {"512B", 512, 20_000_000}, // 20M × 512B ≈ 10GB
	// {"1KB", 1024, 10_000_000}, // 10M × 1KB = 10GB
	// {"2KB", 2048, 5_000_000},  // 5M × 2KB = 10GB
}

func BenchmarkStress10GB_Fredb_SyncOn(b *testing.B) {
	for _, size := range stressTestSizes {
		b.Run(size.name, func(b *testing.B) {
			path := fmt.Sprintf("/tmp/bench_stress_10gb_fredb_sync_%s.db", size.name)
			os.Remove(path)
			defer os.Remove(path)

			db, err := fredb.Open(path, fredb.WithSyncEveryCommit(),
				fredb.WithMaxCacheSizeMB(1024))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Populate 10GB
			b.Logf("Populating 10GB of data (%d records × %dB)...", size.numRecords, size.valueSize)
			value := make([]byte, size.valueSize)
			for i := 0; i < len(value); i++ {
				value[i] = byte(i % 256)
			}

			populateStart := time.Now()
			for i := 0; i < size.numRecords; i += stressTestBatchSize {
				if err := db.Update(func(tx *fredb.Tx) error {
					for j := 0; j < stressTestBatchSize && i+j < size.numRecords; j++ {
						key := []byte(fmt.Sprintf("key-%020d", i+j))
						if err := tx.Set(key, value); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}

				if (i/stressTestBatchSize)%100 == 0 {
					progress := float64(i) / float64(size.numRecords) * 100
					b.Logf("Progress: %.1f%% (%d/%d records)", progress, i, size.numRecords)
				}
			}
			populateDuration := time.Since(populateStart)
			b.Logf("Population complete in %v (%.2f MB/s)", populateDuration,
				float64(size.numRecords*size.valueSize)/populateDuration.Seconds()/1024/1024)

			// Mixed workload: 70% reads, 30% writes
			var totalReads, totalWrites atomic.Int64
			var totalReadNs, totalWriteNs atomic.Int64

			b.ResetTimer()
			timeout := time.After(stressTestDuration)
			done := make(chan struct{})

			go func() {
				<-timeout
				close(done)
			}()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				localReads, localWrites := int64(0), int64(0)
				localReadNs, localWriteNs := int64(0), int64(0)

				for pb.Next() {
					select {
					case <-done:
						totalReads.Add(localReads)
						totalWrites.Add(localWrites)
						totalReadNs.Add(localReadNs)
						totalWriteNs.Add(localWriteNs)
						return
					default:
					}

					idx := rng.Intn(size.numRecords)
					key := []byte(fmt.Sprintf("key-%020d", idx))

					// 70% reads, 30% writes
					if rng.Intn(10) < 7 {
						start := time.Now()
						_ = db.View(func(tx *fredb.Tx) error {
							_, _ = tx.Get(key)
							return nil
						})
						localReadNs += time.Since(start).Nanoseconds()
						localReads++
					} else {
						start := time.Now()
						_ = db.Update(func(tx *fredb.Tx) error {
							return tx.Set(key, value)
						})
						localWriteNs += time.Since(start).Nanoseconds()
						localWrites++
					}
				}

				totalReads.Add(localReads)
				totalWrites.Add(localWrites)
				totalReadNs.Add(localReadNs)
				totalWriteNs.Add(localWriteNs)
			})

			reads, writes := totalReads.Load(), totalWrites.Load()
			readNs, writeNs := totalReadNs.Load(), totalWriteNs.Load()

			b.ReportMetric(float64(reads+writes)/stressTestDuration.Seconds(), "ops/sec")
			if reads > 0 {
				b.ReportMetric(float64(readNs)/float64(reads)/1e6, "read-ms/op")
			}
			if writes > 0 {
				b.ReportMetric(float64(writeNs)/float64(writes)/1e6, "write-ms/op")
			}
			b.ReportMetric(float64(reads), "total-reads")
			b.ReportMetric(float64(writes), "total-writes")
		})
	}
}

func BenchmarkStress10GB_Fredb_SyncOff(b *testing.B) {
	for _, size := range stressTestSizes {
		b.Run(size.name, func(b *testing.B) {
			path := fmt.Sprintf("/tmp/bench_stress_10gb_fredb_nosync_%s.db", size.name)
			os.Remove(path)
			defer os.Remove(path)

			db, err := fredb.Open(path, fredb.WithSyncOff(),
				fredb.WithMaxCacheSizeMB(1024))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Populate 10GB
			b.Logf("Populating 10GB of data (%d records × %dB)...", size.numRecords, size.valueSize)
			value := make([]byte, size.valueSize)
			for i := 0; i < len(value); i++ {
				value[i] = byte(i % 256)
			}

			populateStart := time.Now()
			for i := 0; i < size.numRecords; i += stressTestBatchSize {
				if err := db.Update(func(tx *fredb.Tx) error {
					for j := 0; j < stressTestBatchSize && i+j < size.numRecords; j++ {
						key := []byte(fmt.Sprintf("key-%020d", i+j))
						if err := tx.Set(key, value); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}

				if (i/stressTestBatchSize)%100 == 0 {
					progress := float64(i) / float64(size.numRecords) * 100
					b.Logf("Progress: %.1f%% (%d/%d records)", progress, i, size.numRecords)
				}
			}
			populateDuration := time.Since(populateStart)
			b.Logf("Population complete in %v (%.2f MB/s)", populateDuration,
				float64(size.numRecords*size.valueSize)/populateDuration.Seconds()/1024/1024)

			// Mixed workload: 70% reads, 30% writes
			var totalReads, totalWrites atomic.Int64
			var totalReadNs, totalWriteNs atomic.Int64

			b.ResetTimer()
			timeout := time.After(stressTestDuration)
			done := make(chan struct{})

			go func() {
				<-timeout
				close(done)
			}()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				localReads, localWrites := int64(0), int64(0)
				localReadNs, localWriteNs := int64(0), int64(0)

				for pb.Next() {
					select {
					case <-done:
						totalReads.Add(localReads)
						totalWrites.Add(localWrites)
						totalReadNs.Add(localReadNs)
						totalWriteNs.Add(localWriteNs)
						return
					default:
					}

					idx := rng.Intn(size.numRecords)
					key := []byte(fmt.Sprintf("key-%020d", idx))

					// 70% reads, 30% writes
					if rng.Intn(10) < 7 {
						start := time.Now()
						_ = db.View(func(tx *fredb.Tx) error {
							_, _ = tx.Get(key)
							return nil
						})
						localReadNs += time.Since(start).Nanoseconds()
						localReads++
					} else {
						start := time.Now()
						_ = db.Update(func(tx *fredb.Tx) error {
							return tx.Set(key, value)
						})
						localWriteNs += time.Since(start).Nanoseconds()
						localWrites++
					}
				}

				totalReads.Add(localReads)
				totalWrites.Add(localWrites)
				totalReadNs.Add(localReadNs)
				totalWriteNs.Add(localWriteNs)
			})

			reads, writes := totalReads.Load(), totalWrites.Load()
			readNs, writeNs := totalReadNs.Load(), totalWriteNs.Load()

			b.ReportMetric(float64(reads+writes)/stressTestDuration.Seconds(), "ops/sec")
			if reads > 0 {
				b.ReportMetric(float64(readNs)/float64(reads)/1e6, "read-ms/op")
			}
			if writes > 0 {
				b.ReportMetric(float64(writeNs)/float64(writes)/1e6, "write-ms/op")
			}
			b.ReportMetric(float64(reads), "total-reads")
			b.ReportMetric(float64(writes), "total-writes")
		})
	}
}

func BenchmarkStress10GB_Bbolt_SyncOn(b *testing.B) {
	for _, size := range stressTestSizes {
		b.Run(size.name, func(b *testing.B) {
			path := fmt.Sprintf("/tmp/bench_stress_10gb_bbolt_sync_%s.db", size.name)
			os.Remove(path)
			defer os.Remove(path)

			db, err := bolt.Open(path, 0600, &bolt.Options{NoSync: false})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			if err := db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("test"))
				return err
			}); err != nil {
				b.Fatal(err)
			}

			// Populate 10GB
			b.Logf("Populating 10GB of data (%d records × %dB)...", size.numRecords, size.valueSize)
			value := make([]byte, size.valueSize)
			for i := 0; i < len(value); i++ {
				value[i] = byte(i % 256)
			}

			populateStart := time.Now()
			for i := 0; i < size.numRecords; i += stressTestBatchSize {
				if err := db.Update(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte("test"))
					for j := 0; j < stressTestBatchSize && i+j < size.numRecords; j++ {
						key := []byte(fmt.Sprintf("key-%020d", i+j))
						if err := bucket.Put(key, value); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}

				if (i/stressTestBatchSize)%100 == 0 {
					progress := float64(i) / float64(size.numRecords) * 100
					b.Logf("Progress: %.1f%% (%d/%d records)", progress, i, size.numRecords)
				}
			}
			populateDuration := time.Since(populateStart)
			b.Logf("Population complete in %v (%.2f MB/s)", populateDuration,
				float64(size.numRecords*size.valueSize)/populateDuration.Seconds()/1024/1024)

			// Mixed workload: 70% reads, 30% writes
			var totalReads, totalWrites atomic.Int64
			var totalReadNs, totalWriteNs atomic.Int64

			b.ResetTimer()
			timeout := time.After(stressTestDuration)
			done := make(chan struct{})

			go func() {
				<-timeout
				close(done)
			}()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				localReads, localWrites := int64(0), int64(0)
				localReadNs, localWriteNs := int64(0), int64(0)

				for pb.Next() {
					select {
					case <-done:
						totalReads.Add(localReads)
						totalWrites.Add(localWrites)
						totalReadNs.Add(localReadNs)
						totalWriteNs.Add(localWriteNs)
						return
					default:
					}

					idx := rng.Intn(size.numRecords)
					key := []byte(fmt.Sprintf("key-%020d", idx))

					// 70% reads, 30% writes
					if rng.Intn(10) < 7 {
						start := time.Now()
						_ = db.View(func(tx *bolt.Tx) error {
							_ = tx.Bucket([]byte("test")).Get(key)
							return nil
						})
						localReadNs += time.Since(start).Nanoseconds()
						localReads++
					} else {
						start := time.Now()
						_ = db.Update(func(tx *bolt.Tx) error {
							return tx.Bucket([]byte("test")).Put(key, value)
						})
						localWriteNs += time.Since(start).Nanoseconds()
						localWrites++
					}
				}

				totalReads.Add(localReads)
				totalWrites.Add(localWrites)
				totalReadNs.Add(localReadNs)
				totalWriteNs.Add(localWriteNs)
			})

			reads, writes := totalReads.Load(), totalWrites.Load()
			readNs, writeNs := totalReadNs.Load(), totalWriteNs.Load()

			b.ReportMetric(float64(reads+writes)/stressTestDuration.Seconds(), "ops/sec")
			if reads > 0 {
				b.ReportMetric(float64(readNs)/float64(reads)/1e6, "read-ms/op")
			}
			if writes > 0 {
				b.ReportMetric(float64(writeNs)/float64(writes)/1e6, "write-ms/op")
			}
			b.ReportMetric(float64(reads), "total-reads")
			b.ReportMetric(float64(writes), "total-writes")
		})
	}
}

func BenchmarkStress10GB_Bbolt_SyncOff(b *testing.B) {
	for _, size := range stressTestSizes {
		b.Run(size.name, func(b *testing.B) {
			path := fmt.Sprintf("/tmp/bench_stress_10gb_bbolt_nosync_%s.db", size.name)
			os.Remove(path)
			defer os.Remove(path)

			db, err := bolt.Open(path, 0600, &bolt.Options{NoSync: true})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			if err := db.Update(func(tx *bolt.Tx) error {
				_, err := tx.CreateBucket([]byte("test"))
				return err
			}); err != nil {
				b.Fatal(err)
			}

			// Populate 10GB
			b.Logf("Populating 10GB of data (%d records × %dB)...", size.numRecords, size.valueSize)
			value := make([]byte, size.valueSize)
			for i := 0; i < len(value); i++ {
				value[i] = byte(i % 256)
			}

			populateStart := time.Now()
			for i := 0; i < size.numRecords; i += stressTestBatchSize {
				if err := db.Update(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte("test"))
					for j := 0; j < stressTestBatchSize && i+j < size.numRecords; j++ {
						key := []byte(fmt.Sprintf("key-%020d", i+j))
						if err := bucket.Put(key, value); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}

				if (i/stressTestBatchSize)%100 == 0 {
					progress := float64(i) / float64(size.numRecords) * 100
					b.Logf("Progress: %.1f%% (%d/%d records)", progress, i, size.numRecords)
				}
			}
			populateDuration := time.Since(populateStart)
			b.Logf("Population complete in %v (%.2f MB/s)", populateDuration,
				float64(size.numRecords*size.valueSize)/populateDuration.Seconds()/1024/1024)

			// Mixed workload: 70% reads, 30% writes
			var totalReads, totalWrites atomic.Int64
			var totalReadNs, totalWriteNs atomic.Int64

			b.ResetTimer()
			timeout := time.After(stressTestDuration)
			done := make(chan struct{})

			go func() {
				<-timeout
				close(done)
			}()

			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				localReads, localWrites := int64(0), int64(0)
				localReadNs, localWriteNs := int64(0), int64(0)

				for pb.Next() {
					select {
					case <-done:
						totalReads.Add(localReads)
						totalWrites.Add(localWrites)
						totalReadNs.Add(localReadNs)
						totalWriteNs.Add(localWriteNs)
						return
					default:
					}

					idx := rng.Intn(size.numRecords)
					key := []byte(fmt.Sprintf("key-%020d", idx))

					// 70% reads, 30% writes
					if rng.Intn(10) < 7 {
						start := time.Now()
						_ = db.View(func(tx *bolt.Tx) error {
							_ = tx.Bucket([]byte("test")).Get(key)
							return nil
						})
						localReadNs += time.Since(start).Nanoseconds()
						localReads++
					} else {
						start := time.Now()
						_ = db.Update(func(tx *bolt.Tx) error {
							return tx.Bucket([]byte("test")).Put(key, value)
						})
						localWriteNs += time.Since(start).Nanoseconds()
						localWrites++
					}
				}

				totalReads.Add(localReads)
				totalWrites.Add(localWrites)
				totalReadNs.Add(localReadNs)
				totalWriteNs.Add(localWriteNs)
			})

			reads, writes := totalReads.Load(), totalWrites.Load()
			readNs, writeNs := totalReadNs.Load(), totalWriteNs.Load()

			b.ReportMetric(float64(reads+writes)/stressTestDuration.Seconds(), "ops/sec")
			if reads > 0 {
				b.ReportMetric(float64(readNs)/float64(reads)/1e6, "read-ms/op")
			}
			if writes > 0 {
				b.ReportMetric(float64(writeNs)/float64(writes)/1e6, "write-ms/op")
			}
			b.ReportMetric(float64(reads), "total-reads")
			b.ReportMetric(float64(writes), "total-writes")
		})
	}
}

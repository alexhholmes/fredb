package fredb

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkDBGet(b *testing.B) {
	tmpfile := "/tmp/bench_db_get.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile, WithWALSyncBytes(1024*1024))
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Pre-populate with 10k keys
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			b.Fatalf("Failed to populate DB: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		keyNum := (i * 7) % numKeys
		key := fmt.Sprintf("key%08d", keyNum)
		_, err := db.Get([]byte(key))
		if err != nil {
			b.Errorf("get failed: %v", err)
		}
	}
}

func BenchmarkDBSet(b *testing.B) {
	tmpfile := "/tmp/bench_db_set.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile, WithWALSyncBytes(1024*1024))
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			b.Errorf("Set failed: %v", err)
		}
	}
}

func BenchmarkDBMixed(b *testing.B) {
	tmpfile := "/tmp/bench_db_mixed.DB"
	defer os.Remove(tmpfile)

	// Use WALSyncBytes mode for better throughput during pre-population
	db, err := Open(tmpfile, WithWALSyncBytes(1024*1024))
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Pre-populate with 10k keys (reduced from 100k for faster benchmark)
	numKeys := 10000
	batchSize := 100
	for batch := 0; batch < numKeys/batchSize; batch++ {
		tx, err := db.Begin(true)
		if err != nil {
			b.Fatalf("Failed to begin transaction: %v", err)
		}

		for i := 0; i < batchSize; i++ {
			idx := batch*batchSize + i
			key := fmt.Sprintf("key%08d", idx)
			value := fmt.Sprintf("value%08d", idx)
			if err := tx.Set([]byte(key), []byte(value)); err != nil {
				tx.Rollback()
				b.Fatalf("Failed to populate DB: %v", err)
			}
		}

		if err := tx.Commit(); err != nil {
			b.Fatalf("Failed to commit batch: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%5 < 4 {
			// 80% reads
			keyNum := (i * 7) % numKeys
			key := fmt.Sprintf("key%08d", keyNum)
			_, err := db.Get([]byte(key))
			if err != nil {
				b.Errorf("get failed: %v", err)
			}
		} else {
			// 20% writes
			if i%10 < 9 {
				// Update existing
				keyNum := (i * 13) % numKeys
				key := fmt.Sprintf("key%08d", keyNum)
				value := fmt.Sprintf("updated%08d", i)
				err := db.Set([]byte(key), []byte(value))
				if err != nil {
					b.Errorf("Set failed: %v", err)
				}
			} else {
				// Insert new
				key := fmt.Sprintf("newkey%08d", numKeys+i)
				value := fmt.Sprintf("newvalue%08d", i)
				err := db.Set([]byte(key), []byte(value))
				if err != nil {
					b.Errorf("Set failed: %v", err)
				}
			}
		}
	}
}

func BenchmarkDBConcurrentReads(b *testing.B) {
	tmpfile := "/tmp/bench_db_concurrent_reads.DB"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile, WithWALSyncBytes(1024*1024))
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Pre-populate with 50k keys
	numKeys := 50000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			b.Fatalf("Failed to populate DB: %v", err)
		}
	}

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			keyNum := (i * 7) % numKeys
			key := fmt.Sprintf("key%08d", keyNum)
			_, err := db.Get([]byte(key))
			if err != nil {
				b.Errorf("get failed: %v", err)
			}
			i++
		}
	})
}

func BenchmarkBTreeGet(b *testing.B) {
	// Benchmark get performance
	// - Pre-populate tree with 10000 keys
	// - Measure random get operations
	b.Skip("Not implemented")
}

func BenchmarkBTreeSet(b *testing.B) {
	// Benchmark Set performance
	// - Measure insertion of b.N keys
	// - Include both new keys and updates
	b.Skip("Not implemented")
}

func BenchmarkBTreeMixed(b *testing.B) {
	// Benchmark mixed workload (80% reads, 20% writes)
	// - Pre-populate tree
	// - Run mixed operations
	tmpfile := "/tmp/bench_mixed.DB"
	_ = os.Remove(tmpfile)

	db, err := Open(tmpfile, WithWALSyncBytes(1024*1024))
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer func() {
		_ = db.Close()
		_ = os.Remove(tmpfile)
	}()

	// Pre-populate with 10000 keys
	numKeys := 10000
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key%08d", i)
		value := fmt.Sprintf("value%08d", i)
		err := db.Set([]byte(key), []byte(value))
		if err != nil {
			b.Fatalf("Failed to populate tree: %v", err)
		}
	}

	// Reset timer after setup
	b.ResetTimer()

	// Run mixed workload
	for i := 0; i < b.N; i++ {
		// Use deterministic pattern based on iteration
		if i%5 < 4 {
			// 80% reads - read existing keys
			keyNum := (i * 7) % numKeys // Deterministic key selection
			key := fmt.Sprintf("key%08d", keyNum)

			_, err := db.Get([]byte(key))
			if err != nil {
				b.Errorf("Read failed for key %s: %v", key, err)
			}
		} else {
			// 20% writes - mix of updates and new keys
			if i%10 < 9 {
				// Update existing key
				keyNum := (i * 13) % numKeys
				key := fmt.Sprintf("key%08d", keyNum)
				value := fmt.Sprintf("updated%08d", i)

				err := db.Set([]byte(key), []byte(value))
				if err != nil {
					b.Errorf("Update failed for key %s: %v", key, err)
				}
			} else {
				// Insert new key
				key := fmt.Sprintf("newkey%08d", numKeys+i)
				value := fmt.Sprintf("newvalue%08d", i)

				err := db.Set([]byte(key), []byte(value))
				if err != nil {
					b.Errorf("Insert failed for key %s: %v", key, err)
				}
			}
		}
	}
}

func BenchmarkBTreeSequentialInsert(b *testing.B) {
	// Benchmark sequential insertion pattern
	// - Insert keys in ascending order
	b.Skip("Not implemented")
}

func BenchmarkBTreeRandomInsert(b *testing.B) {
	// Benchmark random insertion pattern
	// - Insert keys in random order
	b.Skip("Not implemented")
}

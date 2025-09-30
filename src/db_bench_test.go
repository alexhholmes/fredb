package src

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkDBGet(b *testing.B) {
	tmpfile := "/tmp/bench_db_get.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Pre-populate with 100k keys
	numKeys := 100000
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
			b.Errorf("Get failed: %v", err)
		}
	}
}

func BenchmarkDBSet(b *testing.B) {
	tmpfile := "/tmp/bench_db_set.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
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
	tmpfile := "/tmp/bench_db_mixed.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
	if err != nil {
		b.Fatalf("Failed to create DB: %v", err)
	}
	defer db.Close()

	// Pre-populate with 100k keys
	numKeys := 100000
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
		if i%5 < 4 {
			// 80% reads
			keyNum := (i * 7) % numKeys
			key := fmt.Sprintf("key%08d", keyNum)
			_, err := db.Get([]byte(key))
			if err != nil {
				b.Errorf("Get failed: %v", err)
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
	tmpfile := "/tmp/bench_db_concurrent_reads.db"
	defer os.Remove(tmpfile)

	db, err := Open(tmpfile)
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
				b.Errorf("Get failed: %v", err)
			}
			i++
		}
	})
}

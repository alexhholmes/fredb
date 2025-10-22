package bench

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alexhholmes/fredb"
)

var (
	batchSize = flag.Int("batch", 1000, "batch size for writes")
	keySize   = flag.Int("keysize", 20, "key size in bytes")
	valueSize = flag.Int("valuesize", 1024, "value size in bytes")
)

func TestLargeWrite(t *testing.T) {
	path := "/tmp/10gb_test.db"
	os.Remove(path)
	defer os.Remove(path)

	db, err := fredb.Open(path, fredb.WithSyncEveryCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	key := make([]byte, *keySize)
	value := make([]byte, *valueSize)

	// Phase 1: Insert 100,000 records
	t.Log("Phase 1: Inserting 100,000 records")
	targetRecords := 100_000
	numBatches := targetRecords / *batchSize

	start := time.Now()
	for i := 0; i < numBatches; i++ {
		err := db.Update(func(tx *fredb.Tx) error {
			for j := 0; j < *batchSize; j++ {
				keyStr := fmt.Sprintf("%0*d", *keySize, i**batchSize+j)
				copy(key, keyStr)
				if err := tx.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}

		if i%2 == 0 {
			stats := db.Stats()
			elapsed := time.Since(start)
			t.Logf("Batch %d/%d: %d keys, freelist=%d, elapsed=%s",
				i, numBatches, (i+1)**batchSize, stats.FreePages, elapsed)
		}
	}

	totalRecords := numBatches * *batchSize
	fillTime := time.Since(start)
	stats := db.Stats()

	// Get file size after Phase 1
	fileInfo, _ := os.Stat(path)
	phase1FileSizeMB := float64(fileInfo.Size()) / (1024 * 1024)

	opsPerSec := float64(totalRecords) / fillTime.Seconds()
	t.Logf("Phase 1 complete: %d keys in %s (%.0f ops/sec), freelist=%d, file size: %.2f MB",
		totalRecords, fillTime, opsPerSec, stats.FreePages, phase1FileSizeMB)

	// Phase 2: Concurrency test (100 readers + 1 writer)
	t.Log("Phase 2: Concurrency test (100 readers, 1 writer)")

	var wg sync.WaitGroup
	done := make(chan struct{})
	readerCount := 100
	testDuration := 30 * time.Second

	// Track metrics
	var readOps, writeOps int64
	var readErrors, writeErrors int64

	// Start 100 readers
	for i := 0; i < readerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
			localReads := 0

			for {
				select {
				case <-done:
					t.Logf("Reader %d: %d reads", id, localReads)
					return
				default:
					// Random read
					keyIdx := rng.Intn(totalRecords)
					keyStr := fmt.Sprintf("%0*d", *keySize, keyIdx)
					var valOk bool
					err := db.View(func(tx *fredb.Tx) error {
						val, _ := tx.Get([]byte(keyStr))
						// Verify we got a real value with expected size
						valOk = val != nil && len(val) == *valueSize
						return nil
					})
					if err != nil || !valOk {
						readErrors++
					} else {
						localReads++
						readOps++
					}
				}
			}
		}(i)
	}

	// Start 1 writer (1 write per tx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		localWrites := 0
		writeIdx := totalRecords

		for {
			select {
			case <-done:
				t.Logf("Writer: %d writes", localWrites)
				return
			default:
				keyStr := fmt.Sprintf("%0*d", *keySize, writeIdx)
				err := db.Update(func(tx *fredb.Tx) error {
					return tx.Put([]byte(keyStr), value)
				})
				if err != nil {
					writeErrors++
				} else {
					localWrites++
					writeOps++
					writeIdx++
				}
			}
		}
	}()

	// Run for duration
	startConcurrency := time.Now()
	time.Sleep(testDuration)
	close(done)
	wg.Wait()
	concurrencyDuration := time.Since(startConcurrency)

	finalStats := db.Stats()

	// Get file size
	fileInfo, _ = os.Stat(path)
	fileSizeGB := float64(fileInfo.Size()) / (1024 * 1024 * 1024)

	t.Logf("Phase 2 complete:")
	t.Logf("  Duration: %s", concurrencyDuration)
	t.Logf("  Read ops: %d (%.0f ops/sec)", readOps, float64(readOps)/concurrencyDuration.Seconds())
	t.Logf("  Write ops: %d (%.0f ops/sec)", writeOps, float64(writeOps)/concurrencyDuration.Seconds())
	t.Logf("  Read errors: %d", readErrors)
	t.Logf("  Write errors: %d", writeErrors)
	t.Logf("  Final freelist: %d", finalStats.FreePages)
	t.Logf("  File size: %.2f GB", fileSizeGB)
}

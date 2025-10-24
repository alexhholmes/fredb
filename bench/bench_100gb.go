package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/alexhholmes/fredb"
)

const (
	BatchSize   = 10_000
	TargetBytes = 100 * 1024 * 1024 * 1024 // 100GB
	ValueSize   = 1024                     // 1KB values
)

func main() {
	dbPath := "/tmp/bench_100gb.db"
	os.RemoveAll(dbPath)

	db, err := fredb.Open(dbPath, fredb.WithSyncOff())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	totalRecords := TargetBytes / (8 + ValueSize) // key(8) + value
	totalTx := (totalRecords + BatchSize - 1) / BatchSize

	fmt.Printf("Target: 100GB, Records: %d, Transactions: %d, Batch: %d\n\n",
		totalRecords, totalTx, BatchSize)

	value := make([]byte, ValueSize)
	for i := range value {
		value[i] = byte(i % 256)
	}

	start := time.Now()
	lastPrint := start
	txCount := uint64(0)
	recCount := uint64(0)

	for recCount < uint64(totalRecords) {
		err := db.Update(func(tx *fredb.Tx) error {
			b := tx.Bucket([]byte("bench"))
			if b == nil {
				var err error
				b, err = tx.CreateBucket([]byte("bench"))
				if err != nil {
					return err
				}
			}

			batchEnd := recCount + BatchSize
			if batchEnd > uint64(totalRecords) {
				batchEnd = uint64(totalRecords)
			}

			for i := recCount; i < batchEnd; i++ {
				key := make([]byte, 8)
				binary.BigEndian.PutUint64(key, i)
				if err := b.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		})

		if err != nil {
			panic(err)
		}

		recCount += BatchSize
		txCount++

		now := time.Now()
		if now.Sub(lastPrint) >= time.Second {
			elapsed := now.Sub(start).Seconds()
			txPerSec := float64(txCount) / elapsed
			recPerSec := float64(recCount) / elapsed
			gbWritten := float64(recCount*(8+ValueSize)) / (1024 * 1024 * 1024)

			fmt.Printf("\rTx: %d (%.0f tx/s) | Records: %d (%.0f rec/s) | %.2f GB",
				txCount, txPerSec, recCount, recPerSec, gbWritten)
			lastPrint = now
		}
	}

	elapsed := time.Since(start).Seconds()
	txPerSec := float64(txCount) / elapsed
	recPerSec := float64(recCount) / elapsed
	gbWritten := float64(recCount*(8+ValueSize)) / (1024 * 1024 * 1024)

	fmt.Printf("\n\nCompleted:\n")
	fmt.Printf("  Time:     %.2fs\n", elapsed)
	fmt.Printf("  Tx:       %d (%.0f tx/s)\n", txCount, txPerSec)
	fmt.Printf("  Records:  %d (%.0f rec/s)\n", recCount, recPerSec)
	fmt.Printf("  Data:     %.2f GB\n", gbWritten)
}

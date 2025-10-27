package bench

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/pebble"
	bolt "go.etcd.io/bbolt"

	"github.com/alexhholmes/fredb"
)

const (
	multiBucketBatchSize   = 1000
	multiBucketNumBuckets  = 10
	multiBucketValueSize   = 32 * 4096
	multiBucketUpdateRatio = 0
	multiBucketDeleteRatio = 0
)

func BenchmarkMultiBucket(b *testing.B) {
	b.Run("Fredb/SyncOn", func(b *testing.B) {
		path := "/tmp/bench_multibucket_fredb.db"
		defer os.Remove(path)

		db, err := fredb.Open(path, fredb.WithSyncEveryCommit())
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		bucketNames := make([][]byte, multiBucketNumBuckets)
		err = db.Update(func(tx *fredb.Tx) error {
			for i := 0; i < multiBucketNumBuckets; i++ {
				name := []byte(fmt.Sprintf("bucket_%02d", i))
				bucketNames[i] = name
				if _, err := tx.CreateBucket(name); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}

		rng := rand.New(rand.NewSource(42))
		value := make([]byte, multiBucketValueSize)
		keysInserted := make(map[string][]uint64)
		for i := 0; i < multiBucketNumBuckets; i++ {
			keysInserted[string(bucketNames[i])] = make([]uint64, 0, b.N/multiBucketNumBuckets)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = db.Update(func(tx *fredb.Tx) error {
				for j := 0; j < multiBucketBatchSize; j++ {
					bucketIdx := rng.Intn(multiBucketNumBuckets)
					bucketName := bucketNames[bucketIdx]
					bucket := tx.Bucket(bucketName)
					if bucket == nil {
						return fmt.Errorf("bucket not found")
					}

					bucketKeys := keysInserted[string(bucketName)]
					roll := rng.Float64()

					if len(bucketKeys) > 0 && roll < multiBucketDeleteRatio {
						// Delete
						keyIdx := rng.Intn(len(bucketKeys))
						keyNum := bucketKeys[keyIdx]
						key := make([]byte, 8)
						binary.BigEndian.PutUint64(key, keyNum)
						if err := bucket.Delete(key); err != nil {
							return err
						}
						keysInserted[string(bucketName)] = append(bucketKeys[:keyIdx], bucketKeys[keyIdx+1:]...)
					} else if len(bucketKeys) > 0 && roll < multiBucketDeleteRatio+multiBucketUpdateRatio {
						// Update
						keyIdx := rng.Intn(len(bucketKeys))
						keyNum := bucketKeys[keyIdx]
						key := make([]byte, 8)
						binary.BigEndian.PutUint64(key, keyNum)
						rng.Read(value)
						if err := bucket.Put(key, value); err != nil {
							return err
						}
					} else {
						// Insert
						keyNum := uint64(len(bucketKeys))
						key := make([]byte, 8)
						binary.BigEndian.PutUint64(key, keyNum)
						rng.Read(value)
						if err := bucket.Put(key, value); err != nil {
							return err
						}
						keysInserted[string(bucketName)] = append(keysInserted[string(bucketName)], keyNum)
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Bbolt/SyncOn", func(b *testing.B) {
		if *benchFredb {
			b.Skip()
		}
		path := "/tmp/bench_multibucket_bbolt.db"
		defer os.Remove(path)

		db, err := bolt.Open(path, 0600, &bolt.Options{NoSync: false})
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		bucketNames := make([][]byte, multiBucketNumBuckets)
		err = db.Update(func(tx *bolt.Tx) error {
			for i := 0; i < multiBucketNumBuckets; i++ {
				name := []byte(fmt.Sprintf("bucket_%02d", i))
				bucketNames[i] = name
				if _, err := tx.CreateBucket(name); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}

		rng := rand.New(rand.NewSource(42))
		value := make([]byte, multiBucketValueSize)
		keysInserted := make(map[string][]uint64)
		for i := 0; i < multiBucketNumBuckets; i++ {
			keysInserted[string(bucketNames[i])] = make([]uint64, 0, b.N/multiBucketNumBuckets)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err = db.Update(func(tx *bolt.Tx) error {
				for j := 0; j < multiBucketBatchSize; j++ {
					bucketIdx := rng.Intn(multiBucketNumBuckets)
					bucketName := bucketNames[bucketIdx]
					bucket := tx.Bucket(bucketName)
					if bucket == nil {
						return fmt.Errorf("bucket not found")
					}

					bucketKeys := keysInserted[string(bucketName)]
					roll := rng.Float64()

					if len(bucketKeys) > 0 && roll < multiBucketDeleteRatio {
						// Delete
						keyIdx := rng.Intn(len(bucketKeys))
						keyNum := bucketKeys[keyIdx]
						key := make([]byte, 8)
						binary.BigEndian.PutUint64(key, keyNum)
						if err := bucket.Delete(key); err != nil {
							return err
						}
						keysInserted[string(bucketName)] = append(bucketKeys[:keyIdx], bucketKeys[keyIdx+1:]...)
					} else if len(bucketKeys) > 0 && roll < multiBucketDeleteRatio+multiBucketUpdateRatio {
						// Update
						keyIdx := rng.Intn(len(bucketKeys))
						keyNum := bucketKeys[keyIdx]
						key := make([]byte, 8)
						binary.BigEndian.PutUint64(key, keyNum)
						rng.Read(value)
						if err := bucket.Put(key, value); err != nil {
							return err
						}
					} else {
						// Insert
						keyNum := uint64(len(bucketKeys))
						key := make([]byte, 8)
						binary.BigEndian.PutUint64(key, keyNum)
						rng.Read(value)
						if err := bucket.Put(key, value); err != nil {
							return err
						}
						keysInserted[string(bucketName)] = append(keysInserted[string(bucketName)], keyNum)
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Pebble/SyncOn", func(b *testing.B) {
		if *benchFredb {
			b.Skip()
		}
		path := "/tmp/bench_multibucket_pebble.db"
		defer os.RemoveAll(path)

		db, err := pebble.Open(path, &pebble.Options{})
		if err != nil {
			b.Fatal(err)
		}
		defer db.Close()

		bucketPrefixes := make([][]byte, multiBucketNumBuckets)
		for i := 0; i < multiBucketNumBuckets; i++ {
			bucketPrefixes[i] = []byte(fmt.Sprintf("bucket_%02d/", i))
		}

		rng := rand.New(rand.NewSource(42))
		value := make([]byte, multiBucketValueSize)
		keysInserted := make(map[int][]uint64)
		for i := 0; i < multiBucketNumBuckets; i++ {
			keysInserted[i] = make([]uint64, 0, b.N/multiBucketNumBuckets)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			batch := db.NewBatch()
			for j := 0; j < multiBucketBatchSize; j++ {
				bucketIdx := rng.Intn(multiBucketNumBuckets)
				prefix := bucketPrefixes[bucketIdx]

				bucketKeys := keysInserted[bucketIdx]
				roll := rng.Float64()

				if len(bucketKeys) > 0 && roll < multiBucketDeleteRatio {
					// Delete
					keyIdx := rng.Intn(len(bucketKeys))
					keyNum := bucketKeys[keyIdx]
					key := make([]byte, len(prefix)+8)
					copy(key, prefix)
					binary.BigEndian.PutUint64(key[len(prefix):], keyNum)
					if err := batch.Delete(key, pebble.Sync); err != nil {
						b.Fatal(err)
					}
					keysInserted[bucketIdx] = append(bucketKeys[:keyIdx], bucketKeys[keyIdx+1:]...)
				} else if len(bucketKeys) > 0 && roll < multiBucketDeleteRatio+multiBucketUpdateRatio {
					// Update
					keyIdx := rng.Intn(len(bucketKeys))
					keyNum := bucketKeys[keyIdx]
					key := make([]byte, len(prefix)+8)
					copy(key, prefix)
					binary.BigEndian.PutUint64(key[len(prefix):], keyNum)
					rng.Read(value)
					if err := batch.Set(key, value, pebble.Sync); err != nil {
						b.Fatal(err)
					}
				} else {
					// Insert
					keyNum := uint64(len(bucketKeys))
					key := make([]byte, len(prefix)+8)
					copy(key, prefix)
					binary.BigEndian.PutUint64(key[len(prefix):], keyNum)
					rng.Read(value)
					if err := batch.Set(key, value, pebble.Sync); err != nil {
						b.Fatal(err)
					}
					keysInserted[bucketIdx] = append(keysInserted[bucketIdx], keyNum)
				}
			}
			if err := batch.Commit(pebble.Sync); err != nil {
				b.Fatal(err)
			}
			batch.Close()
		}
	})
}

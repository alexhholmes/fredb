Bucket Implementation Plan

Feature: Pure Directory Bucket System

Time box: 6-8 hours
Deploy checkpoint: All existing tests pass + bucket CRUD works

  ---
Roll 1: Core Types (1 hour)

Outcome: Bucket metadata serialization compiles + tests

Files to create:
bucket.go         // Bucket type + BucketMeta
bucket_test.go    // Serialization tests

Code:
// bucket.go
type Bucket struct {
tx       *Tx
root     *base.Node
name     []byte
sequence uint64
writable bool
}

type BucketMeta struct {
RootPageID base.PageID
Sequence   uint64
Flags      uint32
}

const bucketMetaFlag = uint32(0x01)

func (bm *BucketMeta) Serialize() []byte
func DeserializeBucketMeta([]byte) *BucketMeta

Test compiles: go build

  ---
Roll 2: Tx Bucket Tracking (1 hour)

Outcome: Tx can track buckets, methods compile

Files to modify:
tx.go      // Add buckets map + skeleton methods
db.go      // Initialize buckets map in Begin()
errors.go  // Add bucket errors

Changes:
// tx.go
type Tx struct {
// ... existing fields
buckets map[string]*Bucket  // NEW
}

func (tx *Tx) Bucket(name []byte) *Bucket { return nil }  // Skeleton
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) { return nil, nil }  // Skeleton
func (tx *Tx) DeleteBucket(name []byte) error { return nil }  // Skeleton
func (tx *Tx) ForEach(fn func([]byte, *Bucket) error) error { return nil }  // Skeleton

// db.go - Begin()
tx := &Tx{
// ... existing
buckets: make(map[string]*Bucket),  // NEW
}

// errors.go
var (
ErrBucketExists   = errors.New("bucket already exists")
ErrBucketNotFound = errors.New("bucket not found")
)

Test compiles: go build

  ---
Roll 3: Database Initialization with Root Bucket (2 hours)

Outcome: New databases create root tree + root bucket

Files to modify:
db.go  // Open() - initialization logic

Changes:
// db.go - Open() - else block (new database)

// Create root tree (directory)
rootPageID, _ := coord.AllocatePage()
rootLeafID, _ := coord.AllocatePage()

rootLeaf := &base.Node{...}  // Empty leaf
root := &base.Node{           // Branch pointing to leaf
Children: []base.PageID{rootLeafID},
}

// Create __root__ bucket's tree
rootBucketRootID, _ := coord.AllocatePage()
rootBucketLeafID, _ := coord.AllocatePage()

rootBucketLeaf := &base.Node{...}  // Empty leaf
rootBucketRoot := &base.Node{      // Branch pointing to leaf
Children: []base.PageID{rootBucketLeafID},
}

// Insert __root__ bucket metadata into root tree
rootBucketMeta := &BucketMeta{
RootPageID: rootBucketRootID,
Sequence:   0,
Flags:      bucketMetaFlag,
}

// Use algo.ApplyLeafInsert to add to rootLeaf
algo.ApplyLeafInsert(rootLeaf, 0, []byte("__root__"), rootBucketMeta.Serialize())
root.NumKeys = 1

// Serialize and write all 4 pages
leafPage, _ := rootLeaf.Serialize(0)
coord.WritePage(rootLeafID, leafPage)

rootPage, _ := root.Serialize(0)
coord.WritePage(rootPageID, rootPage)

bucketLeafPage, _ := rootBucketLeaf.Serialize(0)
coord.WritePage(rootBucketLeafID, bucketLeafPage)

bucketRootPage, _ := rootBucketRoot.Serialize(0)
coord.WritePage(rootBucketRootID, bucketRootPage)

// Update meta
meta.RootPageID = rootPageID
coord.PutSnapshot(meta, root)
coord.Sync()
coord.CommitSnapshot()

Test:
rm -f test.db && go run . # Should create DB with root bucket

  ---
Roll 4: Bucket Loading (1 hour)

Outcome: tx.Bucket() loads existing buckets from disk

Files to modify:
tx.go  // Implement Bucket()

Implementation:
func (tx *Tx) Bucket(name []byte) *Bucket {
// Check cache
if b, exists := tx.buckets[string(name)]; exists {
return b
}

      // Load from root tree
      metaBytes, err := tx.search(tx.root, name)
      if err != nil {
          return nil
      }

      meta := DeserializeBucketMeta(metaBytes)
      if meta == nil {
          return nil
      }

      // Load bucket's root node
      bucketRoot, err := tx.loadNode(meta.RootPageID)
      if err != nil {
          return nil
      }

      // Create and cache
      bucket := &Bucket{
          tx:       tx,
          root:     bucketRoot,
          name:     name,
          sequence: meta.Sequence,
          writable: tx.writable,
      }

      tx.buckets[string(name)] = bucket
      return bucket
}

Test:
// bucket_test.go
func TestBucketLoad(t *testing.T) {
db, _ := Open("test.db")
defer db.Close()

      tx, _ := db.Begin(false)
      defer tx.Rollback()

      bucket := tx.Bucket([]byte("__root__"))
      if bucket == nil {
          t.Fatal("root bucket not found")
      }
}

  ---
Roll 5: Bucket CRUD Operations (1 hour)

Outcome: Can Get/Put/Delete in buckets

Files to modify:
bucket.go  // Implement Get/Put/Delete/Cursor

Implementation:
func (b *Bucket) Get(key []byte) []byte {
if b.root == nil {
return nil
}
val, err := b.tx.search(b.root, key)
if err != nil {
return nil
}
return val
}

func (b *Bucket) Put(key, value []byte) error {
if !b.writable {
return ErrTxNotWritable
}

      // Validation
      if len(key) > MaxKeySize {
          return ErrKeyTooLarge
      }
      if len(value) > MaxValueSize {
          return ErrValueTooLarge
      }

      // Handle root split if needed
      if b.root.IsFull() {
          leftChild, rightChild, midKey, _, err := b.tx.splitChild(b.root)
          if err != nil {
              return err
          }

          newRootID, _, err := b.tx.allocatePage()
          if err != nil {
              return err
          }

          b.root = algo.NewBranchRoot(leftChild, rightChild, midKey, newRootID)
      }

      // Insert with retry logic (copy from tx.Set)
      newRoot, err := b.tx.insertNonFull(b.root, key, value)
      if err != nil {
          return err
      }

      b.root = newRoot
      return nil
}

func (b *Bucket) Delete(key []byte) error {
if !b.writable {
return ErrTxNotWritable
}

      newRoot, err := b.tx.deleteFromNode(b.root, key)
      if err != nil {
          return err
      }

      b.root = newRoot
      return nil
}

func (b *Bucket) Cursor() *Cursor {
return &Cursor{
tx:         b.tx,
bucketRoot: b.root,
valid:      false,
}
}

Test:
go test -v -run TestBucketCRUD

  ---
Roll 6: Bucket Commit Logic (1 hour)

Outcome: Bucket changes persist to disk

Files to modify:
tx.go  // Modify Commit() to save bucket state

Changes:
func (tx *Tx) Commit() error {
// ... existing checks

      tx.db.mu.Lock()
      defer tx.db.mu.Unlock()

      // NEW: Update bucket metadata in root tree
      for name, bucket := range tx.buckets {
          if bucket.root != nil && bucket.root.Dirty {
              // Create updated metadata
              meta := &BucketMeta{
                  RootPageID: bucket.root.PageID,
                  Sequence:   bucket.sequence,
                  Flags:      bucketMetaFlag,
              }

              // Update in root tree
              newRoot, err := tx.insertNonFull(tx.root, []byte(name), meta.Serialize())
              if err != nil {
                  return err
              }
              tx.root = newRoot

              // Add bucket's root to pages map for commit
              tx.pages[bucket.root.PageID] = bucket.root
          }
      }

      // ... rest of existing commit logic (unchanged)
}

Test:
func TestBucketPersistence(t *testing.T) {
db, _ := Open("test.db")

      // Write
      db.Update(func(tx *Tx) error {
          b := tx.Bucket([]byte("__root__"))
          return b.Put([]byte("key"), []byte("value"))
      })

      db.Close()

      // Reopen
      db, _ = Open("test.db")
      defer db.Close()

      // Read
      db.View(func(tx *Tx) error {
          b := tx.Bucket([]byte("__root__"))
          val := b.Get([]byte("key"))
          if !bytes.Equal(val, []byte("value")) {
              t.Fatal("value not persisted")
          }
          return nil
      })
}

  ---
Roll 7: Backward Compatibility (30 min)

Outcome: Old API (tx.Get/Set/Delete/Cursor) works

Files to modify:
tx.go  // Rewrite Get/Set/Delete/Cursor to delegate

Changes:
func (tx *Tx) Get(key []byte) ([]byte, error) {
if err := tx.check(); err != nil {
return nil, err
}

      rootBucket := tx.Bucket([]byte("__root__"))
      if rootBucket == nil {
          return nil, ErrKeyNotFound
      }

      val := rootBucket.Get(key)
      if val == nil {
          return nil, ErrKeyNotFound
      }
      return val, nil
}

func (tx *Tx) Set(key, value []byte) error {
if err := tx.check(); err != nil {
return err
}
if !tx.writable {
return ErrTxNotWritable
}

      rootBucket := tx.Bucket([]byte("__root__"))
      if rootBucket == nil {
          return errors.New("root bucket not found")
      }

      return rootBucket.Put(key, value)
}

func (tx *Tx) Delete(key []byte) error {
if err := tx.check(); err != nil {
return err
}
if !tx.writable {
return ErrTxNotWritable
}

      rootBucket := tx.Bucket([]byte("__root__"))
      if rootBucket == nil {
          return ErrKeyNotFound
      }

      return rootBucket.Delete(key)
}

func (tx *Tx) Cursor() *Cursor {
rootBucket := tx.Bucket([]byte("__root__"))
if rootBucket == nil {
return &Cursor{tx: tx, valid: false}
}
return rootBucket.Cursor()
}

Test:
go test -v  # ALL existing tests should pass

  ---
Roll 8: CreateBucket Implementation (1 hour)

Outcome: Can create new buckets

Files to modify:
tx.go  // Implement CreateBucket

Implementation:
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
if !tx.writable {
return nil, ErrTxNotWritable
}
if err := tx.check(); err != nil {
return nil, err
}

      // Prevent overwriting root bucket
      if bytes.Equal(name, []byte("__root__")) {
          return nil, errors.New("cannot create bucket named __root__")
      }

      // Check if already exists
      existing, _ := tx.search(tx.root, name)
      if existing != nil {
          return nil, ErrBucketExists
      }

      // Allocate bucket's root
      bucketRootID, _, err := tx.allocatePage()
      if err != nil {
          return nil, err
      }

      // Allocate bucket's leaf
      leafID, _, err := tx.allocatePage()
      if err != nil {
          return nil, err
      }

      leaf := &base.Node{
          PageID:   leafID,
          Dirty:    true,
          NumKeys:  0,
          Keys:     make([][]byte, 0),
          Values:   make([][]byte, 0),
          Children: nil,
      }
      tx.pages[leafID] = leaf

      // Create bucket's root (branch)
      bucketRoot := &base.Node{
          PageID:   bucketRootID,
          Dirty:    true,
          NumKeys:  0,
          Keys:     make([][]byte, 0),
          Values:   nil,
          Children: []base.PageID{leafID},
      }
      tx.pages[bucketRootID] = bucketRoot

      // Create metadata
      meta := &BucketMeta{
          RootPageID: bucketRootID,
          Sequence:   0,
          Flags:      bucketMetaFlag,
      }

      // Insert into root tree
      newRoot, err := tx.insertNonFull(tx.root, name, meta.Serialize())
      if err != nil {
          return nil, err
      }
      tx.root = newRoot

      // Create and cache bucket
      bucket := &Bucket{
          tx:       tx,
          root:     bucketRoot,
          name:     name,
          sequence: 0,
          writable: true,
      }

      tx.buckets[string(name)] = bucket

      return bucket, nil
}

func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
if b := tx.Bucket(name); b != nil {
return b, nil
}
return tx.CreateBucket(name)
}

Test:
func TestCreateBucket(t *testing.T) {
db, _ := Open("test.db")
defer db.Close()

      db.Update(func(tx *Tx) error {
          bucket, err := tx.CreateBucket([]byte("users"))
          if err != nil {
              return err
          }

          bucket.Put([]byte("alice"), []byte("data"))
          return nil
      })

      // Verify persistence
      db.View(func(tx *Tx) error {
          bucket := tx.Bucket([]byte("users"))
          if bucket == nil {
              t.Fatal("bucket not found")
          }

          val := bucket.Get([]byte("alice"))
          if !bytes.Equal(val, []byte("data")) {
              t.Fatal("data not found")
          }
          return nil
      })
}

  ---
Roll 9: Cursor Support for Buckets (30 min)

Outcome: Cursors work on bucket data

Files to modify:
cursor.go  // Add bucketRoot field, use it instead of tx.root

Changes:
type Cursor struct {
tx         *Tx
bucketRoot *base.Node  // NEW - nil means use tx.root
stack      []cursorFrame
valid      bool
}

// All cursor methods change from:
//   c.tx.root
// To:
//   c.bucketRoot

func (c *Cursor) First() ([]byte, []byte) {
c.stack = nil
c.valid = false

      if c.bucketRoot == nil {
          return nil, nil
      }

      return c.descendToFirst(c.bucketRoot)  // Use bucketRoot
}

// Similar changes for Last, Next, Prev, Seek

Test:
go test -v -run TestCursor

  ---
Roll 10: Additional Bucket Methods (30 min)

Outcome: ForEach, Sequence, Writable methods work

Files to modify:
bucket.go  // Add remaining methods

Implementation:
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
c := b.Cursor()
for k, v := c.First(); k != nil; k, v = c.Next() {
if err := fn(k, v); err != nil {
return err
}
}
return nil
}

func (b *Bucket) Writable() bool {
return b.writable
}

func (b *Bucket) NextSequence() (uint64, error) {
if !b.writable {
return 0, ErrTxNotWritable
}
b.sequence++
return b.sequence, nil
}

func (b *Bucket) Sequence() uint64 {
return b.sequence
}

func (b *Bucket) SetSequence(v uint64) error {
if !b.writable {
return ErrTxNotWritable
}
b.sequence = v
return nil
}

  ---
Verification Checklist

After all rolls complete:

# Build
go build

# All tests pass
go test -v

# Existing functionality works
go test -run TestSequentialInsert
go test -run TestCursor
go test -run TestConcurrent

# New bucket functionality
go test -run TestBucket

  ---
Next Roll (separate feature)

Roll 11: DeleteBucket + ForEach Buckets (1 hour)

Not needed for initial deployment. Can ship without it.

  ---
Total time: 6-8 hours
Ships when: go test -v passes + can create/use buckets

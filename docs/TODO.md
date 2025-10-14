# TODO List

## Critical Fixes (Blocking Issues)
- [x] Tests are intermittently failing on TestDBConcurrentReads, possible race
  condition with multiple readers
- [ ] Prevent a dead writer tx from blocking writing forever

## High Priority (Core Functionality)
- [ ] Comprehensive unit testing
- [ ] Chaos testing for crash consistency
- [ ] Fuzzing with Github Actions
- [ ] Documentation for all public APIs
- [ ] Examples for all public APIs
- [ ] Performance improvements and profiling
- [x] Direct I/O
- [ ] Page pinning
- [ ] Structured logging for debugging
- [ ] Prevent other threads from writing to the same tx
- [ ] Crash recovery meta page writes

## Low Priority (Enhancements)
- [ ] Cleanup cache of old versions that are between the oldest active snapshot and
  the most recent snapshot
- [ ] Add compression option for keys and values
- [ ] Benchmarking of the addition of bloom filters
- [ ] Buckets with b-tree per-bucket
- [ ] Write tests for btree.go
- [ ] Cache eviction: Track number of pages being read by readers so when cache is
  full we can evict pages and use relocation map for disk mapping
- [ ] Pin branch pages in cache for better performance
- [ ] Zero copy aligned buffers for writes and new pages
- [ ] Nested buckets: Implement as flat key space in root tree (not __root__ bucket)
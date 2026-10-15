Built: Append-only Write-Ahead Log (WAL) with binary framing ([4-byte length][4-byte crc32][key][value or tombstone flag]), crash-safe tail recovery, and configurable fsync policies.
Files: lsm/wal.py, tests/test_wal.py
Next: MemTable implementation (in-memory sorted buffer with WAL integration and flush triggers).

Built: In-memory sorted MemTable using bisect, supporting put, get (distinguishing missing from deleted), delete with tombstones, sorted items iterator, and approximate memory size tracking.
Files: lsm/memtable.py, tests/test_memtable.py
Next: SSTable file writer and reader (immutable on-disk sorted tables with index and bloom filter).

Built: Immutable SSTable file format with data section, sparse index, fixed 12-byte footer with magic number validation, block-bounded binary-search get(), and ordered items iteration.
Files: lsm/sstable.py, tests/test_sstable.py
Next: Bloom filter integration or multi-SSTable compaction and LSM engine coordination.

Built: LSMStore storage engine coordinating WAL, MemTable, and SSTables with size-triggered flushes, crash recovery replay, atomic SSTable replacement, and tombstone search termination.
Files: lsm/store.py, tests/test_store.py
Next: Compaction strategy (tiered or leveled) to merge SSTables and purge obsolete tombstones, or Bloom filter integration.

Built: Multi-way SSTable compaction using heapq k-way merge with newest-wins conflict resolution, configurable tombstone purging, and input file preservation.
Files: lsm/compaction.py, tests/test_compaction.py
Next: Compaction integration in LSMStore (background or threshold-triggered compaction coordinating active tables and replacement).

Built: Threshold-triggered compaction in LSMStore with tombstone purging, 5-step crash-safety protocol, marker tracking, and startup crash recovery.
Files: lsm/store.py, tests/test_store.py
Next: Bloom filter integration for SSTable read acceleration or server protocol implementation.

Built: Probabilistic BloomFilter with theoretical m/k sizing, blake2b double hashing ((h1 + i*h2) % m), binary serialization with CRC32 checksum, and corruption rejection.
Files: lsm/bloom.py, tests/test_bloom.py
Next: Bloom filter integration into SSTable writer and reader for disk-read acceleration.

Built: BloomFilter integration in LSMStore with atomic .bloom sidecar creation, read-path skip optimization (bloom_skips), corrupted/missing fallback rebuild, and crash-safe compaction cleanup.
Files: lsm/store.py, tests/test_store.py
Next: Network server/client protocol implementation or storage benchmarking suite.

Built: Asyncio TCP server supporting RESP2 protocol (PING, SET, GET, DEL) with read buffering for pipelining/split requests, disconnect resilience, clean shutdown, and blocking RespClient.
Files: server/server.py, tests/resp_client.py, tests/test_server.py
Next: Storage engine benchmarking suite (throughput, latency, pipelining) or client connection pooling.

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

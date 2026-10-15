Built: Append-only Write-Ahead Log (WAL) with binary framing ([4-byte length][4-byte crc32][key][value or tombstone flag]), crash-safe tail recovery, and configurable fsync policies.
Files: lsm/wal.py, tests/test_wal.py
Next: MemTable implementation (in-memory sorted buffer with WAL integration and flush triggers).

Built: In-memory sorted MemTable using bisect, supporting put, get (distinguishing missing from deleted), delete with tombstones, sorted items iterator, and approximate memory size tracking.
Files: lsm/memtable.py, tests/test_memtable.py
Next: SSTable file writer and reader (immutable on-disk sorted tables with index and bloom filter).

Built: Append-only Write-Ahead Log (WAL) with binary framing ([4-byte length][4-byte crc32][key][value or tombstone flag]), crash-safe tail recovery, and configurable fsync policies.
Files: lsm/wal.py, tests/test_wal.py
Next: MemTable implementation (in-memory sorted buffer with WAL integration and flush triggers).

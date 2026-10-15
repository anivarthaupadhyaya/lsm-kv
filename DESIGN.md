# LSM-KV Design Documentation

This document describes the architectural design, operational paths, crash-safety protocols, trade-offs, known limitations, and future improvements for the single-node LSM-tree key-value store.

---

## 1. Write Path

Every write operation (`SET` or `DEL`) flows through [LSMStore](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L19-L278) via `put(key, value)` or `delete(key)`.

```
Client (SET / DEL)
        │
        ▼
   LSMStore._lock (RLock)
        │
        ├─────────────────────────────┐
        ▼                             ▼
1. Append to WAL               2. Insert to MemTable
   (CRC32 + framing)              (bisect sorted list)
        │                             │
        ▼                             ▼
   fsync policy check          Size check: >= memtable_limit_bytes?
   (always / interval / never)        │
                                      ▼ (if exceeded)
                               3. Synchronous _flush_locked()
```

### Detailed Steps:
1. **Concurrency Control**: The call enters [LSMStore](file:///c:/Users/Hp/lsm-kv/lsm/store.py) and acquires `self._lock` (`threading.RLock`), serializing write mutations.
2. **Write-Ahead Log (WAL) Append**:
   - The write is formatted as a binary payload: `[key_len: uint32][flag: uint8][key_bytes][val_bytes]`, where `flag=0` represents a normal value and `flag=1` represents a tombstone deletion.
   - A 4-byte CRC32 checksum is calculated over the entire payload.
   - The framed record `[payload_len: uint32][crc32: uint32][payload]` is written to `wal.log` via [WAL.append()](file:///c:/Users/Hp/lsm-kv/lsm/wal.py#L101-L152).
   - **Durability check**:
     - `always`: Immediately flushes user-space buffers (`self._file.flush()`) and triggers an OS-level sync ([os.fsync()](file:///c:/Users/Hp/lsm-kv/lsm/wal.py#L157)).
     - `interval`: Writes to file buffer; a background worker thread invokes `sync()` every $N$ milliseconds.
     - `never`: Writes to file buffer; data is committed at the discretion of the OS page cache flush mechanism.
3. **MemTable Update**:
   - The operation updates the active [MemTable](file:///c:/Users/Hp/lsm-kv/lsm/memtable.py#L37-L141).
   - [MemTable.put()](file:///c:/Users/Hp/lsm-kv/lsm/memtable.py#L57-L88) locates the key index using `bisect.bisect_left` over `_keys`.
   - If the key exists, its value is overwritten; if new, the key and value are inserted at the sorted position ($O(\log N)$ search, $O(N)$ list shift).
   - Deletions are stored with `value=None` (tombstone).
   - Approximate memory consumption (`self._size_bytes`) is tracked with every mutation.
4. **Flush Trigger**:
   - If `self._memtable.size_bytes() >= self.memtable_limit_bytes` (default: 4096 bytes), [LSMStore._flush_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L162-L193) is invoked synchronously.

---

## 2. Read Path

Reads (`GET`) enter [LSMStore.get()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L140-L156) under `self._lock`. Because an LSM-tree does not update data in place, reads query the components in order from newest to oldest:

```
Client (GET key)
        │
        ▼
   Active MemTable
        ├─► Key found (Value)     ──► Return Value
        ├─► Key found (Tombstone) ──► Return None (Halt search immediately)
        └─► Key missing
                 │
                 ▼
   Iterate SSTables (newest to oldest)
        │
        ├─► BloomFilter.might_contain(key) == False?
        │         ├─► Yes: bloom_skips += 1 (Skip SSTable, 0 disk I/O)
        │         └─► No:  Proceed to SSTable binary search
        │
        ├─► SSTable Sparse Index Binary Search
        │         └─► Locate [start_offset, end_offset) block
        │
        ├─► Linear scan at most 16 records in block
        │         ├─► Record matches (Value)     ──► Return Value
        │         ├─► Record matches (Tombstone) ──► Return None (Halt search)
        │         └─► Record > target key        ──► Key absent in this SSTable
        │
        ▼ (All SSTables exhausted)
   Return default (None)
```

### Detailed Steps:
1. **MemTable Inspection**:
   - Queries `self._memtable.get(key)`.
   - If the key is present with a value, the value is returned immediately.
   - If the key is present as a `TOMBSTONE` (`None`), the search **terminates immediately** and returns `None`. Older SSTables are shielded from being queried.
   - If `MISSING`, the search moves to on-disk SSTables.
2. **SSTable Traversal (Newest to Oldest)**:
   - Traverses `self._sstables` (`sst_000004.sst`, `sst_000003.sst`, etc.).
   - **Bloom Filter Check**:
     - When `use_bloom=True`, queries the table's in-memory [BloomFilter](file:///c:/Users/Hp/lsm-kv/lsm/bloom.py#L27-L148) via `might_contain(key)`.
     - If `False`, `bloom_skips` is incremented, and the engine skips the table entirely without performing any disk I/O.
   - **SSTable Sparse Index Lookup**:
     - If key is smaller than the first index entry (`key_bytes < _index_keys[0]`), the key is absent in this table.
     - Performs `bisect.bisect_right` on `_index_keys` to identify the boundary `[start_offset, end_offset)` corresponding to a sparse index block (index interval = 16 records).
   - **Block Scan**:
     - Seeks to `start_offset` in the SSTable file and scans at most 16 records.
     - If a matching key is found:
       - If `val_len == -1` (tombstone), the key was deleted; search stops immediately and returns `None`.
       - If `val_len >= 0`, the value bytes are read and returned.
     - If a scanned key exceeds the target key (`curr_key > key_bytes`), the search in this table terminates early because keys are ordered lexicographically.
3. **Fallback**: If all SSTables are checked without a match, returns `None`.

---

## 3. Flush Order and Atomicity

When a MemTable reaches capacity or a manual `FLUSH` command is executed, [LSMStore._flush_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L162-L193) runs. To guarantee crash recovery without data loss or corruption, operations follow a strict sequence:

1. **Allocate Sequence**: Obtains the next monotonic sequence number `seq = self._next_seq; self._next_seq += 1`.
2. **Write SSTable to Temporary File**:
   - Writes all key-value pairs and tombstones from `_memtable.items()` into `sst_XXXXXX.tmp`.
   - Writes the data section, sparse index (every 16 records), and fixed 12-byte footer (`[index_offset: uint64][magic: 0x53535442]`).
   - Flushes user-space buffers and calls `os.fsync()` on the file descriptor.
3. **Write Bloom Filter to Temporary File**:
   - Builds a [BloomFilter](file:///c:/Users/Hp/lsm-kv/lsm/bloom.py#L27-L148) containing **all** items from the MemTable (including tombstones).
   - Serializes the filter (`[m: uint32][k: uint32][crc32: uint32][bit_array]`) to `sst_XXXXXX.bloom.tmp`.
   - Flushes buffers and calls `os.fsync()`.
4. **Atomic Publication via Rename**:
   - Uses `os.replace()` to atomically rename `sst_XXXXXX.tmp` to `sst_XXXXXX.sst`.
   - Uses `os.replace()` to atomically rename `sst_XXXXXX.bloom.tmp` to `sst_XXXXXX.bloom`.
5. **Update In-Memory Table Descriptors**:
   - Initializes an [SSTable](file:///c:/Users/Hp/lsm-kv/lsm/sstable.py#L105-L245) instance and prepends it to `self._sstables` (index 0).
   - Caches the Bloom filter in `self._bloom_filters`.
6. **Retire WAL and Clear MemTable**:
   - Closes the active WAL: `self._wal.close()`.
   - Unlinks the active WAL file: `self._wal_path.unlink()`.
   - Initializes a fresh, empty WAL: `self._wal = WAL(...)`.
   - Reinitializes the active MemTable: `self._memtable = MemTable()`.

### Why Flush Order Matters:
The newly written SSTable and its Bloom filter sidecar are fully synchronized and atomically published on disk **before** the old WAL file is unlinked. If a crash or power failure occurs at any point before Step 6 completes, the old WAL remains intact on disk. Upon engine restart, [WAL.replay_file()](file:///c:/Users/Hp/lsm-kv/lsm/wal.py#L181-L231) reconstructs the MemTable in memory, ensuring zero data loss. Any incomplete `.tmp` files are cleaned up on startup by `_cleanup_tmp_files()`.

---

## 4. Compaction Commit-Marker Protocol

When the number of active SSTables exceeds `compaction_threshold` (default: 4), [LSMStore._compact_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L199-L246) executes a multi-way k-way merge across all existing SSTables.

Because replacing multiple old SSTables with a new compacted SSTable cannot be done as a single atomic OS filesystem operation, [LSMStore](file:///c:/Users/Hp/lsm-kv/lsm/store.py) enforces a **5-step commit-marker protocol**:

```
Step 1: Write Compacted SSTable & Bloom to .tmp files
        │  (k-way merge with heapq, drop_tombstones=True, fsync both)
        ▼
Step 2: Write compaction.marker
        │  ({"output": "sst_000005.sst", "inputs": ["sst_000001.sst", ...]}, fsync)
        ▼
Step 3: Atomic Publish
        │  (os.replace .tmp -> .sst and .bloom)
        ▼
Step 4: Delete Input Files
        │  (unlink input .sst and .bloom files)
        ▼
Step 5: Unlink compaction.marker
        │  (compaction complete)
```

### The Bug Prevented by the Protocol:
Without a commit marker, a crash occurring during compaction leads to data corruption or inconsistency:
1. **Tombstone Resurrection**: In an LSM-tree, a major compaction that merges all SSTables purges deleted keys (`drop_tombstones=True`). If a crash occurs after publishing the new compacted table but before old tables are deleted:
   - Without a marker, the restart routine would load **both** the new compacted table and the old input tables.
   - Any key that was previously deleted would have its tombstone removed in the new compacted table, while the old tables would still contain the obsolete, pre-deletion value.
   - As a result, the deleted key would **resurrect**, restoring stale data to the client!
2. **Duplicate and Inconsistent State**: If the system crashes mid-write without a marker, half-written or overlapping files would lead to duplicate keys and non-deterministic read results.

### Crash Recovery via `_recover_compaction()`:
During [LSMStore.__init__()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L22-L52), the engine inspects the data directory for `compaction.marker`:
- **If the marker exists**: It parses the marker JSON payload (`{"output": out_name, "inputs": [...]}`).
  - **Case A: `output` file exists on disk**: The crash occurred at or after Step 3 (atomic publish). The new compacted SSTable is complete and authoritative. Recovery iterates over all listed `inputs`, unlinks their `.sst` and `.bloom` files, and removes `compaction.marker`. This eliminates duplicate files and prevents tombstone resurrection.
  - **Case B: `output` file does not exist on disk**: The crash occurred during Step 1 or Step 2 before publication. The compaction was aborted. Recovery unlinks `compaction.marker` and any orphaned `.tmp` files. The original `inputs` are intact and remain authoritative.

---

## 5. Why Deleted Keys Must Be Added to Bloom Filters

In an LSM-tree, deletions do not modify existing SSTables in place. Instead, a deletion is written as a tombstone (`value=None`) into the active MemTable and eventually flushed to a new SSTable.

### The Resurrection Problem:
Suppose key `user_123` was written in an older SSTable (`sst_000001.sst`). Later, `user_123` is deleted, and its tombstone is written to a newer SSTable (`sst_000002.sst`).

If the Bloom filter for `sst_000002.sst` only contained keys with non-null values (omitting tombstones):
1. A client calls `GET user_123`.
2. The read path checks `sst_000002.sst`'s Bloom filter.
3. The Bloom filter reports `might_contain("user_123") == False` because the key was not added to the filter.
4. The engine skips `sst_000002.sst` entirely to save disk I/O.
5. The read path proceeds to `sst_000001.sst`.
6. In `sst_000001.sst`, the key exists with its old value!
7. The engine returns the old value to the client, resurrecting the deleted record.

### The Solution:
Both [LSMStore._flush_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L172-L173) and [LSMStore._compact_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L213-L215) insert **all** keys from the source table—including tombstones—into the Bloom filter. When `GET user_123` is called:
- The Bloom filter for `sst_000002.sst` returns `True`.
- The engine reads `sst_000002.sst`, encounters the tombstone record (`val_len == -1`), and halts the search, returning `None`.

---

## 6. The fsync Trade-Off

The storage engine provides three WAL fsync policies with distinct durability and performance characteristics. The benchmark results from `bench/results.json` (running 2,000 operations with 4 threads and pipeline=1) demonstrate these trade-offs:

| Policy | Ops/sec | p50 Latency (ms) | p99 Latency (ms) | Total Duration (s) | Durability Guarantee |
|---|---|---|---|---|---|
| `always` | **519.9** | **5.049** | **46.016** | **3.847** | Every acknowledged write is flushed to disk via `os.fsync()`. Zero data loss on sudden power failure or crash. |
| `10ms` | **1700.1** | **0.236** | **41.161** | **1.176** | Writes are buffered in memory and flushed periodically by a background worker every 10 ms. Up to 10 ms of acknowledged writes may be lost on power loss. |
| `never` | **1686.2** | **0.228** | **40.463** | **1.186** | Writes rely entirely on OS page cache writeback; `os.fsync()` only called on close. All writes since last OS flush are lost on power failure. |

### Analysis:
- **Throughput & Latency Gain**: Moving from `always` to `10ms` periodic sync increases throughput by over **3.2x** (from `519.9` to `1700.1` ops/sec) and decreases p50 latency by over **21x** (from `5.049 ms` down to `0.236 ms`).
- **Disk Bottleneck**: Under `always`, write speed is physically bounded by storage drive flush latency (rotational delay or SSD flash commit cycles). Periodic background syncing batches multiple writes into single I/O sync operations, achieving throughput comparable to `never` while bounding potential data loss to a predictable 10 ms window.

---

## 7. Known Limitations

The following limitations are verified directly from the codebase:

1. **Compaction Merges All Tables at Once on the Write Path (Write Stalls)**:
   - In [LSMStore._flush_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L190-L191), if `len(self._sstables) > self.compaction_threshold`, the engine invokes `self._compact_locked()` synchronously.
   - [LSMStore._compact_locked()](file:///c:/Users/Hp/lsm-kv/lsm/store.py#L202) collects **all** existing SSTables (`input_tables = list(reversed(self._sstables))`) and rewrites them into a single new table.
   - Because compaction executes synchronously while holding `self._lock`, all incoming client `SET` and `DEL` operations are blocked for the entire duration of the merge.
   - This directly explains the write latency spike observed in `bench/results.json`: during the 100,000 SET benchmark, the **p99 latency spikes to 503.477 ms** (compared to a **p50 of 0.235 ms**), pulling overall SET throughput down to **320.8 ops/sec**.
2. **Single Node**:
   - The engine is designed as an embedded, single-instance storage system. There is no support for horizontal clustering, data partitioning/sharding, consensus protocols (such as Raft or Paxos), or primary-replica replication.
3. **Python Server Throughput Ceiling**:
   - The network server ([server.py](file:///c:/Users/Hp/lsm-kv/server/server.py)) is implemented using Python's `asyncio` event loop.
   - All store mutations and queries are serialized through a Python `threading.RLock`.
   - Due to Python's Global Interpreter Lock (GIL), byte protocol parsing overhead, and single-threaded execution, the server throughput reaches a ceiling of **6615.1 ops/sec on GET** and **1700.1 ops/sec on buffered SET**, which is bounded by Python CPU overhead rather than storage I/O capacity.
4. **Process-Kill Testing Only**:
   - The automated crash testing harness ([test_crash.py](file:///c:/Users/Hp/lsm-kv/tests/test_crash.py)) terminates the subprocess using `Popen.kill()`.
   - This verifies recovery against process crashes, unhandled exceptions, and forced process termination (`SIGKILL` / `TerminateProcess`). It does not simulate power loss, volatile drive-controller write cache drop, operating system kernel panics, or hardware block corruption.

---

## 8. Next Steps

- **Leveled or Size-Tiered Compaction in a Background Thread**:
  - **Decouple from Write Path**: Move compaction execution out of `_flush_locked()` into an asynchronous background worker thread or queue. Flushes will complete immediately, eliminating the `503.477 ms` p99 write stalls.
  - **Size-Tiered Compaction Strategy (STCS)**: Group SSTables into size tiers and compact only within matching tiers when a tier fills, reducing write amplification for write-intensive workloads.
  - **Leveled Compaction Strategy (LCS)**: Organize storage into hierarchical levels ($L_0, L_1, L_2, \dots$) where levels $L \ge 1$ have strict size limits and non-overlapping key ranges. Compacting a single file from $L_i$ into overlapping files in $L_{i+1}$ provides deterministic space bounds, eliminates redundant key checks, and improves read latency predictability.

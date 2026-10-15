# LSM-KV

A lightweight, single-node Log-Structured Merge-tree (LSM-tree) key-value storage engine implemented in Python 3.11+. The engine includes an append-only Write-Ahead Log (WAL) with CRC32 checksums, an in-memory MemTable, immutable on-disk SSTables with sparse indexing, Kirsch-Mitzenmacher double-hashing Bloom filters, crash-safe multi-way compaction, and an asyncio TCP server speaking the Redis RESP2 protocol.

---

## Architecture Overview

- **Write-Ahead Log (WAL)**: Binary framed log (`[length: uint32][crc32: uint32][payload]`) supporting configurable fsync durability policies (`always`, periodic interval, `never`) and tail crash recovery ([wal.py](file:///c:/Users/Hp/lsm-kv/lsm/wal.py)).
- **MemTable**: In-memory sorted buffer backed by `bisect` for fast key lookups and ordered iteration, tracking approximate memory footprint ([memtable.py](file:///c:/Users/Hp/lsm-kv/lsm/memtable.py)).
- **SSTable**: Immutable on-disk sorted string tables with binary key-value records, block-bounded sparse indices, and a fixed 12-byte footer validated by magic bytes `0x53535442` (`SSTB`) ([sstable.py](file:///c:/Users/Hp/lsm-kv/lsm/sstable.py)).
- **Bloom Filter**: Probabilistic set membership filters using Kirsch-Mitzenmacher double hashing via `blake2b` (`(h1 + i * h2) % m`), stored as atomic `.bloom` sidecars to eliminate unnecessary disk reads on absent keys ([bloom.py](file:///c:/Users/Hp/lsm-kv/lsm/bloom.py)).
- **Compaction**: Multi-way k-way merge using `heapq` with newest-wins resolution, tombstone purging, atomic temporary file publishing, and a 5-step commit-marker protocol ([compaction.py](file:///c:/Users/Hp/lsm-kv/lsm/compaction.py), [store.py](file:///c:/Users/Hp/lsm-kv/lsm/store.py)).
- **RESP2 TCP Server**: Asyncio TCP server supporting streaming input buffering, request pipelining, and Redis-compatible commands ([server.py](file:///c:/Users/Hp/lsm-kv/server/server.py), [resp.py](file:///c:/Users/Hp/lsm-kv/server/resp.py)).

---

## How to Run

### Running the Server

Start the RESP2 TCP server with default settings (binds to `127.0.0.1:6380` with data stored in `./data`):

```bash
python -m server.server
```

Custom configuration options:

```bash
python -m server.server --host 127.0.0.1 --port 6380 --dir ./data --fsync-policy always --use-bloom --memtable-limit 4096 --compaction-threshold 4
```

Command-line flags:
- `--host`: Bind host address (default: `127.0.0.1`).
- `--port`: Bind port number (default: `6380`).
- `--dir`: Storage directory path for WAL, SSTables, and Bloom filters (default: `./data`).
- `--fsync-policy`: Fsync durability mode: `always` (every write), interval in ms such as `10`, or `never` (default: `always`).
- `--use-bloom` / `--no-bloom`: Toggle Bloom filter read acceleration (default: enabled).
- `--memtable-limit`: MemTable size threshold in bytes before triggering an SSTable flush (default: `4096`).
- `--compaction-threshold`: Number of SSTables that triggers automatic multi-way compaction (default: `4`).

You can connect to the running server using standard Redis clients or `redis-cli`:

```bash
redis-cli -p 6380 PING
```

### Running the Tests

Execute the unit and integration test suite using pytest:

```powershell
pytest -x -q --timeout=30 2>&1 | Select-Object -Last 40
```

Run the crash recovery test suite:

```powershell
python tests/test_crash.py --rounds 200
```

Or run the crash recovery test via pytest:

```powershell
pytest tests/test_crash.py -q --rounds=200
```

---

## Supported Commands

The server implements the Redis Serialization Protocol 2 (RESP2) and supports the following commands:

| Command | Syntax | Description | Success Response | Error / Missing Response |
|---|---|---|---|---|
| `PING` | `PING [message]` | Checks server connectivity | `+PONG\r\n` or bulk string `$len\r\n<message>\r\n` | `-ERR wrong number of arguments for 'ping' command\r\n` |
| `SET` | `SET key value` | Inserts or overwrites a key-value pair | `+OK\r\n` | `-ERR wrong number of arguments for 'set' command\r\n` |
| `GET` | `GET key` | Retrieves the value of a key | Bulk string `$len\r\n<value>\r\n` | `$-1\r\n` (null bulk string if key does not exist or was deleted) |
| `DEL` | `DEL key` | Deletes a key via a tombstone | Integer `:1\r\n` (if key existed) | Integer `:0\r\n` (if key did not exist) |
| `FLUSH` | `FLUSH` | Flushes active MemTable to an immutable SSTable | `+OK\r\n` | `-ERR ...\r\n` |
| `COMPACT` | `COMPACT` | Manually triggers multi-way SSTable compaction | `+OK\r\n` | `-ERR ...\r\n` |

---

## Performance Benchmarks

The benchmark suite ([run.py](file:///c:/Users/Hp/lsm-kv/bench/run.py)) measures read/write throughput, latency distributions, Bloom filter efficiency, fsync policy trade-offs, and compaction impacts.

### Benchmark Settings
- **Client Threads**: 4
- **Pipeline Batch Size**: 1
- **Value Size**: 100 bytes

### Results Table (from `bench/results.json`)

| Benchmark | Configuration / Policy | Keys | Ops/sec | p50 Latency (ms) | p99 Latency (ms) | Duration (sec) |
|---|---|---|---|---|---|---|
| 1. SET | 100-byte values, pipeline=1 | 100,000 | 320.8 | 0.235 | 503.477 | 311.718 |
| 2. GET (existing keys) | pipeline=1 | 100,000 | 6615.1 | 0.594 | 0.753 | 15.117 |
| 3. GET non-existent | `use_bloom=ON` (28 SSTs) | 2,000 | 6538.1 | 0.542 | 1.742 | 0.306 |
| 3. GET non-existent | `use_bloom=OFF` (28 SSTs) | 2,000 | 577.8 | 6.889 | 7.341 | 3.461 |
| 4. SET fsync policy | `always` | 2,000 | 519.9 | 5.049 | 46.016 | 3.847 |
| 4. SET fsync policy | `10ms` interval | 2,000 | 1700.1 | 0.236 | 41.161 | 1.176 |
| 4. SET fsync policy | `never` | 2,000 | 1686.2 | 0.228 | 40.463 | 1.186 |
| 5. Read latency | Before compaction (28 SSTs) | 350 | 4871.0 | 0.727 | 1.705 | 0.072 |
| 5. Read latency | After compaction (1 SST) | 350 | 6931.4 | 0.567 | 0.657 | 0.05 |

---

## Crash Resilience Testing

- **Result**: **200 of 200 rounds passed** (0 failures).
- **Test Methodology**:
  - The crash test harness ([test_crash.py](file:///c:/Users/Hp/lsm-kv/tests/test_crash.py)) executes a separate writer process ([crash_writer.py](file:///c:/Users/Hp/lsm-kv/tests/crash_writer.py)) with aggressive write thresholds (`memtable_limit_bytes=40`, `compaction_threshold=2`, `fsync_policy="always"`).
  - The writer continuously executes a randomized sequence of `SET` and `DEL` operations over key space `k0`–`k5`, printing each completed operation to stdout.
  - The parent runner kills the writer subprocess abruptly after a random delay using `proc.kill()`, interrupting active writes, WAL flushes, or compactions.
  - Upon process termination, the store is reopened and validated against the replayed log of acknowledged operations. The test confirms exact state equality with at most 1 in-flight operation difference (for an operation killed between returning and writing to stdout).
- **Scope & Limitations of Testing**:
  - This test validates resilience against **forced process kills** (`Popen.kill()` / `TerminateProcess`), application crashes, and sudden process terminations.
  - It does **not** test physical power loss, dirty un-flushed hardware drive controller write caches, sudden OS kernel panics, or storage media hardware faults.

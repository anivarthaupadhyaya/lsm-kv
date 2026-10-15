"""LSM-Tree storage engine coordinating WAL, MemTable, SSTables, and BloomFilter."""

import json
import os
import re
import threading
from pathlib import Path
from typing import Optional, Union

from lsm.bloom import BloomFilter
from lsm.compaction import compact
from lsm.memtable import MISSING, TOMBSTONE, MemTable
from lsm.sstable import SSTable, write_sstable
from lsm.wal import WAL

SST_PATTERN = re.compile(r"^sst_(\d+)\.sst$")


class LSMStore:
    """Single-node LSM-tree key-value store engine."""

    def __init__(
        self, data_dir: Union[str, Path], memtable_limit_bytes: Optional[int] = None, *,
        memtable_size_limit: Optional[int] = None, memtable_limit: Optional[int] = None,
        fsync_policy: Union[str, int, float] = "always", compaction_threshold: int = 4,
        crash_step: Optional[int] = None, crash_at_step: Optional[int] = None, use_bloom: bool = True,
    ) -> None:
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        limit = memtable_limit_bytes if memtable_limit_bytes is not None else (memtable_size_limit or memtable_limit)
        self.memtable_limit_bytes = self.memtable_size_limit = int(limit) if limit is not None else 4096
        self._fsync_policy = fsync_policy
        self.compaction_threshold = compaction_threshold
        self.crash_step = crash_step if crash_step is not None else crash_at_step
        self.use_bloom = use_bloom
        self.bloom_skips = 0
        self._bloom_filters: dict[Path, BloomFilter] = {}
        self._lock = threading.RLock()
        self._closed = False

        self._recover_compaction()
        self._cleanup_tmp_files()
        self._sstables: list[SSTable] = []
        self._next_seq = self._load_sstables()

        self._wal_path = self._resolve_wal_path()
        self._memtable = MemTable()
        if self._wal_path.exists():
            for record in WAL.replay_file(self._wal_path):
                self._memtable.put(record.key, record.value)
        self._wal = WAL(self._wal_path, fsync_policy=self._fsync_policy)

    @staticmethod
    def _bloom_path(sst_path: Path) -> Path:
        return sst_path.with_suffix(".bloom")

    def _recover_compaction(self) -> None:
        """Startup recovery for crashed compactions based on compaction.marker."""
        marker = self.data_dir / "compaction.marker"
        if not marker.exists(): return
        out_name, in_names = self._parse_marker(marker)
        if out_name and (self.data_dir / Path(out_name).name).exists():
            for name in in_names:
                p = self.data_dir / Path(name).name
                p.unlink(missing_ok=True)
                self._bloom_path(p).unlink(missing_ok=True)
            marker.unlink(missing_ok=True)
        else:
            marker.unlink(missing_ok=True)
            self._cleanup_tmp_files()

    @staticmethod
    def _parse_marker(marker: Path) -> tuple[Optional[str], list[str]]:
        """Parses compaction.marker returning (output_name, input_names)."""
        try:
            content = marker.read_text(encoding="utf-8").strip()
            if not content: return None, []
            try:
                data = json.loads(content)
                if isinstance(data, dict): return data.get("output"), data.get("inputs", [])
            except Exception: pass
            output, inputs = None, []
            for ln in (line.strip() for line in content.splitlines() if line.strip()):
                if ln.lower().startswith("output:"): output = ln.split(":", 1)[1].strip()
                elif ln.lower().startswith("input:"): inputs.append(ln.split(":", 1)[1].strip())
                elif output is None: output = ln
                else: inputs.append(ln)
            return output, inputs
        except Exception: return None, []

    def _cleanup_tmp_files(self) -> None:
        """Removes any leftover temporary files from aborted flushes or compactions."""
        for p in self.data_dir.iterdir():
            if p.is_file() and p.name.endswith(".tmp"): p.unlink(missing_ok=True)

    def _load_or_rebuild_bloom(self, sst: SSTable) -> BloomFilter:
        bp = self._bloom_path(sst.path)
        if bp.exists():
            try: return BloomFilter.from_bytes(bp.read_bytes())
            except Exception: pass
        try: keys = [r.key for r in sst.items()]
        except Exception: keys = []
        bf = BloomFilter(expected_items=max(1, len(keys)), fp_rate=0.01)
        for k in keys: bf.add(k)
        return bf

    def _load_sstables(self) -> int:
        """Loads SSTables sorted newest-to-oldest, returning next sequence number."""
        sst_entries: list[tuple[int, Path]] = []
        for p in self.data_dir.iterdir():
            if p.is_file() and (m := SST_PATTERN.match(p.name)):
                sst_entries.append((int(m.group(1)), p))
        sst_entries.sort(key=lambda x: x[0])
        self._sstables = [SSTable(p) for _, p in reversed(sst_entries)]
        for sst in self._sstables:
            self._bloom_filters[sst.path] = self._load_or_rebuild_bloom(sst)
        return sst_entries[-1][0] + 1 if sst_entries else 1

    def _resolve_wal_path(self) -> Path:
        for name in ("wal.log", "wal.wal"):
            if (p := self.data_dir / name).exists(): return p
        return self.data_dir / "wal.log"

    def put(self, key: Union[bytes, str], value: Optional[Union[bytes, str]]) -> None:
        with self._lock:
            if self._closed: raise ValueError("Store is closed.")
            self._wal.append(key, value)
            self._memtable.put(key, value)
            if self._memtable.size_bytes() >= self.memtable_limit_bytes:
                self._flush_locked()

    def delete(self, key: Union[bytes, str]) -> None:
        with self._lock:
            if self._closed: raise ValueError("Store is closed.")
            self._wal.append(key, None)
            self._memtable.delete(key)
            if self._memtable.size_bytes() >= self.memtable_limit_bytes:
                self._flush_locked()

    def get(self, key: Union[bytes, str], default: object = None) -> Optional[bytes]:
        with self._lock:
            if self._closed: raise ValueError("Store is closed.")
            val = self._memtable.get(key, default=MISSING)
            if val is not MISSING:
                return default if val is TOMBSTONE else val
            for sst in self._sstables:
                if self.use_bloom:
                    bf = self._bloom_filters.get(sst.path)
                    if bf is not None and not bf.might_contain(key):
                        self.bloom_skips += 1
                        continue
                val = sst.get(key, default=MISSING)
                if val is not MISSING:
                    return default if val is TOMBSTONE else val
            return default

    def flush(self) -> Optional[SSTable]:
        with self._lock:
            if self._closed: raise ValueError("Store is closed.")
            return self._flush_locked()

    def _flush_locked(self) -> Optional[SSTable]:
        if len(self._memtable) == 0: return None
        seq = self._next_seq
        self._next_seq += 1
        final_path, tmp_path = self.data_dir / f"sst_{seq:06d}.sst", self.data_dir / f"sst_{seq:06d}.tmp"
        final_bloom = self._bloom_path(final_path)
        tmp_bloom = self.data_dir / f"{final_bloom.name}.tmp"

        items = list(self._memtable.items())
        write_sstable(tmp_path, items)
        bf = BloomFilter(expected_items=max(1, len(items)), fp_rate=0.01)
        for it in items: bf.add(it.key)
        with open(tmp_bloom, "wb") as f:
            f.write(bf.to_bytes())
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, final_path)
        os.replace(tmp_bloom, final_bloom)
        sstable = SSTable(final_path)
        self._sstables.insert(0, sstable)
        self._bloom_filters[sstable.path] = bf

        if self._wal is not None: self._wal.close()
        self._wal_path.unlink(missing_ok=True)
        self._wal = WAL(self._wal_path, fsync_policy=self._fsync_policy)
        self._memtable = MemTable()

        if self.compaction_threshold and len(self._sstables) > self.compaction_threshold:
            self._compact_locked()
        return sstable

    def compact(self, crash_step: Optional[int] = None) -> Optional[SSTable]:
        with self._lock:
            if self._closed: raise ValueError("Store is closed.")
            return self._compact_locked(crash_step=crash_step)

    def _compact_locked(self, crash_step: Optional[int] = None) -> Optional[SSTable]:
        if len(self._sstables) <= 1: return None
        step = self.crash_step if crash_step is None else crash_step
        input_tables = list(reversed(self._sstables))
        in_names = [t.path.name for t in input_tables]
        max_in_seq = max((int(m.group(1)) for t in input_tables if (m := SST_PATTERN.match(t.path.name))), default=0)
        out_seq = max(self._next_seq, max_in_seq + 1)
        self._next_seq = out_seq + 1
        out_name, tmp_name = f"sst_{out_seq:06d}.sst", f"sst_{out_seq:06d}.tmp"
        final_path, tmp_path = self.data_dir / out_name, self.data_dir / tmp_name
        final_bloom = self._bloom_path(final_path)
        tmp_bloom = self.data_dir / f"{final_bloom.name}.tmp"

        compact(input_tables, tmp_path, drop_tombstones=True)
        keys = [rec.key for rec in SSTable(tmp_path).items()]
        bf = BloomFilter(expected_items=max(1, len(keys)), fp_rate=0.01)
        for k in keys: bf.add(k)
        with open(tmp_bloom, "wb") as f:
            f.write(bf.to_bytes())
            f.flush()
            os.fsync(f.fileno())
        if step == 1: raise RuntimeError("Simulated crash after step 1")

        marker_path = self.data_dir / "compaction.marker"
        with open(marker_path, "w", encoding="utf-8") as f:
            json.dump({"output": out_name, "inputs": in_names}, f)
            f.flush()
            os.fsync(f.fileno())
        if step == 2: raise RuntimeError("Simulated crash after step 2")

        os.replace(tmp_path, final_path)
        os.replace(tmp_bloom, final_bloom)
        if step == 3: raise RuntimeError("Simulated crash after step 3")

        for t in input_tables:
            if hasattr(t, "close"): t.close()
            t.path.unlink(missing_ok=True)
            self._bloom_path(t.path).unlink(missing_ok=True)
            self._bloom_filters.pop(t.path, None)
        if step == 4: raise RuntimeError("Simulated crash after step 4")

        marker_path.unlink(missing_ok=True)
        if step == 5: raise RuntimeError("Simulated crash after step 5")

        compacted = SSTable(final_path)
        self._sstables = [compacted]
        self._bloom_filters[compacted.path] = bf
        return compacted

    def close(self) -> None:
        with self._lock:
            if self._closed: return
            self._closed = True
            if self._wal is not None: self._wal.close()

    @property
    def memtable(self) -> MemTable: return self._memtable
    @property
    def sstables(self) -> list[SSTable]: return list(self._sstables)
    @property
    def wal(self) -> WAL: return self._wal
    @property
    def is_closed(self) -> bool: return self._closed
    @property
    def crash_at_step(self) -> Optional[int]: return self.crash_step
    @crash_at_step.setter
    def crash_at_step(self, val: Optional[int]) -> None: self.crash_step = val

    def __contains__(self, k: Union[bytes, str]) -> bool: return self.get(k) is not None
    def __getitem__(self, k: Union[bytes, str]) -> bytes:
        v = self.get(k)
        if v is None: raise KeyError(k)
        return v
    def __setitem__(self, k: Union[bytes, str], v: Union[bytes, str]) -> None: self.put(k, v)
    def __delitem__(self, k: Union[bytes, str]) -> None: self.delete(k)
    def __enter__(self) -> "LSMStore": return self
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: self.close()


Store = LSMStore

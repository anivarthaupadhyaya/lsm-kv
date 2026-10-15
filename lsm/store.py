"""LSM-Tree storage engine coordinating WAL, MemTable, and SSTables."""

import json
import os
import re
import threading
from pathlib import Path
from typing import Optional, Union

from lsm.compaction import compact
from lsm.memtable import MISSING, TOMBSTONE, MemTable
from lsm.sstable import SSTable, write_sstable
from lsm.wal import WAL

SST_PATTERN = re.compile(r"^sst_(\d+)\.sst$")


class LSMStore:
    """Single-node LSM-tree key-value store engine."""

    def __init__(
        self,
        data_dir: Union[str, Path],
        memtable_limit_bytes: Optional[int] = None,
        *,
        memtable_size_limit: Optional[int] = None,
        memtable_limit: Optional[int] = None,
        fsync_policy: Union[str, int, float] = "always",
        compaction_threshold: int = 4,
        crash_step: Optional[int] = None,
        crash_at_step: Optional[int] = None,
    ) -> None:
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)
        limit = memtable_limit_bytes if memtable_limit_bytes is not None else (memtable_size_limit or memtable_limit)
        self.memtable_limit_bytes = self.memtable_size_limit = int(limit) if limit is not None else 4096
        self._fsync_policy = fsync_policy
        self.compaction_threshold = compaction_threshold
        self.crash_step = crash_step if crash_step is not None else crash_at_step
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

    def _recover_compaction(self) -> None:
        """Startup recovery for crashed compactions based on compaction.marker."""
        marker = self.data_dir / "compaction.marker"
        if not marker.exists():
            return
        out_name, in_names = self._parse_marker(marker)
        if out_name and (self.data_dir / Path(out_name).name).exists():
            for name in in_names:
                p = self.data_dir / Path(name).name
                if p.exists():
                    try: p.unlink()
                    except OSError: pass
            try: marker.unlink()
            except OSError: pass
        else:
            try: marker.unlink()
            except OSError: pass
            self._cleanup_tmp_files()

    @staticmethod
    def _parse_marker(marker: Path) -> tuple[Optional[str], list[str]]:
        """Parses compaction.marker returning (output_name, input_names)."""
        try:
            content = marker.read_text(encoding="utf-8").strip()
            if not content:
                return None, []
            try:
                data = json.loads(content)
                if isinstance(data, dict):
                    return data.get("output"), data.get("inputs", [])
            except Exception:
                pass
            output, inputs = None, []
            for ln in (line.strip() for line in content.splitlines() if line.strip()):
                if ln.lower().startswith("output:"):
                    output = ln.split(":", 1)[1].strip()
                elif ln.lower().startswith("input:"):
                    inputs.append(ln.split(":", 1)[1].strip())
                elif output is None:
                    output = ln
                else:
                    inputs.append(ln)
            return output, inputs
        except Exception:
            return None, []

    def _cleanup_tmp_files(self) -> None:
        """Removes any leftover temporary files from aborted flushes."""
        for p in self.data_dir.iterdir():
            if p.is_file() and p.name.endswith(".tmp"):
                try: p.unlink()
                except OSError: pass

    def _load_sstables(self) -> int:
        """Loads SSTables sorted newest-to-oldest, returning next sequence number."""
        sst_entries: list[tuple[int, Path]] = []
        for p in self.data_dir.iterdir():
            if p.is_file() and (m := SST_PATTERN.match(p.name)):
                sst_entries.append((int(m.group(1)), p))
        sst_entries.sort(key=lambda x: x[0])
        self._sstables = [SSTable(p) for _, p in reversed(sst_entries)]
        return sst_entries[-1][0] + 1 if sst_entries else 1

    def _resolve_wal_path(self) -> Path:
        """Finds existing WAL file if present, or defaults to wal.log."""
        for name in ("wal.log", "wal.wal"):
            p = self.data_dir / name
            if p.exists():
                return p
        return self.data_dir / "wal.log"

    def put(self, key: Union[bytes, str], value: Optional[Union[bytes, str]]) -> None:
        """Writes a key-value pair. Appends to WAL first, then updates MemTable."""
        with self._lock:
            if self._closed:
                raise ValueError("Store is closed.")
            self._wal.append(key, value)
            self._memtable.put(key, value)
            if self._memtable.size_bytes() >= self.memtable_limit_bytes:
                self._flush_locked()

    def delete(self, key: Union[bytes, str]) -> None:
        """Deletes a key by appending a tombstone to WAL and MemTable."""
        with self._lock:
            if self._closed:
                raise ValueError("Store is closed.")
            self._wal.append(key, None)
            self._memtable.delete(key)
            if self._memtable.size_bytes() >= self.memtable_limit_bytes:
                self._flush_locked()

    def get(self, key: Union[bytes, str], default: object = None) -> Optional[bytes]:
        """Retrieves value for key (MemTable first, then SSTables newest to oldest)."""
        with self._lock:
            if self._closed:
                raise ValueError("Store is closed.")
            val = self._memtable.get(key, default=MISSING)
            if val is not MISSING:
                return default if val is TOMBSTONE else val
            for sst in self._sstables:
                val = sst.get(key, default=MISSING)
                if val is not MISSING:
                    return default if val is TOMBSTONE else val
            return default

    def flush(self) -> Optional[SSTable]:
        """Manually flushes the current MemTable to an immutable SSTable."""
        with self._lock:
            if self._closed:
                raise ValueError("Store is closed.")
            return self._flush_locked()

    def _flush_locked(self) -> Optional[SSTable]:
        """Flushes MemTable to SSTable. Must be called while holding self._lock."""
        if len(self._memtable) == 0:
            return None
        seq = self._next_seq
        self._next_seq += 1
        final_path = self.data_dir / f"sst_{seq:06d}.sst"
        tmp_path = self.data_dir / f"sst_{seq:06d}.tmp"

        write_sstable(tmp_path, self._memtable.items())
        os.replace(tmp_path, final_path)
        sstable = SSTable(final_path)
        self._sstables.insert(0, sstable)

        if self._wal is not None:
            self._wal.close()
        if self._wal_path.exists():
            self._wal_path.unlink()
        self._wal = WAL(self._wal_path, fsync_policy=self._fsync_policy)
        self._memtable = MemTable()

        if self.compaction_threshold and len(self._sstables) > self.compaction_threshold:
            self._compact_locked()
        return sstable

    def compact(self, crash_step: Optional[int] = None) -> Optional[SSTable]:
        """Manually compacts all SSTables into one using crash-safe protocol."""
        with self._lock:
            if self._closed:
                raise ValueError("Store is closed.")
            return self._compact_locked(crash_step=crash_step)

    def _compact_locked(self, crash_step: Optional[int] = None) -> Optional[SSTable]:
        """Merges all SSTables into a single table with crash-safety protocol."""
        if len(self._sstables) <= 1:
            return None
        step = self.crash_step if crash_step is None else crash_step
        input_tables = list(reversed(self._sstables))
        in_names = [t.path.name for t in input_tables]
        max_in_seq = max((int(m.group(1)) for t in input_tables if (m := SST_PATTERN.match(t.path.name))), default=0)
        out_seq = max(self._next_seq, max_in_seq + 1)
        self._next_seq = out_seq + 1
        out_name, tmp_name = f"sst_{out_seq:06d}.sst", f"sst_{out_seq:06d}.tmp"
        final_path, tmp_path = self.data_dir / out_name, self.data_dir / tmp_name

        # (1) write output to .tmp and fsync
        compact(input_tables, tmp_path, drop_tombstones=True)
        if step == 1:
            raise RuntimeError("Simulated crash after step 1")

        # (2) write compaction.marker file and fsync
        marker_path = self.data_dir / "compaction.marker"
        with open(marker_path, "w", encoding="utf-8") as f:
            json.dump({"output": out_name, "inputs": in_names}, f)
            f.flush()
            os.fsync(f.fileno())
        if step == 2:
            raise RuntimeError("Simulated crash after step 2")

        # (3) os.replace .tmp to final name
        os.replace(tmp_path, final_path)
        if step == 3:
            raise RuntimeError("Simulated crash after step 3")

        # (4) delete input files (close handles first)
        for t in input_tables:
            if hasattr(t, "close"):
                t.close()
        for t in input_tables:
            if t.path.exists():
                try: t.path.unlink()
                except OSError: pass
        if step == 4:
            raise RuntimeError("Simulated crash after step 4")

        # (5) delete marker
        if marker_path.exists():
            try: marker_path.unlink()
            except OSError: pass
        if step == 5:
            raise RuntimeError("Simulated crash after step 5")

        compacted = SSTable(final_path)
        self._sstables = [compacted]
        return compacted

    def close(self) -> None:
        """Closes the engine and its active WAL."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._wal is not None:
                self._wal.close()

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

    def __contains__(self, key: Union[bytes, str]) -> bool: return self.get(key) is not None
    def __getitem__(self, key: Union[bytes, str]) -> bytes:
        val = self.get(key)
        if val is None: raise KeyError(key)
        return val
    def __setitem__(self, key: Union[bytes, str], value: Union[bytes, str]) -> None: self.put(key, value)
    def __delitem__(self, key: Union[bytes, str]) -> None: self.delete(key)
    def __enter__(self) -> "LSMStore": return self
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: self.close()


Store = LSMStore

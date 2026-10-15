"""LSM-Tree storage engine coordinating WAL, MemTable, and SSTables."""

import os
import re
import threading
from pathlib import Path
from typing import Optional, Union

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
    ) -> None:
        self.data_dir = Path(data_dir).resolve()
        self.data_dir.mkdir(parents=True, exist_ok=True)

        limit = (
            memtable_limit_bytes
            if memtable_limit_bytes is not None
            else (memtable_size_limit if memtable_size_limit is not None else memtable_limit)
        )
        self.memtable_limit_bytes = int(limit) if limit is not None else 4096
        self.memtable_size_limit = self.memtable_limit_bytes
        self._fsync_policy = fsync_policy

        self._lock = threading.RLock()
        self._closed = False

        # 1. Ignore and delete leftover .tmp files
        self._cleanup_tmp_files()

        # 2. Discover and load existing SSTables ordered by sequence number
        self._sstables: list[SSTable] = []
        self._next_seq = self._load_sstables()

        # 3. Locate WAL path, replay existing WAL into fresh memtable, then open WAL
        self._wal_path = self._resolve_wal_path()
        self._memtable = MemTable()
        if self._wal_path.exists():
            for record in WAL.replay_file(self._wal_path):
                self._memtable.put(record.key, record.value)

        self._wal = WAL(self._wal_path, fsync_policy=self._fsync_policy)

    def _cleanup_tmp_files(self) -> None:
        """Removes any leftover temporary files from aborted flushes."""
        for p in self.data_dir.iterdir():
            if p.is_file() and p.name.endswith(".tmp"):
                try:
                    p.unlink()
                except OSError:
                    pass

    def _load_sstables(self) -> int:
        """Loads SSTables sorted newest-to-oldest, returning next sequence number."""
        sst_entries: list[tuple[int, Path]] = []
        for p in self.data_dir.iterdir():
            if p.is_file():
                m = SST_PATTERN.match(p.name)
                if m:
                    sst_entries.append((int(m.group(1)), p))

        # Sort ascending to find max sequence number
        sst_entries.sort(key=lambda x: x[0])
        next_seq = sst_entries[-1][0] + 1 if sst_entries else 1

        # Store in newest-to-oldest order for reads
        self._sstables = [SSTable(p) for _, p in reversed(sst_entries)]
        return next_seq

    def _resolve_wal_path(self) -> Path:
        """Finds existing WAL file if present, or defaults to wal.log."""
        for name in ("wal.log", "wal.wal"):
            p = self.data_dir / name
            if p.exists():
                return p
        return self.data_dir / "wal.log"

    def put(
        self,
        key: Union[bytes, str],
        value: Optional[Union[bytes, str]],
    ) -> None:
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

    def get(
        self,
        key: Union[bytes, str],
        default: object = None,
    ) -> Optional[bytes]:
        """Retrieves value for key.

        Checks MemTable first, then SSTables newest-to-oldest.
        First hit wins; tombstones immediately return 'not found' (default).
        """
        with self._lock:
            if self._closed:
                raise ValueError("Store is closed.")

            # 1. Check MemTable
            val = self._memtable.get(key, default=MISSING)
            if val is not MISSING:
                if val is TOMBSTONE:
                    return default
                return val

            # 2. Check SSTables from newest to oldest
            for sst in self._sstables:
                val = sst.get(key, default=MISSING)
                if val is not MISSING:
                    if val is TOMBSTONE:
                        return default
                    return val

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
        final_name = f"sst_{seq:06d}.sst"
        tmp_name = f"sst_{seq:06d}.tmp"
        final_path = self.data_dir / final_name
        tmp_path = self.data_dir / tmp_name

        # 1. Write MemTable to temporary SSTable (.tmp) and fsync
        write_sstable(tmp_path, self._memtable.items())

        # 2. Atomically rename/replace to final SSTable name
        os.replace(tmp_path, final_path)

        # 3. Add to head of SSTables list (newest first)
        sstable = SSTable(final_path)
        self._sstables.insert(0, sstable)

        # 4. Start fresh WAL and delete old one
        # Close old WAL file handle before deleting it (Windows file lock requirement)
        if self._wal is not None:
            self._wal.close()
        if self._wal_path.exists():
            self._wal_path.unlink()
        self._wal = WAL(self._wal_path, fsync_policy=self._fsync_policy)

        # 5. Clear MemTable with a fresh one
        self._memtable = MemTable()

        return sstable

    def close(self) -> None:
        """Closes the engine and its active WAL."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            if self._wal is not None:
                self._wal.close()

    @property
    def memtable(self) -> MemTable:
        return self._memtable

    @property
    def sstables(self) -> list[SSTable]:
        return list(self._sstables)

    @property
    def wal(self) -> WAL:
        return self._wal

    @property
    def is_closed(self) -> bool:
        return self._closed

    def __contains__(self, key: Union[bytes, str]) -> bool:
        return self.get(key) is not None

    def __getitem__(self, key: Union[bytes, str]) -> bytes:
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: Union[bytes, str], value: Union[bytes, str]) -> None:
        self.put(key, value)

    def __delitem__(self, key: Union[bytes, str]) -> None:
        self.delete(key)

    def __enter__(self) -> "LSMStore":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


Store = LSMStore

"""Immutable Sorted-String-Table (SSTable) file writer and reader for LSM-Tree.

File Layout:
  +-------------------------------------------------------+
  | Data Section                                          |
  |   Record 0: [key_len: uint32][val_len: int32][k][v]   |
  |   Record 1: ...                                       |
  +-------------------------------------------------------+
  | Sparse Index                                          |
  |   num_entries: uint32                                 |
  |   Entry 0: [offset: uint64][key_len: uint32][key]     |
  |   Entry 1: ...                                        |
  +-------------------------------------------------------+
  | Footer (12 bytes fixed)                               |
  |   index_offset: uint64 (8 bytes)                      |
  |   magic: uint32 (4 bytes, 0x53535442 = 'SSTB')       |
  +-------------------------------------------------------+
"""

import bisect
import os
import struct
from pathlib import Path
from typing import Iterable, Iterator, Optional, Union

from lsm.memtable import MISSING, TOMBSTONE
from lsm.wal import Record

MAGIC = 0x53535442  # 'SSTB'
FOOTER_FORMAT = ">QI"
FOOTER_SIZE = struct.calcsize(FOOTER_FORMAT)

RECORD_HEADER_FORMAT = ">Ii"
RECORD_HEADER_SIZE = struct.calcsize(RECORD_HEADER_FORMAT)

INDEX_ENTRY_HEADER = ">QI"
INDEX_ENTRY_HEADER_SIZE = struct.calcsize(INDEX_ENTRY_HEADER)


def write_sstable(
    path: Union[str, Path],
    items: Iterable[Union[Record, tuple[Union[bytes, str], Optional[Union[bytes, str]]]]],
    index_interval: int = 16,
) -> "SSTable":
    """Writes sorted key-value pairs or tombstones into an immutable SSTable file.

    Parameters:
      path: Destination file path.
      items: Sorted sequence of (key, value) pairs (e.g. from MemTable.items()).
      index_interval: Sparse index frequency (record 1 entry every N records).
    """
    sstable_path = Path(path).resolve()
    sstable_path.parent.mkdir(parents=True, exist_ok=True)

    index_entries: list[tuple[bytes, int]] = []
    count = 0

    with open(sstable_path, "wb") as f:
        # 1. Data Section
        for item in items:
            key, val = item[0], item[1]
            if isinstance(key, str):
                key_bytes = key.encode("utf-8")
            elif isinstance(key, (bytes, bytearray)):
                key_bytes = bytes(key)
            else:
                raise TypeError(f"Key must be bytes or str, got {type(key).__name__}")

            if val is None:
                val_bytes = None
            elif isinstance(val, str):
                val_bytes = val.encode("utf-8")
            elif isinstance(val, (bytes, bytearray)):
                val_bytes = bytes(val)
            else:
                raise TypeError(f"Value must be bytes, str, or None, got {type(val).__name__}")

            offset = f.tell()
            if count % index_interval == 0:
                index_entries.append((key_bytes, offset))

            val_len = -1 if val_bytes is None else len(val_bytes)
            f.write(struct.pack(RECORD_HEADER_FORMAT, len(key_bytes), val_len))
            f.write(key_bytes)
            if val_bytes is not None:
                f.write(val_bytes)

            count += 1

        # 2. Sparse Index
        index_offset = f.tell()
        f.write(struct.pack(">I", len(index_entries)))
        for ikey, ioffset in index_entries:
            f.write(struct.pack(INDEX_ENTRY_HEADER, ioffset, len(ikey)))
            f.write(ikey)

        # 3. Footer
        f.write(struct.pack(FOOTER_FORMAT, index_offset, MAGIC))
        f.flush()
        os.fsync(f.fileno())

    return SSTable(sstable_path)


class SSTable:
    """Reader for an immutable SSTable file."""

    def __init__(self, path: Union[str, Path]) -> None:
        self.path = Path(path).resolve()
        if not self.path.exists():
            raise FileNotFoundError(f"SSTable file not found: {self.path}")

        self._index_keys: list[bytes] = []
        self._index_offsets: list[int] = []
        self._index_offset: int = 0
        self._last_scan_count: int = 0

        self._read_footer_and_index()

    def _read_footer_and_index(self) -> None:
        file_size = self.path.stat().st_size
        if file_size < FOOTER_SIZE:
            raise ValueError(f"SSTable file size ({file_size}) is smaller than footer ({FOOTER_SIZE})")

        with open(self.path, "rb") as f:
            f.seek(file_size - FOOTER_SIZE)
            footer_bytes = f.read(FOOTER_SIZE)
            index_offset, magic = struct.unpack(FOOTER_FORMAT, footer_bytes)

            if magic != MAGIC:
                raise ValueError(f"Invalid SSTable magic number: expected {hex(MAGIC)}, got {hex(magic)}")

            if index_offset > file_size - FOOTER_SIZE:
                raise ValueError("Corrupt SSTable: index offset exceeds file boundary")

            self._index_offset = index_offset

            f.seek(index_offset)
            num_entries_bytes = f.read(4)
            if len(num_entries_bytes) < 4:
                raise ValueError("Corrupt SSTable: missing index count")

            num_entries, = struct.unpack(">I", num_entries_bytes)
            for _ in range(num_entries):
                entry_hdr = f.read(INDEX_ENTRY_HEADER_SIZE)
                if len(entry_hdr) < INDEX_ENTRY_HEADER_SIZE:
                    raise ValueError("Corrupt SSTable: truncated index entry header")
                ioffset, key_len = struct.unpack(INDEX_ENTRY_HEADER, entry_hdr)
                ikey = f.read(key_len)
                if len(ikey) < key_len:
                    raise ValueError("Corrupt SSTable: truncated index key")
                self._index_keys.append(ikey)
                self._index_offsets.append(ioffset)

    @staticmethod
    def _to_bytes(data: Union[bytes, str], name: str = "key") -> bytes:
        if isinstance(data, str):
            return data.encode("utf-8")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        raise TypeError(f"{name} must be bytes or str, got {type(data).__name__}")

    def get(
        self,
        key: Union[bytes, str],
        default: object = MISSING,
    ) -> Union[bytes, Optional[bytes], object]:
        """Retrieves value for key using sparse index binary search.

        Scans only the relevant block between sparse index entries.
        Returns:
          - bytes: if key exists with a value
          - None: if key was deleted (tombstone)
          - MISSING (or default): if key is not in this table
        """
        key_bytes = self._to_bytes(key, "Key")
        if not self._index_keys:
            self._last_scan_count = 0
            return default

        # If key is smaller than the smallest key in this table, it cannot be present
        if key_bytes < self._index_keys[0]:
            self._last_scan_count = 0
            return default

        # Find largest index entry <= key_bytes
        idx = bisect.bisect_right(self._index_keys, key_bytes) - 1
        start_offset = self._index_offsets[idx]

        if idx + 1 < len(self._index_offsets):
            end_offset = self._index_offsets[idx + 1]
        else:
            end_offset = self._index_offset

        scan_count = 0
        with open(self.path, "rb") as f:
            f.seek(start_offset)
            while f.tell() < end_offset:
                header_bytes = f.read(RECORD_HEADER_SIZE)
                if len(header_bytes) < RECORD_HEADER_SIZE:
                    break
                scan_count += 1
                key_len, val_len = struct.unpack(RECORD_HEADER_FORMAT, header_bytes)
                curr_key = f.read(key_len)

                if curr_key == key_bytes:
                    self._last_scan_count = scan_count
                    if val_len == -1:
                        return TOMBSTONE
                    return f.read(val_len)

                if curr_key > key_bytes:
                    # Sorted data: passed the target key
                    self._last_scan_count = scan_count
                    return default

                if val_len > 0:
                    f.seek(val_len, os.SEEK_CUR)

        self._last_scan_count = scan_count
        return default

    def items(self) -> Iterator[Record]:
        """Yields all Record(key, value) in ascending sorted order from the data section."""
        with open(self.path, "rb") as f:
            while f.tell() < self._index_offset:
                header_bytes = f.read(RECORD_HEADER_SIZE)
                if len(header_bytes) < RECORD_HEADER_SIZE:
                    break
                key_len, val_len = struct.unpack(RECORD_HEADER_FORMAT, header_bytes)
                key = f.read(key_len)
                if val_len == -1:
                    val = None
                else:
                    val = f.read(val_len)
                yield Record(key=key, value=val)

    def is_deleted(self, key: Union[bytes, str]) -> bool:
        """Returns True if the key exists and is a tombstone."""
        return self.get(key) is TOMBSTONE

    def __contains__(self, key: Union[bytes, str]) -> bool:
        """Returns True if key is present in table (including tombstones)."""
        return self.get(key) is not MISSING

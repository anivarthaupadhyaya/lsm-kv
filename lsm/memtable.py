"""In-memory sorted table (MemTable) for LSM-Tree.

Maintains key-value pairs and tombstones in lexicographically sorted key order
using a bisect-backed sorted list.
"""

import bisect
from typing import Iterator, Optional, Union

from lsm.wal import Record


class _Missing:
    """Sentinel representing a key that was never written to the MemTable."""

    _instance = None

    def __new__(cls) -> "_Missing":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "<MISSING>"

    def __bool__(self) -> bool:
        return False

    def __eq__(self, other: object) -> bool:
        return other is self


MISSING = _Missing()
TOMBSTONE = None


class MemTable:
    """Sorted in-memory table storing keys, values, and tombstones.

    Keys are maintained in sorted byte order.
    Tombstones are represented with value=None (reusing Record from lsm.wal).
    """

    def __init__(self) -> None:
        self._keys: list[bytes] = []
        self._values: list[Optional[bytes]] = []
        self._size_bytes: int = 0

    @staticmethod
    def _to_bytes(data: Union[bytes, str], name: str = "key") -> bytes:
        if isinstance(data, str):
            return data.encode("utf-8")
        if isinstance(data, (bytes, bytearray)):
            return bytes(data)
        raise TypeError(f"{name} must be bytes or str, got {type(data).__name__}")

    def put(
        self,
        key: Union[bytes, str],
        value: Optional[Union[bytes, str]],
    ) -> None:
        """Inserts or updates a key-value pair. If value is None, stores a tombstone."""
        key_bytes = self._to_bytes(key, "Key")

        if value is None:
            val_bytes = None
        elif isinstance(value, str):
            val_bytes = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray)):
            val_bytes = bytes(value)
        else:
            raise TypeError(f"Value must be bytes, str, or None, got {type(value).__name__}")

        idx = bisect.bisect_left(self._keys, key_bytes)
        new_val_len = len(val_bytes) if val_bytes is not None else 0

        if idx < len(self._keys) and self._keys[idx] == key_bytes:
            # Overwrite existing key
            old_val = self._values[idx]
            old_val_len = len(old_val) if old_val is not None else 0
            self._values[idx] = val_bytes
            self._size_bytes += new_val_len - old_val_len
        else:
            # Insert new key
            self._keys.insert(idx, key_bytes)
            self._values.insert(idx, val_bytes)
            self._size_bytes += len(key_bytes) + new_val_len

    def get(
        self,
        key: Union[bytes, str],
        default: object = MISSING,
    ) -> Union[bytes, Optional[bytes], object]:
        """Retrieves the value for key.

        Returns:
          - bytes: if key exists with a value
          - None: if key exists as a tombstone (deleted)
          - MISSING (or default): if key was never written
        """
        key_bytes = self._to_bytes(key, "Key")
        idx = bisect.bisect_left(self._keys, key_bytes)

        if idx < len(self._keys) and self._keys[idx] == key_bytes:
            return self._values[idx]
        return default

    def delete(self, key: Union[bytes, str]) -> None:
        """Deletes a key by inserting a tombstone (value=None)."""
        self.put(key, None)

    def items(self) -> Iterator[Record]:
        """Yields Record(key, value) in ascending lexicographical key order.

        Includes tombstones where record.value is None.
        """
        for k, v in zip(self._keys, self._values):
            yield Record(key=k, value=v)

    def size_bytes(self) -> int:
        """Returns approximate total memory size of stored keys and values in bytes."""
        return self._size_bytes

    def is_deleted(self, key: Union[bytes, str]) -> bool:
        """Returns True if the key exists and is marked as a tombstone."""
        val = self.get(key)
        return val is TOMBSTONE

    def __contains__(self, key: Union[bytes, str]) -> bool:
        """Returns True if key is present in table (including tombstones)."""
        return self.get(key) is not MISSING

    def __len__(self) -> int:
        return len(self._keys)

    def clear(self) -> None:
        """Clears all entries from the MemTable."""
        self._keys.clear()
        self._values.clear()
        self._size_bytes = 0

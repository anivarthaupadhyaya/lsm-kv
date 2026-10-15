"""Write-Ahead Log (WAL) module for append-only crash-resilient persistence.

Record Format:
  [4-byte length][4-byte crc32][key][value or tombstone flag]

Details:
  - Header:
      length: 4 bytes unsigned int (big-endian), byte length of payload
      crc32:  4 bytes unsigned int (big-endian), CRC32 checksum of payload
  - Payload:
      key_length:     4 bytes unsigned int (big-endian)
      key:            key_length bytes
      tombstone_flag: 1 byte (0x01 = tombstone, 0x00 = normal value)
      value:          remaining bytes in payload (if tombstone_flag == 0x00)
"""

import os
import struct
import time
import threading
import zlib
from pathlib import Path
from typing import Iterator, NamedTuple, Optional, Union

HEADER_FORMAT = ">II"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
PAYLOAD_HEADER_FORMAT = ">IB"
PAYLOAD_HEADER_SIZE = struct.calcsize(PAYLOAD_HEADER_FORMAT)

FLAG_VALUE = 0
FLAG_TOMBSTONE = 1


class Record(NamedTuple):
    """Represents a WAL record yielding (key, value).

    If value is None, the record is a tombstone (deletion).
    Supports attribute access (r.key, r.value) and tuple unpacking (k, v = r).
    """
    key: bytes
    value: Optional[bytes]

    @property
    def is_tombstone(self) -> bool:
        return self.value is None


class WAL:
    """Append-only Write-Ahead Log with configurable fsync policy."""

    def __init__(
        self,
        path: Union[str, Path],
        fsync_policy: Union[str, int, float] = "always",
        fsync_interval_ms: Optional[Union[int, float]] = None,
    ):
        self.path = Path(path).resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._dirty = False
        self._closed = False
        self._last_fsync = time.monotonic()

        # Resolve fsync policy: 0 = every write, >0 = periodic every N ms, <0 = never (on close)
        self.fsync_interval_ms = self._parse_fsync_policy(fsync_policy, fsync_interval_ms)

        # Open in append-binary mode
        self._file = open(self.path, "a+b")

        # Start periodic background fsync worker if interval policy is selected
        self._stop_worker = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        if self.fsync_interval_ms > 0:
            self._worker_thread = threading.Thread(
                target=self._periodic_sync_worker,
                name="WAL-FsyncWorker",
                daemon=True,
            )
            self._worker_thread.start()

    @staticmethod
    def _parse_fsync_policy(
        policy: Union[str, int, float],
        interval_ms: Optional[Union[int, float]],
    ) -> float:
        if interval_ms is not None:
            return float(interval_ms)
        if isinstance(policy, (int, float)) and not isinstance(policy, bool):
            return float(policy)
        policy_str = str(policy).strip().lower()
        if policy_str in ("always", "every_write", "write", "true"):
            return 0.0
        if policy_str in ("never", "none", "false"):
            return -1.0
        try:
            return float(policy_str)
        except ValueError:
            raise ValueError(f"Unknown fsync policy: {policy}")

    def append(
        self,
        key: Union[bytes, str],
        value: Optional[Union[bytes, str]] = None,
    ) -> int:
        """Appends a key-value pair or tombstone (value=None) to the WAL.

        Returns the number of bytes written.
        """
        if isinstance(key, str):
            key_bytes = key.encode("utf-8")
        elif isinstance(key, (bytes, bytearray)):
            key_bytes = bytes(key)
        else:
            raise TypeError(f"Key must be bytes or str, got {type(key).__name__}")

        if value is None:
            is_tombstone = True
            val_bytes = b""
        elif isinstance(value, str):
            is_tombstone = False
            val_bytes = value.encode("utf-8")
        elif isinstance(value, (bytes, bytearray)):
            is_tombstone = False
            val_bytes = bytes(value)
        else:
            raise TypeError(f"Value must be bytes, str, or None, got {type(value).__name__}")

        flag = FLAG_TOMBSTONE if is_tombstone else FLAG_VALUE
        payload_header = struct.pack(PAYLOAD_HEADER_FORMAT, len(key_bytes), flag)
        payload = payload_header + key_bytes + (b"" if is_tombstone else val_bytes)

        payload_len = len(payload)
        checksum = zlib.crc32(payload) & 0xFFFFFFFF
        header = struct.pack(HEADER_FORMAT, payload_len, checksum)
        entry = header + payload

        with self._lock:
            if self._closed:
                raise ValueError("Cannot append to a closed WAL.")
            self._file.write(entry)
            self._dirty = True

            if self.fsync_interval_ms == 0:
                self._flush_and_fsync_locked()
            elif self.fsync_interval_ms > 0:
                now = time.monotonic()
                if (now - self._last_fsync) * 1000.0 >= self.fsync_interval_ms:
                    self._flush_and_fsync_locked()

        return len(entry)

    def _flush_and_fsync_locked(self) -> None:
        """Flushes user-space buffers and issues an OS fsync. Must hold self._lock."""
        if self._file and not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._dirty = False
            self._last_fsync = time.monotonic()

    def sync(self) -> None:
        """Manually flush and fsync uncommitted writes."""
        with self._lock:
            if not self._closed and self._dirty:
                self._flush_and_fsync_locked()

    def _periodic_sync_worker(self) -> None:
        interval_sec = max(0.001, self.fsync_interval_ms / 1000.0)
        while not self._stop_worker.wait(interval_sec):
            self.sync()

    def replay(self) -> Iterator[Record]:
        """Yields records from this WAL file.

        Safely stops when encountering a truncated or corrupt tail.
        """
        self.sync()
        yield from self.replay_file(self.path)

    @classmethod
    def replay_file(cls, path: Union[str, Path]) -> Iterator[Record]:
        """Class method to replay any WAL file without requiring an open WAL instance.

        Stops safely at clean EOF, truncated last record, or corrupted checksum.
        """
        wal_path = Path(path)
        if not wal_path.exists():
            return

        with open(wal_path, "rb") as f:
            while True:
                header_bytes = f.read(HEADER_SIZE)
                if not header_bytes:
                    break
                if len(header_bytes) < HEADER_SIZE:
                    # Truncated header at tail
                    break

                payload_len, expected_crc = struct.unpack(HEADER_FORMAT, header_bytes)

                payload = f.read(payload_len)
                if len(payload) < payload_len:
                    # Truncated payload at tail
                    break

                computed_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if computed_crc != expected_crc:
                    # Corrupted checksum at tail
                    break

                if len(payload) < PAYLOAD_HEADER_SIZE:
                    # Corrupted inner payload
                    break

                key_len, flag = struct.unpack_from(PAYLOAD_HEADER_FORMAT, payload, 0)
                if PAYLOAD_HEADER_SIZE + key_len > len(payload):
                    # Key length exceeds payload
                    break

                key = payload[PAYLOAD_HEADER_SIZE : PAYLOAD_HEADER_SIZE + key_len]

                if flag == FLAG_TOMBSTONE:
                    value = None
                elif flag == FLAG_VALUE:
                    value = payload[PAYLOAD_HEADER_SIZE + key_len :]
                else:
                    # Corrupted tombstone flag
                    break

                yield Record(key=key, value=value)

    def close(self) -> None:
        """Closes the WAL, stopping background workers and syncing pending data."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        if self._worker_thread is not None:
            self._stop_worker.set()
            self._worker_thread.join(timeout=2.0)

        with self._lock:
            if self._file and not self._file.closed:
                if self._dirty:
                    self._flush_and_fsync_locked()
                self._file.close()

    def __enter__(self) -> "WAL":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

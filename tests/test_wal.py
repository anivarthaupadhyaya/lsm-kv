import os
import struct
import time
from pathlib import Path
from unittest.mock import patch

import pytest
from lsm.wal import WAL, Record, HEADER_SIZE


@pytest.fixture
def wal_path(tmp_path: Path) -> Path:
    return tmp_path / "test.wal"


def test_normal_replay(wal_path: Path):
    """Verifies sequential append of puts, deletes (tombstones), and empty values."""
    with WAL(wal_path) as wal:
        wal.append(b"key1", b"val1")
        wal.append("key2", "val2")
        wal.append(b"key3", None)  # Tombstone
        wal.append(b"key4", b"")    # Empty value
        wal.append("key5", None)   # Tombstone via string key

    # Replay from open WAL and class method
    with WAL(wal_path) as wal:
        records = list(wal.replay())

    assert len(records) == 5

    # 1: key1 -> val1
    assert records[0] == Record(key=b"key1", value=b"val1")
    assert not records[0].is_tombstone
    k, v = records[0]
    assert (k, v) == (b"key1", b"val1")

    # 2: key2 -> val2
    assert records[1] == Record(key=b"key2", value=b"val2")
    assert not records[1].is_tombstone

    # 3: key3 -> tombstone
    assert records[2].key == b"key3"
    assert records[2].value is None
    assert records[2].is_tombstone

    # 4: key4 -> empty value
    assert records[3].key == b"key4"
    assert records[3].value == b""
    assert not records[3].is_tombstone

    # 5: key5 -> tombstone
    assert records[4].key == b"key5"
    assert records[4].is_tombstone


def test_truncated_last_record_in_header(wal_path: Path):
    """Replay stops safely when tail has a truncated length or crc32 header."""
    with WAL(wal_path) as wal:
        wal.append(b"k1", b"v1")
        wal.append(b"k2", b"v2")

    # Append 3 arbitrary bytes (fewer than 8-byte header)
    with open(wal_path, "ab") as f:
        f.write(b"XYZ")

    records = list(WAL.replay_file(wal_path))
    assert len(records) == 2
    assert records[0] == Record(b"k1", b"v1")
    assert records[1] == Record(b"k2", b"v2")


def test_truncated_last_record_in_payload(wal_path: Path):
    """Replay stops safely when tail has partial payload bytes."""
    with WAL(wal_path) as wal:
        wal.append(b"first", b"11111")
        wal.append(b"second", b"22222")

    raw_data = wal_path.read_bytes()
    # Truncate the file by 3 bytes from the end of the second record's payload
    wal_path.write_bytes(raw_data[:-3])

    records = list(WAL.replay_file(wal_path))
    assert len(records) == 1
    assert records[0] == Record(b"first", b"11111")


def test_corrupted_checksum(wal_path: Path):
    """Replay stops safely at corrupted CRC32 or corrupted payload bytes."""
    with WAL(wal_path) as wal:
        wal.append(b"good1", b"val1")
        wal.append(b"corrupt", b"val2")

    raw_data = bytearray(wal_path.read_bytes())

    # Flip the very last byte of the second record's payload
    raw_data[-1] ^= 0xFF
    wal_path.write_bytes(raw_data)

    records = list(WAL.replay_file(wal_path))
    # Should safely yield good1 and discard the corrupted record
    assert len(records) == 1
    assert records[0] == Record(b"good1", b"val1")


def test_corrupted_header_checksum(wal_path: Path):
    """Replay stops safely if the CRC32 field in the header is tampered with."""
    with WAL(wal_path) as wal:
        wal.append(b"rec1", b"val1")
        wal.append(b"rec2", b"val2")

    with open(wal_path, "rb") as f:
        # Find offset of second record
        h1 = f.read(HEADER_SIZE)
        payload_len1, _ = struct.unpack(">II", h1)
        f.read(payload_len1)
        rec2_offset = f.tell()

    raw_data = bytearray(wal_path.read_bytes())
    # The CRC32 is at offset + 4 bytes within header
    raw_data[rec2_offset + 4] ^= 0xAA
    wal_path.write_bytes(raw_data)

    records = list(WAL.replay_file(wal_path))
    assert len(records) == 1
    assert records[0] == Record(b"rec1", b"val1")


def test_corrupted_key_length(wal_path: Path):
    """Replay stops safely if key length inside payload claims to exceed payload size."""
    with WAL(wal_path) as wal:
        wal.append(b"valid", b"val")

    # Manually append an entry with valid CRC but corrupt internal key_len
    # payload: key_len=9999, flag=0, key=b"k"
    fake_payload = struct.pack(">IB", 9999, 0) + b"k"
    import zlib
    fake_crc = zlib.crc32(fake_payload) & 0xFFFFFFFF
    fake_header = struct.pack(">II", len(fake_payload), fake_crc)

    with open(wal_path, "ab") as f:
        f.write(fake_header + fake_payload)

    records = list(WAL.replay_file(wal_path))
    assert len(records) == 1
    assert records[0] == Record(b"valid", b"val")


def test_fsync_policy_every_write(wal_path: Path):
    """Verifies that fsync_policy='always' triggers os.fsync on each write."""
    with patch("os.fsync") as mock_fsync:
        with WAL(wal_path, fsync_policy="always") as wal:
            wal.append(b"k1", b"v1")
            assert mock_fsync.call_count == 1
            wal.append(b"k2", b"v2")
            assert mock_fsync.call_count == 2


def test_fsync_policy_interval(wal_path: Path):
    """Verifies periodic fsync worker flushes and syncs within the interval."""
    # 20ms interval
    with WAL(wal_path, fsync_interval_ms=20) as wal:
        wal.append(b"k1", b"v1")
        # Give periodic thread enough time to sync
        time.sleep(0.06)
        assert not wal._dirty

    # Should replay successfully
    records = list(WAL.replay_file(wal_path))
    assert len(records) == 1
    assert records[0] == Record(b"k1", b"v1")


def test_empty_or_nonexistent_file(tmp_path: Path):
    """Verifies empty or missing WAL files produce an empty generator safely."""
    missing = tmp_path / "missing.wal"
    assert list(WAL.replay_file(missing)) == []

    empty = tmp_path / "empty.wal"
    empty.touch()
    assert list(WAL.replay_file(empty)) == []


def test_reopen_and_append(wal_path: Path):
    """Verifies reopening an existing WAL continues appending properly."""
    wal1 = WAL(wal_path)
    wal1.append(b"k1", b"v1")
    wal1.close()

    wal2 = WAL(wal_path)
    wal2.append(b"k2", b"v2")
    wal2.close()

    records = list(WAL.replay_file(wal_path))
    assert records == [
        Record(b"k1", b"v1"),
        Record(b"k2", b"v2"),
    ]

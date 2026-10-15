from pathlib import Path
import pytest

from lsm.memtable import MemTable, MISSING, TOMBSTONE
from lsm.sstable import SSTable, write_sstable, FOOTER_SIZE, MAGIC
from lsm.wal import Record


@pytest.fixture
def sstable_path(tmp_path: Path) -> Path:
    return tmp_path / "test.sst"


def test_write_and_read_back(sstable_path: Path):
    """Verify writing records to an SSTable and reading them back with get()."""
    mt = MemTable()
    mt.put(b"k1", b"val1")
    mt.put(b"k2", b"val2")
    mt.put("k3", "val3")

    sst = write_sstable(sstable_path, mt.items())

    # Read back through returned instance and fresh instance
    assert sst.get(b"k1") == b"val1"
    assert sst.get("k2") == b"val2"
    assert sst.get(b"k3") == b"val3"

    fresh_sst = SSTable(sstable_path)
    assert fresh_sst.get(b"k1") == b"val1"
    assert fresh_sst.get(b"k2") == b"val2"
    assert fresh_sst.get(b"k3") == b"val3"


def test_missing_key(sstable_path: Path):
    """Verify get() returns MISSING for keys smaller than min, between keys, and greater than max."""
    mt = MemTable()
    mt.put(b"cat", b"1")
    mt.put(b"dog", b"2")
    mt.put(b"fox", b"3")

    sst = write_sstable(sstable_path, mt.items())

    # Before first key
    assert sst.get(b"ant") is MISSING
    assert b"ant" not in sst

    # Between existing keys
    assert sst.get(b"cow") is MISSING
    assert b"cow" not in sst

    # After last key
    assert sst.get(b"zebra") is MISSING
    assert b"zebra" not in sst


def test_deleted_key_tombstone(sstable_path: Path):
    """Verify tombstones are stored and get() distinguishes deleted from not in table."""
    mt = MemTable()
    mt.put(b"active", b"present")
    mt.delete(b"deleted")

    sst = write_sstable(sstable_path, mt.items())

    # Active key
    assert sst.get(b"active") == b"present"
    assert not sst.is_deleted(b"active")

    # Deleted key
    res_deleted = sst.get(b"deleted")
    assert res_deleted is None
    assert res_deleted is TOMBSTONE
    assert sst.is_deleted(b"deleted")
    assert b"deleted" in sst

    # Key not in table
    res_missing = sst.get(b"unwritten")
    assert res_missing is MISSING
    assert res_missing != res_deleted
    assert res_missing is not res_deleted
    assert b"unwritten" not in sst


def test_ordered_iteration(sstable_path: Path):
    """Verify items() yields all records in strictly sorted key order."""
    mt = MemTable()
    raw_data = [
        (b"apple", b"fruit"),
        (b"banana", None),  # Tombstone
        (b"carrot", b"vegetable"),
        (b"date", b"fruit"),
    ]
    for k, v in raw_data:
        mt.put(k, v)

    write_sstable(sstable_path, mt.items())
    sst = SSTable(sstable_path)

    records = list(sst.items())
    assert len(records) == 4
    assert records == [
        Record(b"apple", b"fruit"),
        Record(b"banana", None),
        Record(b"carrot", b"vegetable"),
        Record(b"date", b"fruit"),
    ]
    assert records[1].is_tombstone


def test_table_spanning_several_index_entries(sstable_path: Path):
    """Verify large tables span multiple sparse index blocks and get() scans only relevant block."""
    total_records = 60
    interval = 5  # 12 index blocks

    items = [
        Record(f"key_{i:04d}".encode("utf-8"), f"val_{i:04d}".encode("utf-8"))
        for i in range(total_records)
    ]

    sst = write_sstable(sstable_path, items, index_interval=interval)

    # Verify index size
    assert len(sst._index_keys) == total_records // interval

    # Query key in first block (key_0002)
    assert sst.get(b"key_0002") == b"val_0002"
    assert sst._last_scan_count <= interval

    # Query key in middle block (key_0028)
    assert sst.get(b"key_0028") == b"val_0028"
    assert sst._last_scan_count <= interval

    # Query key in last block (key_0059)
    assert sst.get(b"key_0059") == b"val_0059"
    assert sst._last_scan_count <= interval

    # Missing key between blocks (key_0024_missing)
    assert sst.get(b"key_0024_missing") is MISSING
    assert sst._last_scan_count <= interval

    # Full iteration yields all records in order
    read_items = list(sst.items())
    assert len(read_items) == total_records
    assert read_items == items


def test_bad_magic_number_rejected(sstable_path: Path):
    """Verify an SSTable file with a corrupted or invalid magic number is rejected with ValueError."""
    mt = MemTable()
    mt.put(b"k", b"v")
    write_sstable(sstable_path, mt.items())

    # Corrupt magic number in the last 4 bytes of footer
    data = bytearray(sstable_path.read_bytes())
    data[-4:] = b"BAD!"
    sstable_path.write_bytes(data)

    with pytest.raises(ValueError, match="Invalid SSTable magic number"):
        SSTable(sstable_path)


def test_file_too_short_rejected(tmp_path: Path):
    """Verify a file smaller than the fixed footer is rejected."""
    short_file = tmp_path / "short.sst"
    short_file.write_bytes(b"short")

    with pytest.raises(ValueError, match="smaller than footer"):
        SSTable(short_file)

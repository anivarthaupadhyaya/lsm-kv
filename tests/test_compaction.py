"""Tests for SSTable multi-way compaction."""

from pathlib import Path
import pytest

from lsm.compaction import compact
from lsm.memtable import MISSING, TOMBSTONE
from lsm.sstable import SSTable, write_sstable
from lsm.wal import Record


def _create_sst(path: Path, items: list[tuple[bytes, bytes | None]]) -> SSTable:
    """Helper to write an SSTable from key-value pairs."""
    return write_sstable(path, items)


def test_overwritten_keys_keep_newest_value(tmp_path: Path):
    """Verify that when duplicate keys exist across tables, the newest table wins."""
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    t3_path = tmp_path / "t3.sst"
    out_path = tmp_path / "compacted.sst"

    # t1 (oldest), t2 (middle), t3 (newest)
    t1 = _create_sst(t1_path, [(b"k1", b"v1_old"), (b"k2", b"v2_old"), (b"k3", b"v3_only")])
    t2 = _create_sst(t2_path, [(b"k1", b"v1_mid"), (b"k2", b"v2_new")])
    t3 = _create_sst(t3_path, [(b"k1", b"v1_newest")])

    res = compact([t1, t2, t3], out_path, drop_tombstones=False)
    assert res is None

    out = SSTable(out_path)
    assert out.get(b"k1") == b"v1_newest"
    assert out.get(b"k2") == b"v2_new"
    assert out.get(b"k3") == b"v3_only"

    records = list(out.items())
    assert records == [
        Record(b"k1", b"v1_newest"),
        Record(b"k2", b"v2_new"),
        Record(b"k3", b"v3_only"),
    ]


def test_deleted_key_disappears_when_drop_tombstones_true(tmp_path: Path):
    """Verify a key deleted in a newer table disappears completely when drop_tombstones=True."""
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    out_path = tmp_path / "compacted_drop.sst"

    # t1 (oldest): active k1, k2, k3
    t1 = _create_sst(t1_path, [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")])
    # t2 (newest): k2 is deleted, and k4 is deleted without prior existence
    t2 = _create_sst(t2_path, [(b"k2", None), (b"k4", None)])

    compact([t1, t2], out_path, drop_tombstones=True)

    out = SSTable(out_path)
    assert out.get(b"k1") == b"v1"
    assert out.get(b"k2") is MISSING
    assert b"k2" not in out
    assert not out.is_deleted(b"k2")

    assert out.get(b"k3") == b"v3"
    assert out.get(b"k4") is MISSING
    assert b"k4" not in out

    records = list(out.items())
    assert records == [
        Record(b"k1", b"v1"),
        Record(b"k3", b"v3"),
    ]


def test_deleted_key_stays_as_tombstone_when_drop_tombstones_false(tmp_path: Path):
    """Verify a deleted key remains stored as a tombstone when drop_tombstones=False."""
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    out_path = tmp_path / "compacted_keep.sst"

    t1 = _create_sst(t1_path, [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")])
    t2 = _create_sst(t2_path, [(b"k2", None)])

    compact([t1, t2], out_path, drop_tombstones=False)

    out = SSTable(out_path)
    assert out.get(b"k1") == b"v1"
    assert out.get(b"k2") is TOMBSTONE
    assert b"k2" in out
    assert out.is_deleted(b"k2")
    assert out.get(b"k3") == b"v3"

    records = list(out.items())
    assert records == [
        Record(b"k1", b"v1"),
        Record(b"k2", None),
        Record(b"k3", b"v3"),
    ]


def test_output_is_in_sorted_order(tmp_path: Path):
    """Verify output keys from multi-way compaction are in strictly ascending sorted order."""
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    t3_path = tmp_path / "t3.sst"
    out_path = tmp_path / "compacted_sorted.sst"

    # Staggered keys across tables
    t1 = _create_sst(t1_path, [(b"b", b"2"), (b"e", b"5"), (b"h", b"8")])
    t2 = _create_sst(t2_path, [(b"a", b"1"), (b"d", b"4"), (b"g", b"7")])
    t3 = _create_sst(t3_path, [(b"c", b"3"), (b"f", b"6"), (b"i", b"9")])

    compact([t1, t2, t3], out_path, drop_tombstones=True)

    out = SSTable(out_path)
    keys = [rec.key for rec in out.items()]
    assert keys == [b"a", b"b", b"c", b"d", b"e", b"f", b"g", b"h", b"i"]
    assert keys == sorted(keys)


def test_three_or_more_input_tables(tmp_path: Path):
    """Verify compaction correctly merges four tables with updates, deletions, and new keys."""
    t0_path = tmp_path / "t0.sst"
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    t3_path = tmp_path / "t3.sst"

    t0 = _create_sst(t0_path, [(b"k1", b"t0"), (b"k2", b"t0"), (b"k3", b"t0")])
    t1 = _create_sst(t1_path, [(b"k2", b"t1"), (b"k4", b"t1")])
    t2 = _create_sst(t2_path, [(b"k3", b"t2"), (b"k5", b"t2")])
    t3 = _create_sst(t3_path, [(b"k1", b"t3"), (b"k4", None)])  # Deletes k4

    out_drop = tmp_path / "out_drop.sst"
    compact([t0, t1, t2, t3], out_drop, drop_tombstones=True)
    out1 = SSTable(out_drop)
    assert out1.get(b"k1") == b"t3"
    assert out1.get(b"k2") == b"t1"
    assert out1.get(b"k3") == b"t2"
    assert out1.get(b"k4") is MISSING
    assert out1.get(b"k5") == b"t2"
    assert [r.key for r in out1.items()] == [b"k1", b"k2", b"k3", b"k5"]

    out_keep = tmp_path / "out_keep.sst"
    compact([t0, t1, t2, t3], out_keep, drop_tombstones=False)
    out2 = SSTable(out_keep)
    assert out2.get(b"k4") is TOMBSTONE
    assert [r.key for r in out2.items()] == [b"k1", b"k2", b"k3", b"k4", b"k5"]


def test_empty_input_table(tmp_path: Path):
    """Verify compaction handles empty input tables gracefully in all positions and alone."""
    empty_path = tmp_path / "empty.sst"
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"

    t_empty = _create_sst(empty_path, [])
    t1 = _create_sst(t1_path, [(b"alpha", b"1"), (b"gamma", b"3")])
    t2 = _create_sst(t2_path, [(b"beta", b"2"), (b"gamma", b"3_new")])

    # Empty table as first
    out1 = tmp_path / "out1.sst"
    compact([t_empty, t1, t2], out1, drop_tombstones=True)
    assert [r.key for r in SSTable(out1).items()] == [b"alpha", b"beta", b"gamma"]
    assert SSTable(out1).get(b"gamma") == b"3_new"

    # Empty table in middle
    out2 = tmp_path / "out2.sst"
    compact([t1, t_empty, t2], out2, drop_tombstones=True)
    assert [r.key for r in SSTable(out2).items()] == [b"alpha", b"beta", b"gamma"]

    # Empty table as last
    out3 = tmp_path / "out3.sst"
    compact([t1, t2, t_empty], out3, drop_tombstones=True)
    assert [r.key for r in SSTable(out3).items()] == [b"alpha", b"beta", b"gamma"]

    # Only an empty table
    out_alone = tmp_path / "out_alone.sst"
    compact([t_empty], out_alone, drop_tombstones=True)
    alone_sst = SSTable(out_alone)
    assert list(alone_sst.items()) == []
    assert alone_sst.get(b"alpha") is MISSING


def test_input_files_not_touched_or_deleted(tmp_path: Path):
    """Verify input SSTable files are never modified or deleted during compaction."""
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    out_path = tmp_path / "out.sst"

    t1 = _create_sst(t1_path, [(b"k1", b"v1")])
    t2 = _create_sst(t2_path, [(b"k2", b"v2")])

    t1_bytes = t1_path.read_bytes()
    t2_bytes = t2_path.read_bytes()

    compact([t1, t2], out_path, drop_tombstones=False)

    assert t1_path.exists()
    assert t2_path.exists()
    assert t1_path.read_bytes() == t1_bytes
    assert t2_path.read_bytes() == t2_bytes


def test_resurrected_key_overwrites_tombstone(tmp_path: Path):
    """Verify a key that was deleted in an older table and rewritten in a newer table is active."""
    t1_path = tmp_path / "t1.sst"
    t2_path = tmp_path / "t2.sst"
    t3_path = tmp_path / "t3.sst"
    out_path = tmp_path / "resurrected.sst"

    t1 = _create_sst(t1_path, [(b"key", b"v1")])
    t2 = _create_sst(t2_path, [(b"key", None)])  # Deleted
    t3 = _create_sst(t3_path, [(b"key", b"v3_resurrected")])  # Re-inserted

    compact([t1, t2, t3], out_path, drop_tombstones=True)
    out = SSTable(out_path)
    assert out.get(b"key") == b"v3_resurrected"
    assert not out.is_deleted(b"key")


def test_output_path_cannot_be_input_table(tmp_path: Path):
    """Verify passing an input table path as out_path raises ValueError to protect inputs."""
    t1_path = tmp_path / "t1.sst"
    t1 = _create_sst(t1_path, [(b"k", b"v")])

    with pytest.raises(ValueError, match="Output path cannot match input SSTable path"):
        compact([t1], t1_path)

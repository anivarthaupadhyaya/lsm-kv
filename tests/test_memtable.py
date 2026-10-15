import pytest
from lsm.memtable import MemTable, MISSING, TOMBSTONE
from lsm.wal import Record


def test_sorted_iteration():
    """Verify items() yields entries in lexicographical sorted order including tombstones."""
    mt = MemTable()
    # Insert in unsorted order
    entries = [
        (b"pear", b"40"),
        (b"apple", b"10"),
        (b"orange", b"30"),
        (b"banana", b"20"),
    ]
    for k, v in entries:
        mt.put(k, v)

    # Delete banana -> tombstone
    mt.delete(b"banana")

    records = list(mt.items())
    keys = [r.key for r in records]
    assert keys == [b"apple", b"banana", b"orange", b"pear"]

    # Verify banana is yielded as a tombstone record
    assert records[1] == Record(key=b"banana", value=None)
    assert records[1].is_tombstone


def test_overwrite():
    """Verify that overwriting a key updates the value and does not duplicate keys."""
    mt = MemTable()
    mt.put(b"user:1", b"alice")
    assert mt.get(b"user:1") == b"alice"
    assert len(mt) == 1

    mt.put(b"user:1", b"bob")
    assert mt.get(b"user:1") == b"bob"
    assert len(mt) == 1

    items = list(mt.items())
    assert len(items) == 1
    assert items[0] == Record(key=b"user:1", value=b"bob")


def test_delete():
    """Verify delete stores a tombstone (value=None)."""
    mt = MemTable()
    mt.put("foo", "bar")
    assert mt.get("foo") == b"bar"

    mt.delete("foo")
    assert mt.get("foo") is None
    assert mt.get("foo") is TOMBSTONE
    assert mt.is_deleted("foo")

    # The key is still present in the table as a tombstone
    assert "foo" in mt
    assert len(mt) == 1

    rec = next(mt.items())
    assert rec.key == b"foo"
    assert rec.value is None
    assert rec.is_tombstone


def test_missing_vs_deleted():
    """Verify get() distinguishes between a missing key and a deleted key."""
    mt = MemTable()
    mt.put("exists", "val")
    mt.put("deleted", "val")
    mt.delete("deleted")

    res_exists = mt.get("exists")
    res_deleted = mt.get("deleted")
    res_missing = mt.get("never_written")

    # Present key returns byte value
    assert res_exists == b"val"

    # Deleted key returns None (tombstone representation)
    assert res_deleted is None
    assert res_deleted is TOMBSTONE

    # Missing key returns MISSING sentinel
    assert res_missing is MISSING
    assert not bool(res_missing)

    # Distinct results
    assert res_deleted != res_missing
    assert res_deleted is not res_missing
    assert res_exists != res_missing

    # Membership check differentiates them
    assert "deleted" in mt
    assert "never_written" not in mt


def test_size_grows_with_puts():
    """Verify size_bytes tracks memory additions and overwrites accurately."""
    mt = MemTable()
    assert mt.size_bytes() == 0

    k1, v1 = b"key1", b"val1"  # 4 + 4 = 8 bytes
    mt.put(k1, v1)
    size1 = mt.size_bytes()
    assert size1 == 8

    k2, v2 = b"key2", b"longer_value"  # 4 + 12 = 16 bytes
    mt.put(k2, v2)
    size2 = mt.size_bytes()
    assert size2 > size1
    assert size2 == 8 + 16

    # Overwrite k1 with longer value -> size grows
    v1_longer = b"val1_extended"  # 13 bytes (+9)
    mt.put(k1, v1_longer)
    size3 = mt.size_bytes()
    assert size3 > size2
    assert size3 == size2 + (len(v1_longer) - len(v1))

    # Overwrite k1 with shorter value -> size shrinks
    mt.put(k1, b"v")  # 1 byte (-12)
    size4 = mt.size_bytes()
    assert size4 < size3

    # Delete k2: value freed, tombstone key remains
    mt.delete(k2)
    size5 = mt.size_bytes()
    assert size5 == size4 - len(v2)


def test_delete_before_put():
    """Deleting a key never written inserts a tombstone."""
    mt = MemTable()
    mt.delete("ghost")
    assert "ghost" in mt
    assert mt.get("ghost") is None
    assert mt.size_bytes() == len(b"ghost")


def test_empty_value_is_not_tombstone():
    """Verify empty byte value b'' is not treated as tombstone None."""
    mt = MemTable()
    mt.put("empty", b"")
    val = mt.get("empty")
    assert val == b""
    assert val is not None
    assert not mt.is_deleted("empty")

    rec = next(mt.items())
    assert rec.value == b""
    assert not rec.is_tombstone


def test_clear():
    """Verify clear resets entries and size."""
    mt = MemTable()
    mt.put("k1", "v1")
    mt.put("k2", "v2")
    assert len(mt) == 2
    assert mt.size_bytes() > 0

    mt.clear()
    assert len(mt) == 0
    assert mt.size_bytes() == 0
    assert list(mt.items()) == []
    assert mt.get("k1") is MISSING

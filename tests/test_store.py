"""Tests for LSMStore (coordinating WAL, MemTable, and SSTables)."""

from pathlib import Path
import pytest

from lsm.store import LSMStore, Store


def test_persistence_across_close_and_reopen(tmp_path: Path):
    """Verify that data written to the store survives close and reopen."""
    with LSMStore(tmp_path) as store:
        store.put("key1", "val1")
        store.put(b"key2", b"val2")
        store.put("key3", "val3")

    # Reopen fresh store instance from same directory
    with LSMStore(tmp_path) as store2:
        assert store2.get("key1") == b"val1"
        assert store2.get(b"key2") == b"val2"
        assert store2.get("key3") == b"val3"
        assert store2.get("nonexistent") is None
        assert "key1" in store2
        assert store2["key1"] == b"val1"


def test_overwrite_across_flush(tmp_path: Path):
    """Verify overwriting keys works both before and after flushing to SSTables."""
    store = LSMStore(tmp_path)
    store.put("user:1", "alice_v1")
    store.flush()  # Flushed to sst_000001.sst
    assert len(store.sstables) == 1
    assert store.get("user:1") == b"alice_v1"

    # Overwrite in MemTable while old value is in SSTable
    store.put("user:1", "alice_v2")
    assert store.get("user:1") == b"alice_v2"

    # Flush again: new value in sst_000002.sst
    store.flush()
    assert len(store.sstables) == 2
    assert store.get("user:1") == b"alice_v2"
    store.close()

    # Reopen to verify newest SSTable wins over older SSTable on disk
    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 2
    assert store2.get("user:1") == b"alice_v2"
    store2.close()


def test_delete_across_flush(tmp_path: Path):
    """Verify delete tombstones hide older values across flushes and reopens."""
    store = LSMStore(tmp_path)
    store.put("item:1", "data1")
    store.put("item:2", "data2")
    store.flush()  # Persisted to sst_000001.sst

    # Delete item:1 (tombstone in memtable)
    store.delete("item:1")
    assert store.get("item:1") is None
    assert "item:1" not in store
    assert store.get("item:2") == b"data2"

    # Flush tombstone to sst_000002.sst
    store.flush()
    assert len(store.sstables) == 2
    assert store.get("item:1") is None
    assert "item:1" not in store
    assert store.get("item:2") == b"data2"
    store.close()

    # Reopen: tombstone in newest SSTable must stop search and hide item:1
    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 2
    assert store2.get("item:1") is None
    assert store2.get("item:2") == b"data2"
    store2.close()


def test_flush_triggered_by_small_size_limit(tmp_path: Path):
    """Verify exceeding memtable limit triggers automatic SSTable flush."""
    # Small limit of 50 bytes
    store = LSMStore(tmp_path, memtable_size_limit=50)
    assert len(store.sstables) == 0

    # Key + value = 2 + 15 = 17 bytes
    store.put("k1", "x" * 15)
    assert len(store.sstables) == 0

    # Key + value = 2 + 15 = 17 bytes (cumulative ~ 34 bytes)
    store.put("k2", "x" * 15)
    assert len(store.sstables) == 0

    # Key + value = 2 + 20 = 22 bytes (cumulative ~ 56 bytes >= 50 limit)
    store.put("k3", "x" * 20)
    assert len(store.sstables) == 1
    assert store.sstables[0].path.name == "sst_000001.sst"

    # All keys should still be retrievable
    assert store.get("k1") == b"x" * 15
    assert store.get("k2") == b"x" * 15
    assert store.get("k3") == b"x" * 20

    # Trigger a second flush
    store.put("k4", "y" * 60)
    assert len(store.sstables) == 2
    assert store.sstables[0].path.name == "sst_000002.sst"
    assert store.get("k4") == b"y" * 60
    store.close()


def test_reopen_with_data_in_wal_and_sstables(tmp_path: Path):
    """Verify recovery when data exists in both flushed SSTables and unflushed WAL."""
    store = LSMStore(tmp_path)
    store.put("sst_key", "value_in_sstable")
    store.put("overwrite_me", "old_val")
    store.flush()  # Flushed to SSTable

    # Write more keys to WAL & MemTable without flushing
    store.put("wal_key", "value_in_wal")
    store.put("overwrite_me", "new_val_in_wal")
    store.close()

    # Reopen: should load 1 SSTable and replay WAL into fresh MemTable
    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 1
    # MemTable must contain replayed records from WAL
    assert store2.memtable.get("wal_key") == b"value_in_wal"
    assert store2.get("sst_key") == b"value_in_sstable"
    assert store2.get("wal_key") == b"value_in_wal"
    assert store2.get("overwrite_me") == b"new_val_in_wal"

    # Sequence numbering should continue properly
    store2.flush()
    assert len(store2.sstables) == 2
    assert store2.sstables[0].path.name == "sst_000002.sst"
    store2.close()


def test_leftover_tmp_file_ignored(tmp_path: Path):
    """Verify leftover .tmp files from aborted flushes are deleted and ignored."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    leftover1 = tmp_path / "sst_999999.tmp"
    leftover2 = tmp_path / "corrupt_partial.tmp"
    leftover1.write_bytes(b"garbage temporary data")
    leftover2.write_bytes(b"incomplete write")

    assert leftover1.exists()
    assert leftover2.exists()

    # Initialize store: startup should clean up .tmp files and ignore them
    store = LSMStore(tmp_path)
    assert not leftover1.exists()
    assert not leftover2.exists()
    assert len(store.sstables) == 0

    # Sequence numbering should start from 1, unaffected by sst_999999.tmp
    store.put("foo", "bar")
    store.flush()
    assert len(store.sstables) == 1
    assert store.sstables[0].path.name == "sst_000001.sst"
    assert store.get("foo") == b"bar"
    store.close()


def test_store_alias_and_closed_errors(tmp_path: Path):
    """Verify Store alias, closed error guards, and custom default."""
    store = Store(tmp_path, 1024)
    store.put("alpha", "1")
    assert store.get("alpha") == b"1"
    assert store.get("beta", default=b"def") == b"def"

    store.close()
    assert store.is_closed
    with pytest.raises(ValueError, match="closed"):
        store.put("alpha", "2")
    with pytest.raises(ValueError, match="closed"):
        store.get("alpha")
    with pytest.raises(ValueError, match="closed"):
        store.delete("alpha")
    with pytest.raises(ValueError, match="closed"):
        store.flush()

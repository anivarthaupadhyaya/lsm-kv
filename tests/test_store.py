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
    store.flush()
    assert len(store.sstables) == 1
    assert store.get("user:1") == b"alice_v1"

    store.put("user:1", "alice_v2")
    assert store.get("user:1") == b"alice_v2"
    store.flush()
    assert len(store.sstables) == 2
    assert store.get("user:1") == b"alice_v2"
    store.close()

    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 2
    assert store2.get("user:1") == b"alice_v2"
    store2.close()


def test_delete_across_flush(tmp_path: Path):
    """Verify delete tombstones hide older values across flushes and reopens."""
    store = LSMStore(tmp_path)
    store.put("item:1", "data1")
    store.put("item:2", "data2")
    store.flush()

    store.delete("item:1")
    assert store.get("item:1") is None
    assert "item:1" not in store
    assert store.get("item:2") == b"data2"

    store.flush()
    assert len(store.sstables) == 2
    assert store.get("item:1") is None
    assert "item:1" not in store
    assert store.get("item:2") == b"data2"
    store.close()

    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 2
    assert store2.get("item:1") is None
    assert store2.get("item:2") == b"data2"
    store2.close()


def test_flush_triggered_by_small_size_limit(tmp_path: Path):
    """Verify exceeding memtable limit triggers automatic SSTable flush."""
    store = LSMStore(tmp_path, memtable_size_limit=50)
    assert len(store.sstables) == 0

    store.put("k1", "x" * 15)
    assert len(store.sstables) == 0
    store.put("k2", "x" * 15)
    assert len(store.sstables) == 0

    store.put("k3", "x" * 20)
    assert len(store.sstables) == 1
    assert store.sstables[0].path.name == "sst_000001.sst"
    assert store.get("k1") == b"x" * 15
    assert store.get("k2") == b"x" * 15
    assert store.get("k3") == b"x" * 20

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
    store.flush()

    store.put("wal_key", "value_in_wal")
    store.put("overwrite_me", "new_val_in_wal")
    store.close()

    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 1
    assert store2.memtable.get("wal_key") == b"value_in_wal"
    assert store2.get("sst_key") == b"value_in_sstable"
    assert store2.get("wal_key") == b"value_in_wal"
    assert store2.get("overwrite_me") == b"new_val_in_wal"

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

    store = LSMStore(tmp_path)
    assert not leftover1.exists()
    assert not leftover2.exists()
    assert len(store.sstables) == 0

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


def test_compaction_triggered_by_threshold_and_reads_identical(tmp_path: Path):
    """Compaction triggers when SSTables exceed threshold, dropping table count and keeping reads identical."""
    store = LSMStore(tmp_path, compaction_threshold=4)
    store.put("k1", "v1_orig"); store.put("common", "c1"); store.flush()
    store.put("k2", "v2"); store.put("common", "c2"); store.flush()
    store.put("k3", "v3"); store.flush()
    store.put("k4", "v4"); store.put("common", "c3"); store.flush()
    assert len(store.sstables) == 4

    expected = {"k1": b"v1_orig", "k2": b"v2", "k3": b"v3", "k4": b"v4", "k5": b"v5", "common": b"c4"}
    store.put("k5", "v5"); store.put("common", "c4"); store.flush()

    assert len(store.sstables) == 1
    assert store.sstables[0].path.name == "sst_000006.sst"
    for k, v in expected.items():
        assert store.get(k) == v
    assert store.get("missing") is None
    store.close()


def test_compaction_deleted_keys_stay_deleted(tmp_path: Path):
    """Verify deleted keys stay deleted and tombstones are purged during compaction."""
    store = LSMStore(tmp_path, compaction_threshold=4)
    store.put("d1", "val1"); store.put("survivor", "alive"); store.flush()
    store.put("d2", "val2"); store.flush()
    store.delete("d1"); store.flush()
    store.delete("d2"); store.put("d3", "val3"); store.flush()
    assert len(store.sstables) == 4

    store.delete("d3"); store.put("survivor2", "alive2"); store.flush()
    assert len(store.sstables) == 1

    assert store.get("d1") is None
    assert store.get("d2") is None
    assert store.get("d3") is None
    assert "d1" not in store
    assert "d2" not in store
    assert "d3" not in store
    assert store.get("survivor") == b"alive"
    assert store.get("survivor2") == b"alive2"

    records = list(store.sstables[0].items())
    keys = [r.key for r in records]
    assert b"d1" not in keys
    assert b"d2" not in keys
    assert b"d3" not in keys
    assert all(not r.is_tombstone for r in records)
    store.close()


def test_compaction_crash_recovery_step_2(tmp_path: Path):
    """Crash after step 2 (marker written, output not renamed): recovery deletes marker/.tmp, inputs valid."""
    store = LSMStore(tmp_path, compaction_threshold=4)
    for i in range(4):
        store.put(f"k{i}", f"v{i}"); store.flush()
    store.put("k4", "v4")

    store.crash_step = 2
    with pytest.raises(RuntimeError, match="step 2"):
        store.flush()

    assert (tmp_path / "compaction.marker").exists()
    assert not (tmp_path / "sst_000006.sst").exists()
    assert (tmp_path / "sst_000006.tmp").exists()
    store.close()

    store2 = LSMStore(tmp_path)
    assert not (tmp_path / "compaction.marker").exists()
    assert not (tmp_path / "sst_000006.tmp").exists()
    assert len(store2.sstables) == 5
    for i in range(5):
        assert store2.get(f"k{i}") == f"v{i}".encode()
    store2.close()


def test_compaction_crash_recovery_step_3(tmp_path: Path):
    """Crash after step 3 (output renamed, inputs not deleted): recovery commits and deletes inputs."""
    store = LSMStore(tmp_path, compaction_threshold=4)
    for i in range(4):
        store.put(f"k{i}", f"v{i}"); store.flush()
    store.put("k4", "v4")

    store.crash_step = 3
    with pytest.raises(RuntimeError, match="step 3"):
        store.flush()

    assert (tmp_path / "compaction.marker").exists()
    assert (tmp_path / "sst_000006.sst").exists()
    assert (tmp_path / "sst_000001.sst").exists()
    store.close()

    store2 = LSMStore(tmp_path)
    assert not (tmp_path / "compaction.marker").exists()
    assert not (tmp_path / "sst_000001.sst").exists()
    assert len(store2.sstables) == 1
    assert store2.sstables[0].path.name == "sst_000006.sst"
    for i in range(5):
        assert store2.get(f"k{i}") == f"v{i}".encode()
    store2.close()


def test_compaction_crash_recovery_step_4(tmp_path: Path):
    """Crash after step 4 (inputs deleted, marker still exists): recovery cleans up marker."""
    store = LSMStore(tmp_path, compaction_threshold=4)
    for i in range(4):
        store.put(f"k{i}", f"v{i}"); store.flush()
    store.put("k4", "v4")

    store.crash_step = 4
    with pytest.raises(RuntimeError, match="step 4"):
        store.flush()

    assert (tmp_path / "compaction.marker").exists()
    assert (tmp_path / "sst_000006.sst").exists()
    assert not (tmp_path / "sst_000001.sst").exists()
    store.close()

    store2 = LSMStore(tmp_path)
    assert not (tmp_path / "compaction.marker").exists()
    assert len(store2.sstables) == 1
    for i in range(5):
        assert store2.get(f"k{i}") == f"v{i}".encode()
    store2.close()

"""Tests for LSMStore (coordinating WAL, MemTable, SSTables, and BloomFilter)."""

from pathlib import Path
import pytest

from lsm.store import LSMStore, Store


def test_persistence_across_close_and_reopen(tmp_path: Path):
    with LSMStore(tmp_path) as store:
        store.put("key1", "val1"); store.put(b"key2", b"val2"); store.put("key3", "val3")
    with LSMStore(tmp_path) as store2:
        assert store2.get("key1") == b"val1"
        assert store2.get(b"key2") == b"val2"
        assert store2.get("key3") == b"val3"
        assert store2.get("nonexistent") is None
        assert "key1" in store2 and store2["key1"] == b"val1"


def test_overwrite_across_flush(tmp_path: Path):
    store = LSMStore(tmp_path)
    store.put("user:1", "alice_v1"); store.flush()
    assert len(store.sstables) == 1 and store.get("user:1") == b"alice_v1"
    store.put("user:1", "alice_v2")
    assert store.get("user:1") == b"alice_v2"
    store.flush()
    assert len(store.sstables) == 2 and store.get("user:1") == b"alice_v2"
    store.close()
    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 2 and store2.get("user:1") == b"alice_v2"
    store2.close()


def test_delete_across_flush(tmp_path: Path):
    store = LSMStore(tmp_path)
    store.put("item:1", "data1"); store.put("item:2", "data2"); store.flush()
    store.delete("item:1")
    assert store.get("item:1") is None and "item:1" not in store and store.get("item:2") == b"data2"
    store.flush()
    assert len(store.sstables) == 2 and store.get("item:1") is None and store.get("item:2") == b"data2"
    store.close()
    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 2 and store2.get("item:1") is None and store2.get("item:2") == b"data2"
    store2.close()


def test_flush_triggered_by_small_size_limit(tmp_path: Path):
    store = LSMStore(tmp_path, memtable_size_limit=50)
    assert len(store.sstables) == 0
    store.put("k1", "x" * 15); store.put("k2", "x" * 15)
    assert len(store.sstables) == 0
    store.put("k3", "x" * 20)
    assert len(store.sstables) == 1 and store.sstables[0].path.name == "sst_000001.sst"
    assert store.get("k1") == b"x" * 15 and store.get("k2") == b"x" * 15 and store.get("k3") == b"x" * 20
    store.put("k4", "y" * 60)
    assert len(store.sstables) == 2 and store.sstables[0].path.name == "sst_000002.sst"
    assert store.get("k4") == b"y" * 60
    store.close()


def test_reopen_with_data_in_wal_and_sstables(tmp_path: Path):
    store = LSMStore(tmp_path)
    store.put("sst_key", "value_in_sstable"); store.put("overwrite_me", "old_val"); store.flush()
    store.put("wal_key", "value_in_wal"); store.put("overwrite_me", "new_val_in_wal"); store.close()
    store2 = LSMStore(tmp_path)
    assert len(store2.sstables) == 1 and store2.memtable.get("wal_key") == b"value_in_wal"
    assert store2.get("sst_key") == b"value_in_sstable"
    assert store2.get("wal_key") == b"value_in_wal" and store2.get("overwrite_me") == b"new_val_in_wal"
    store2.flush()
    assert len(store2.sstables) == 2 and store2.sstables[0].path.name == "sst_000002.sst"
    store2.close()


def test_leftover_tmp_file_ignored(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "sst_999999.tmp").write_bytes(b"garbage temporary data")
    (tmp_path / "corrupt_partial.tmp").write_bytes(b"incomplete write")
    store = LSMStore(tmp_path)
    assert not (tmp_path / "sst_999999.tmp").exists() and len(store.sstables) == 0
    store.put("foo", "bar"); store.flush()
    assert len(store.sstables) == 1 and store.get("foo") == b"bar"
    store.close()


def test_store_alias_and_closed_errors(tmp_path: Path):
    store = Store(tmp_path, 1024)
    store.put("alpha", "1")
    assert store.get("alpha") == b"1" and store.get("beta", default=b"def") == b"def"
    store.close()
    assert store.is_closed
    with pytest.raises(ValueError, match="closed"): store.put("alpha", "2")
    with pytest.raises(ValueError, match="closed"): store.get("alpha")
    with pytest.raises(ValueError, match="closed"): store.delete("alpha")
    with pytest.raises(ValueError, match="closed"): store.flush()


def test_compaction_triggered_by_threshold_and_reads_identical(tmp_path: Path):
    store = LSMStore(tmp_path, compaction_threshold=4)
    store.put("k1", "v1_orig"); store.put("common", "c1"); store.flush()
    store.put("k2", "v2"); store.put("common", "c2"); store.flush()
    store.put("k3", "v3"); store.flush()
    store.put("k4", "v4"); store.put("common", "c3"); store.flush()
    assert len(store.sstables) == 4
    expected = {"k1": b"v1_orig", "k2": b"v2", "k3": b"v3", "k4": b"v4", "k5": b"v5", "common": b"c4"}
    store.put("k5", "v5"); store.put("common", "c4"); store.flush()
    assert len(store.sstables) == 1 and store.sstables[0].path.name == "sst_000006.sst"
    for k, v in expected.items(): assert store.get(k) == v
    assert store.get("missing") is None
    store.close()


def test_compaction_deleted_keys_stay_deleted(tmp_path: Path):
    store = LSMStore(tmp_path, compaction_threshold=4)
    store.put("d1", "val1"); store.put("survivor", "alive"); store.flush()
    store.put("d2", "val2"); store.flush()
    store.delete("d1"); store.flush()
    store.delete("d2"); store.put("d3", "val3"); store.flush()
    assert len(store.sstables) == 4
    store.delete("d3"); store.put("survivor2", "alive2"); store.flush()
    assert len(store.sstables) == 1
    for k in ("d1", "d2", "d3"):
        assert store.get(k) is None and k not in store
    assert store.get("survivor") == b"alive" and store.get("survivor2") == b"alive2"
    records = list(store.sstables[0].items())
    assert all(not r.is_tombstone for r in records) and b"d1" not in [r.key for r in records]
    store.close()


def test_compaction_crash_recovery_step_2(tmp_path: Path):
    store = LSMStore(tmp_path, compaction_threshold=4)
    for i in range(4): store.put(f"k{i}", f"v{i}"); store.flush()
    store.put("k4", "v4")
    store.crash_step = 2
    with pytest.raises(RuntimeError, match="step 2"): store.flush()
    assert (tmp_path / "compaction.marker").exists()
    assert not (tmp_path / "sst_000006.sst").exists() and (tmp_path / "sst_000006.tmp").exists()
    store.close()
    store2 = LSMStore(tmp_path)
    assert not (tmp_path / "compaction.marker").exists() and not (tmp_path / "sst_000006.tmp").exists()
    assert len(store2.sstables) == 5
    for i in range(5): assert store2.get(f"k{i}") == f"v{i}".encode()
    store2.close()


def test_compaction_crash_recovery_step_3(tmp_path: Path):
    store = LSMStore(tmp_path, compaction_threshold=4)
    for i in range(4): store.put(f"k{i}", f"v{i}"); store.flush()
    store.put("k4", "v4")
    store.crash_step = 3
    with pytest.raises(RuntimeError, match="step 3"): store.flush()
    assert (tmp_path / "compaction.marker").exists()
    assert (tmp_path / "sst_000006.sst").exists() and (tmp_path / "sst_000001.sst").exists()
    store.close()
    store2 = LSMStore(tmp_path)
    assert not (tmp_path / "compaction.marker").exists() and not (tmp_path / "sst_000001.sst").exists()
    assert len(store2.sstables) == 1 and store2.sstables[0].path.name == "sst_000006.sst"
    for i in range(5): assert store2.get(f"k{i}") == f"v{i}".encode()
    store2.close()


def test_compaction_crash_recovery_step_4(tmp_path: Path):
    store = LSMStore(tmp_path, compaction_threshold=4)
    for i in range(4): store.put(f"k{i}", f"v{i}"); store.flush()
    store.put("k4", "v4")
    store.crash_step = 4
    with pytest.raises(RuntimeError, match="step 4"): store.flush()
    assert (tmp_path / "compaction.marker").exists()
    assert (tmp_path / "sst_000006.sst").exists() and not (tmp_path / "sst_000001.sst").exists()
    store.close()
    store2 = LSMStore(tmp_path)
    assert not (tmp_path / "compaction.marker").exists() and len(store2.sstables) == 1
    for i in range(5): assert store2.get(f"k{i}") == f"v{i}".encode()
    store2.close()


def test_results_identical_with_and_without_bloom(tmp_path: Path):
    """Verify get and membership results are identical with and without Bloom filter."""
    dir_b, dir_nb = tmp_path / "bloom", tmp_path / "nobloom"
    keys = [f"k{i}".encode() for i in range(15)] + [b"absent1", b"absent2", b"del_key"]
    with LSMStore(dir_b, use_bloom=True) as s1, LSMStore(dir_nb, use_bloom=False) as s2:
        for i in range(12):
            s1.put(f"k{i}", f"val{i}"); s2.put(f"k{i}", f"val{i}")
            if i % 4 == 0: s1.flush(); s2.flush()
        s1.put("del_key", "tomb"); s2.put("del_key", "tomb")
        s1.flush(); s2.flush()
        s1.delete("del_key"); s2.delete("del_key")
        s1.flush(); s2.flush()
        for k in keys:
            assert s1.get(k) == s2.get(k)
            assert (k in s1) == (k in s2)
        assert s1.bloom_skips > 0 and s2.bloom_skips == 0


def test_lookups_of_absent_keys_increase_bloom_skips(tmp_path: Path):
    """Verify queries for absent keys skip SSTables via bloom filter and increment bloom_skips."""
    with LSMStore(tmp_path, use_bloom=True) as store:
        for i in range(3):
            store.put(f"batch_{i}_k1", "v1"); store.put(f"batch_{i}_k2", "v2"); store.flush()
        assert len(store.sstables) == 3 and store.bloom_skips == 0
        assert store.get("completely_absent_key_1") is None
        assert store.bloom_skips == 3
        assert store.get("completely_absent_key_2") is None
        assert store.bloom_skips == 6
        assert store.get("batch_2_k1") == b"v1"
        assert store.bloom_skips == 6


def test_deleted_key_still_hidden_after_flush_and_compaction(tmp_path: Path):
    """Verify deleted keys are hidden across flushes and after compaction with bloom filter."""
    store = LSMStore(tmp_path, use_bloom=True, compaction_threshold=10)
    store.put("target_key", "old_value"); store.put("survivor", "alive"); store.flush()
    assert store.get("target_key") == b"old_value"
    store.delete("target_key"); store.flush()
    assert len(store.sstables) == 2
    assert store.get("target_key") is None and "target_key" not in store
    store.compact()
    assert len(store.sstables) == 1
    assert store.get("target_key") is None and "target_key" not in store
    assert store.get("survivor") == b"alive"
    store.close()
    with LSMStore(tmp_path, use_bloom=True) as store2:
        assert store2.get("target_key") is None and store2.get("survivor") == b"alive"


def test_missing_bloom_file_falls_back_correctly(tmp_path: Path):
    """Verify store rebuilds filter in memory when .bloom sidecar is missing."""
    store = LSMStore(tmp_path)
    store.put("k1", "v1"); store.put("k2", "v2"); store.flush()
    bp = store.sstables[0].path.with_suffix(".bloom")
    assert bp.exists()
    store.close()
    bp.unlink()
    assert not bp.exists()
    store2 = LSMStore(tmp_path)
    assert store2.get("k1") == b"v1" and store2.get("k2") == b"v2"
    assert store2.get("absent") is None
    assert store2.bloom_skips == 1
    store2.close()


def test_corrupt_bloom_file_falls_back_correctly(tmp_path: Path):
    """Verify store falls back to in-memory filter rebuild when .bloom file is corrupt."""
    store = LSMStore(tmp_path)
    store.put("user_a", "data_a"); store.put("user_b", "data_b"); store.flush()
    bp = store.sstables[0].path.with_suffix(".bloom")
    assert bp.exists()
    store.close()
    bp.write_bytes(b"BAD_CORRUPTED_BLOOM_DATA_HEADER")
    store2 = LSMStore(tmp_path)
    assert store2.get("user_a") == b"data_a" and store2.get("user_b") == b"data_b"
    assert store2.get("user_c") is None
    assert store2.bloom_skips == 1
    store2.close()


def test_compaction_and_recovery_cleans_bloom_files(tmp_path: Path):
    """Verify compaction creates new .bloom, removes old .bloom, and recovery cleans up."""
    store = LSMStore(tmp_path, compaction_threshold=3)
    for i in range(3): store.put(f"key{i}", f"val{i}"); store.flush()
    old_blooms = [sst.path.with_suffix(".bloom") for sst in store.sstables]
    assert all(b.exists() for b in old_blooms)
    store.put("key3", "val3"); store.flush()
    assert len(store.sstables) == 1
    assert store.sstables[0].path.with_suffix(".bloom").exists()
    assert all(not b.exists() for b in old_blooms)
    store.close()

    store2 = LSMStore(tmp_path, compaction_threshold=2)
    store2.put("x1", "y1"); store2.flush()
    store2.put("x2", "y2")
    store2.crash_step = 3
    with pytest.raises(RuntimeError, match="step 3"): store2.flush()
    store2.close()

    store3 = LSMStore(tmp_path)
    assert len(store3.sstables) == 1
    assert store3.sstables[0].path.with_suffix(".bloom").exists()
    assert store3.get("x1") == b"y1"
    store3.close()

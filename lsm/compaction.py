"""Compaction module for multi-way merging of SSTables in an LSM-Tree."""

import heapq
from pathlib import Path
from typing import Iterator, Sequence, Union

from lsm.sstable import SSTable, write_sstable
from lsm.wal import Record


def compact(
    tables: Sequence[SSTable],
    out_path: Union[str, Path],
    drop_tombstones: bool = False,
) -> None:
    """Merges multiple SSTables into a single compacted SSTable using k-way merge.

    Parameters:
        tables: List/sequence of SSTable instances ordered from oldest to newest.
        out_path: Destination file path for the compacted SSTable.
        drop_tombstones: If True, tombstones are omitted from the output.
            If False, tombstones are retained in the output.

    Notes:
        - For duplicate keys across tables, the newest table's entry wins.
        - The input files are never touched or deleted.
        - Returns nothing (None).
    """
    out_resolved = Path(out_path).resolve()
    for t in tables:
        if Path(t.path).resolve() == out_resolved:
            raise ValueError(f"Output path cannot match input SSTable path: {out_path}")

    def _merged_records() -> Iterator[Record]:
        # Heap elements: (record_key, table_index, record, iterator)
        # table_index ensures stable unique ordering so Record/Iterator are never compared.
        heap: list[tuple[bytes, int, Record, Iterator[Record]]] = []

        for idx, table in enumerate(tables):
            it = table.items()
            try:
                first_rec = next(it)
                heapq.heappush(heap, (first_rec.key, idx, first_rec, it))
            except StopIteration:
                continue

        while heap:
            curr_key = heap[0][0]
            winning_rec: Record = heap[0][2]
            max_idx = -1

            # Drain all records from all tables sharing curr_key.
            # Table indices are 0 (oldest) to len(tables)-1 (newest); highest idx wins.
            while heap and heap[0][0] == curr_key:
                _, idx, rec, it = heapq.heappop(heap)
                if idx >= max_idx:
                    max_idx = idx
                    winning_rec = rec

                try:
                    next_rec = next(it)
                    heapq.heappush(heap, (next_rec.key, idx, next_rec, it))
                except StopIteration:
                    pass

            if drop_tombstones and winning_rec.is_tombstone:
                continue

            yield winning_rec

    write_sstable(out_path, _merged_records())

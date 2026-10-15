"""Crash test writer subprocess for LSMStore.

Loops forever performing a random mix of puts, overwrites, and deletes
over a small key space. After each operation returns, prints and flushes
the operation to stdout (SET key value or DEL key).
"""

import os
import random
import sys
from pathlib import Path

# Add project root to sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lsm.store import LSMStore

KEY_SPACE = [f"k{i}" for i in range(6)]


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python crash_writer.py <data_dir>", file=sys.stderr)
        sys.exit(1)

    data_dir = sys.argv[1]
    store = LSMStore(
        data_dir,
        fsync_policy="always",
        memtable_limit_bytes=40,
        compaction_threshold=2,
    )

    op_id = 0
    try:
        while True:
            key = random.choice(KEY_SPACE)
            # Random mix: 80% put/overwrite, 20% delete
            if random.random() < 0.2:
                store.delete(key)
                # Never print before operation returns
                print(f"DEL {key}", flush=True)
            else:
                val = f"v{op_id:06d}_{random.randint(1000, 9999)}"
                store.put(key, val)
                # Never print before operation returns
                print(f"SET {key} {val}", flush=True)
            op_id += 1
    except (KeyboardInterrupt, BrokenPipeError):
        pass
    finally:
        try:
            store.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

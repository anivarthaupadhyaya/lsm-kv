"""Crash recovery test runner for LSMStore.

Runs crash_writer as a subprocess, reads its stdout, kills it with Popen.kill()
after a random delay, reopens the store, and asserts that the store state matches
the replayed log within a single in-flight operation difference.
"""

import argparse
import os
import random
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Optional

# Ensure project root is in sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from lsm.store import LSMStore
from tests.crash_writer import KEY_SPACE

WRITER_PATH = Path(__file__).resolve().parent / "crash_writer.py"
HARD_ROUND_TIMEOUT = 10.0  # seconds


def pytest_addoption(parser) -> None:
    """Register --rounds command-line option for pytest."""
    try:
        parser.addoption(
            "--rounds",
            action="store",
            default=20,
            type=int,
            help="Number of crash test rounds (default: 20)",
        )
    except Exception:
        pass


def _resolve_rounds(request=None) -> int:
    """Resolves round count from pytest request, sys.argv, or environment."""
    if request is not None:
        try:
            val = request.config.getoption("--rounds")
            if val is not None:
                return int(val)
        except Exception:
            pass

    for i, arg in enumerate(sys.argv):
        if arg == "--rounds" and i + 1 < len(sys.argv):
            try:
                return int(sys.argv[i + 1])
            except ValueError:
                pass
        elif arg.startswith("--rounds="):
            try:
                return int(arg.split("=", 1)[1])
            except ValueError:
                pass

    if "ROUNDS" in os.environ:
        try:
            return int(os.environ["ROUNDS"])
        except ValueError:
            pass

    return 20


def run_single_round(round_idx: int, timeout: float = HARD_ROUND_TIMEOUT) -> None:
    """Executes a single crash round with a fresh temporary directory."""
    t0 = time.monotonic()
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp_dir:
        proc = subprocess.Popen(
            [sys.executable, "-u", str(WRITER_PATH), tmp_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        lines: list[str] = []

        def read_stdout() -> None:
            try:
                if proc.stdout is not None:
                    for line in proc.stdout:
                        s = line.strip()
                        if s:
                            lines.append(s)
            except Exception:
                pass

        reader = threading.Thread(target=read_stdout, daemon=True)
        reader.start()

        # Random delay to allow random mix of operations, flushes, and compactions
        delay = random.uniform(0.06, 0.20)
        time.sleep(delay)

        # Kill the writer process abruptly (simulating a crash)
        proc.kill()
        try:
            proc.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2.0)

        reader.join(timeout=2.0)
        if proc.stdout:
            proc.stdout.close()
        if proc.stderr:
            proc.stderr.close()

        if time.monotonic() - t0 > timeout:
            raise TimeoutError(f"Round {round_idx} exceeded hard timeout of {timeout}s")

        # Replay the printed log into a plain dict
        expected_state: dict[str, Optional[bytes]] = {}
        for line in lines:
            parts = line.split(" ", 2)
            if len(parts) == 3 and parts[0] == "SET":
                expected_state[parts[1]] = parts[2].encode("utf-8")
            elif len(parts) == 2 and parts[0] == "DEL":
                expected_state[parts[1]] = None

        # Reopen the store in the parent (with retry for Windows handle cleanup)
        store = None
        for attempt in range(20):
            try:
                store = LSMStore(tmp_dir)
                break
            except PermissionError:
                if attempt == 19:
                    raise
                time.sleep(0.05)
        try:
            diffs = []
            for key in KEY_SPACE:
                actual_val = store.get(key)
                exp_val = expected_state.get(key, None)
                if actual_val != exp_val:
                    diffs.append((key, actual_val, exp_val))

            assert len(diffs) <= 1, (
                f"Round {round_idx}: expected at most 1 in-flight operation difference, "
                f"but found {len(diffs)} differences: {diffs} (total logged lines: {len(lines)})"
            )

            # Extra sanity check: if there is a single difference, ensure valid format
            if len(diffs) == 1:
                diff_key, actual_val, exp_val = diffs[0]
                assert diff_key in KEY_SPACE
                if actual_val is not None:
                    assert isinstance(actual_val, bytes)
                    assert actual_val.startswith(b"v")
        finally:
            store.close()


def run_rounds(
    rounds: int = 20,
    fail_fast: bool = True,
    timeout_per_round: float = HARD_ROUND_TIMEOUT,
) -> tuple[int, int]:
    """Runs the specified number of crash rounds, tracking passed and failed count."""
    passed = 0
    failed = 0

    for r in range(rounds):
        try:
            run_single_round(r, timeout=timeout_per_round)
            passed += 1
        except Exception as e:
            failed += 1
            if fail_fast:
                raise
            print(f"Round {r} failed: {e}", file=sys.stderr)

    return passed, failed


def test_crash_recovery(request=None, rounds: Optional[int] = None) -> None:
    """Pytest test case running crash recovery test rounds."""
    if rounds is None:
        rounds = _resolve_rounds(request)
    passed, failed = run_rounds(rounds=rounds, fail_fast=True)
    assert failed == 0, f"Crash recovery test had {failed} failures out of {rounds} rounds"


def main() -> None:
    parser = argparse.ArgumentParser(description="LSMStore Crash Recovery Test")
    parser.add_argument(
        "--rounds",
        type=int,
        default=20,
        help="Number of crash test rounds (default: 20)",
    )
    args = parser.parse_args()

    passed, failed = run_rounds(rounds=args.rounds, fail_fast=False)
    print(f"Passed: {passed}, Failed: {failed}")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()

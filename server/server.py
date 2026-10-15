"""Asyncio TCP server wrapping LSMStore with RESP2 protocol support."""

import asyncio
import os
import threading
from pathlib import Path
from typing import Any, Optional, Union

from lsm.store import LSMStore
from server.resp import (
    ProtocolError,
    bulk_string,
    error,
    integer,
    parse,
    simple_string,
)


class Server:
    """Asyncio TCP server serving LSMStore over RESP2."""

    def __init__(
        self,
        store: Optional[LSMStore] = None,
        data_dir: Optional[Union[str, Path]] = None,
        host: str = "127.0.0.1",
        port: int = 6380,
        fsync_policy: Union[str, int, float] = "always",
        use_bloom: bool = True,
        memtable_limit_bytes: Optional[int] = None,
        compaction_threshold: int = 4,
    ) -> None:
        self.host = host
        self.port = port
        self.store: LSMStore = (
            store
            if store is not None
            else LSMStore(
                data_dir=data_dir or "./data",
                fsync_policy=fsync_policy,
                use_bloom=use_bloom,
                memtable_limit_bytes=memtable_limit_bytes,
                compaction_threshold=compaction_threshold,
            )
        )
        self._server: Optional[asyncio.Server] = None
        self._active_writers: set[asyncio.StreamWriter] = set()
        self._active_tasks: set[asyncio.Task] = set()
        self._stop_event: Optional[asyncio.Event] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._closed = False

    def _dispatch(self, args: list[bytes]) -> bytes:
        """Execute a parsed command and return the encoded RESP response."""
        if not args:
            return error("ERR unknown command ''")

        cmd = args[0].upper()
        try:
            if cmd == b"PING":
                if len(args) == 1:
                    return simple_string("PONG")
                elif len(args) == 2:
                    return bulk_string(args[1])
                else:
                    return error("ERR wrong number of arguments for 'ping' command")

            elif cmd == b"SET":
                if len(args) != 3:
                    return error("ERR wrong number of arguments for 'set' command")
                self.store.put(args[1], args[2])
                return simple_string("OK")

            elif cmd == b"GET":
                if len(args) != 2:
                    return error("ERR wrong number of arguments for 'get' command")
                val = self.store.get(args[1])
                return bulk_string(val)

            elif cmd == b"DEL":
                if len(args) != 2:
                    return error("ERR wrong number of arguments for 'del' command")
                key = args[1]
                val = self.store.get(key)
                if val is not None:
                    self.store.delete(key)
                    return integer(1)
                else:
                    return integer(0)

            elif cmd == b"FLUSH":
                self.store.flush()
                return simple_string("OK")

            elif cmd == b"COMPACT":
                self.store.compact()
                return simple_string("OK")

            else:
                name = args[0].decode("utf-8", errors="replace").replace("\r", " ").replace("\n", " ")
                return error(f"ERR unknown command '{name}'")

        except Exception as e:
            msg = str(e).replace("\r", " ").replace("\n", " ")
            return error(f"ERR {msg}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle an individual TCP client connection."""
        task = asyncio.current_task()
        if task is not None:
            self._active_tasks.add(task)
        self._active_writers.add(writer)
        buf = bytearray()

        try:
            while not writer.is_closing():
                try:
                    data = await reader.read(4096)
                except (
                    ConnectionResetError,
                    ConnectionAbortedError,
                    BrokenPipeError,
                    asyncio.IncompleteReadError,
                    OSError,
                ):
                    break
                except Exception:
                    break

                if not data:
                    break

                buf.extend(data)

                # Process all complete requests currently in the buffer
                while True:
                    try:
                        parsed = parse(bytes(buf))
                    except ProtocolError as e:
                        err_msg = str(e).replace("\r", " ").replace("\n", " ")
                        try:
                            writer.write(error(f"ERR Protocol error: {err_msg}"))
                            await writer.drain()
                        except Exception:
                            pass
                        try:
                            writer.close()
                            await writer.wait_closed()
                        except Exception:
                            pass
                        return

                    if parsed is None:
                        # Request is incomplete (split across packets)
                        break

                    cmd_args, consumed = parsed
                    del buf[:consumed]

                    resp = self._dispatch(cmd_args)
                    try:
                        writer.write(resp)
                    except Exception:
                        return

                try:
                    await writer.drain()
                except Exception:
                    break
        finally:
            self._active_writers.discard(writer)
            if task is not None:
                self._active_tasks.discard(task)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def run(self, ready_event: Optional[threading.Event] = None) -> None:
        """Run the server until stop() is called."""
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
        )
        if self._server.sockets:
            self.port = self._server.sockets[0].getsockname()[1]
        if ready_event is not None:
            ready_event.set()
        try:
            await self._stop_event.wait()
        finally:
            await self._cleanup()

    def stop(self) -> None:
        """Signal the server to stop in a thread-safe manner."""
        if (
            self._loop is not None
            and self._loop.is_running()
            and self._stop_event is not None
        ):
            self._loop.call_soon_threadsafe(self._stop_event.set)
        else:
            if self.store is not None and not getattr(self.store, "is_closed", False):
                self.store.close()

    def close(self) -> None:
        """Alias for stop()."""
        self.stop()

    def __enter__(self) -> "Server":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    async def _cleanup(self) -> None:
        """Cleanly close server socket, active clients, and storage."""
        if self._closed:
            return
        self._closed = True

        if self._server is not None:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None

        for w in list(self._active_writers):
            try:
                w.close()
                await w.wait_closed()
            except Exception:
                pass
        self._active_writers.clear()

        for t in list(self._active_tasks):
            if not t.done():
                t.cancel()
        if self._active_tasks:
            await asyncio.gather(*self._active_tasks, return_exceptions=True)
        self._active_tasks.clear()

        if self.store is not None and not getattr(self.store, "is_closed", False):
            self.store.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LSM-KV RESP Server")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", type=int, default=6380, help="Bind port")
    parser.add_argument("--dir", default="./data", help="Data directory")
    parser.add_argument("--fsync-policy", "--fsync", dest="fsync_policy", default="always", help="fsync policy")
    parser.add_argument("--use-bloom", dest="use_bloom", action=argparse.BooleanOptionalAction, default=True, help="Bloom filter")
    parser.add_argument("--no-bloom", dest="use_bloom", action="store_false")
    parser.add_argument("--memtable-limit", dest="memtable_limit", type=int, default=None, help="MemTable limit in bytes")
    parser.add_argument("--compaction-threshold", dest="compaction_threshold", type=int, default=4, help="Compaction threshold")
    cli_args = parser.parse_args()

    srv = Server(
        data_dir=cli_args.dir,
        host=cli_args.host,
        port=cli_args.port,
        fsync_policy=cli_args.fsync_policy,
        use_bloom=cli_args.use_bloom,
        memtable_limit_bytes=cli_args.memtable_limit,
        compaction_threshold=cli_args.compaction_threshold,
    )
    try:
        asyncio.run(srv.run())
    except KeyboardInterrupt:
        pass


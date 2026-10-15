"""Tests for asyncio RESP2 server."""

import asyncio
import contextlib
import threading
import time
from pathlib import Path

import pytest

from server.server import Server
from tests.resp_client import RespClient, ResponseError, encode_command


@contextlib.contextmanager
def start_server_in_thread(data_dir: Path, port: int = 0):
    """Start an asyncio server on port 0 in a background thread."""
    server = Server(data_dir=data_dir, host="127.0.0.1", port=port)
    ready = threading.Event()
    thread = threading.Thread(
        target=lambda: asyncio.run(server.run(ready_event=ready)),
        daemon=True,
    )
    thread.start()
    if not ready.wait(timeout=5.0):
        raise TimeoutError("Server failed to start within timeout")
    try:
        yield server
    finally:
        server.stop()
        thread.join(timeout=5.0)


@pytest.fixture
def running_server(tmp_path):
    with start_server_in_thread(tmp_path) as srv:
        yield srv


@pytest.fixture
def client(running_server):
    with RespClient(running_server.host, running_server.port) as c:
        yield c


def test_ping(client):
    assert client.ping() == b"PONG"
    assert client.execute("PING") == b"PONG"
    assert client.execute("ping") == b"PONG"
    assert client.execute("PING", "hello") == b"hello"


def test_set_then_get(client):
    assert client.set("key1", "val1") == b"OK"
    assert client.get("key1") == b"val1"

    # Binary safe keys and values
    bin_key = b"bin\x00key"
    bin_val = b"bin\r\n\x00val"
    assert client.set(bin_key, bin_val) == b"OK"
    assert client.get(bin_key) == bin_val

    # Empty string value
    assert client.set("empty", "") == b"OK"
    assert client.get("empty") == b""


def test_get_missing(client):
    assert client.get("nonexistent_key") is None


def test_del_existing_and_missing(client):
    # Missing key returns 0
    assert client.delete("not_here") == 0

    # Set and delete existing key returns 1
    assert client.set("del_me", "hello") == b"OK"
    assert client.delete("del_me") == 1
    assert client.get("del_me") is None

    # Deleting again returns 0
    assert client.delete("del_me") == 0


def test_overwrite(client):
    assert client.set("k", "v1") == b"OK"
    assert client.get("k") == b"v1"
    assert client.set("k", "v2") == b"OK"
    assert client.get("k") == b"v2"
    assert client.set("k", "v3") == b"OK"
    assert client.get("k") == b"v3"


def test_unknown_command(client):
    # Unknown command raises ResponseError
    with pytest.raises(ResponseError, match="unknown command"):
        client.execute("NOT_A_COMMAND")

    # Command without arguments when required
    with pytest.raises(ResponseError, match="wrong number of arguments"):
        client.execute("SET", "only_one")

    with pytest.raises(ResponseError, match="wrong number of arguments"):
        client.execute("GET")

    with pytest.raises(ResponseError, match="wrong number of arguments"):
        client.execute("GET", "k1", "k2")

    with pytest.raises(ResponseError, match="wrong number of arguments"):
        client.execute("DEL")

    with pytest.raises(ResponseError, match="wrong number of arguments"):
        client.execute("PING", "arg1", "arg2")


def test_pipelined_commands_in_one_send(client):
    # Send multiple commands in a single network transmission
    batch = (
        encode_command("SET", "p1", "v1")
        + encode_command("SET", "p2", "v2")
        + encode_command("GET", "p1")
        + encode_command("GET", "p2")
        + encode_command("DEL", "p1")
        + encode_command("GET", "p1")
        + encode_command("PING")
    )
    client.send_raw(batch)

    assert client.read_response() == b"OK"
    assert client.read_response() == b"OK"
    assert client.read_response() == b"v1"
    assert client.read_response() == b"v2"
    assert client.read_response() == 1
    assert client.read_response() is None
    assert client.read_response() == b"PONG"


def test_two_clients_at_once(running_server):
    with RespClient(running_server.host, running_server.port) as c1:
        with RespClient(running_server.host, running_server.port) as c2:
            assert c1.set("shared", "val_from_c1") == b"OK"
            assert c2.get("shared") == b"val_from_c1"

            assert c2.set("shared2", "val_from_c2") == b"OK"
            assert c1.get("shared2") == b"val_from_c2"


def test_data_surviving_server_restart(tmp_path):
    # First server lifecycle: write data and cleanly shutdown
    with start_server_in_thread(tmp_path) as srv1:
        with RespClient(srv1.host, srv1.port) as c1:
            assert c1.set("persist1", "data1") == b"OK"
            assert c1.set("persist2", "data2") == b"OK"
            assert c1.set("temp", "temp_data") == b"OK"
            assert c1.delete("temp") == 1

    # Second server lifecycle: start on the same data directory
    with start_server_in_thread(tmp_path) as srv2:
        with RespClient(srv2.host, srv2.port) as c2:
            assert c2.get("persist1") == b"data1"
            assert c2.get("persist2") == b"data2"
            assert c2.get("temp") is None


def test_split_request_buffering(client):
    # Send a request fragmented across multiple writes
    cmd = encode_command("SET", "split_key", "split_val")
    client.send_raw(cmd[:5])
    time.sleep(0.02)
    client.send_raw(cmd[5:])

    assert client.read_response() == b"OK"
    assert client.get("split_key") == b"split_val"


def test_protocol_error_closes_connection(running_server):
    with RespClient(running_server.host, running_server.port) as c:
        # Send invalid RESP input (doesn't start with '*')
        c.send_raw(b"INVALID_PROTOCOL_LINE\r\n")
        resp = c.read_response(raise_error=False)
        assert b"ERR Protocol error" in resp

        # Server should have closed the connection
        with pytest.raises((ConnectionResetError, ConnectionError, OSError)):
            c.read_response(raise_error=False)


def test_client_disconnect_never_crashes_server(running_server):
    # Abruptly connect and disconnect without sending complete command
    c1 = RespClient(running_server.host, running_server.port)
    c1.send_raw(b"*2\r\n$3\r\nSET\r\n")
    c1.close()

    # The server must continue serving normal clients
    with RespClient(running_server.host, running_server.port) as c2:
        assert c2.ping() == b"PONG"
        assert c2.set("alive", "yes") == b"OK"
        assert c2.get("alive") == b"yes"

"""Tiny blocking RESP2 client for testing and benchmarks."""

import socket
from typing import Any, Optional, Union

from server.resp import array, bulk_string


class ResponseError(Exception):
    """Raised when the server returns a RESP error (-ERR ...)."""
    pass


def encode_command(*args: Union[str, bytes]) -> bytes:
    """Encode arguments into a RESP2 array of bulk strings."""
    return array([bulk_string(a) for a in args])


class RespClient:
    """Tiny blocking RESP client using standard library sockets."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        timeout: float = 5.0,
        connect: bool = True,
    ) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self._file: Optional[Any] = None
        if connect:
            self.connect()

    def connect(self) -> None:
        """Connect to the server."""
        if self.sock is not None:
            self.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))
        self._file = self.sock.makefile("rb")

    def close(self) -> None:
        """Close the connection."""
        if self._file is not None:
            try:
                self._file.close()
            except Exception:
                pass
            self._file = None
        if self.sock is not None:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def __enter__(self) -> "RespClient":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def send_cmd(self, *args: Union[str, bytes]) -> None:
        """Encode and send a single RESP command."""
        self.send_raw(encode_command(*args))

    def send_raw(self, data: bytes) -> None:
        """Send raw bytes directly over the socket."""
        if self.sock is None:
            raise ConnectionError("Not connected")
        self.sock.sendall(data)

    def read_response(self, raise_error: bool = True) -> Any:
        """Read and parse a single RESP response from the server."""
        if self._file is None:
            raise ConnectionError("Not connected")
        line = self._file.readline()
        if not line:
            raise ConnectionResetError("Server closed connection")
        if not line.endswith(b"\r\n"):
            raise ConnectionError(f"Incomplete line from server: {line!r}")

        prefix = line[0:1]
        content = line[1:-2]

        if prefix == b"+":
            return content
        elif prefix == b"-":
            if raise_error:
                raise ResponseError(content.decode("utf-8", errors="replace"))
            return line
        elif prefix == b":":
            return int(content)
        elif prefix == b"$":
            bulk_len = int(content)
            if bulk_len == -1:
                return None
            data = self._file.read(bulk_len)
            crlf = self._file.read(2)
            if crlf != b"\r\n":
                raise ConnectionError(f"Expected CRLF after bulk data, got {crlf!r}")
            return data
        elif prefix == b"*":
            num_items = int(content)
            if num_items == -1:
                return None
            return [self.read_response(raise_error=raise_error) for _ in range(num_items)]
        else:
            raise ConnectionError(f"Unexpected RESP prefix: {prefix!r}")

    def execute(self, *args: Union[str, bytes], raise_error: bool = True) -> Any:
        """Send a command and return its response."""
        self.send_cmd(*args)
        return self.read_response(raise_error=raise_error)

    def ping(self, msg: Optional[Union[str, bytes]] = None) -> bytes:
        """Send PING command."""
        if msg is None:
            return self.execute("PING")
        return self.execute("PING", msg)

    def set(self, key: Union[str, bytes], value: Union[str, bytes]) -> bytes:
        """Send SET key value command."""
        return self.execute("SET", key, value)

    def get(self, key: Union[str, bytes]) -> Optional[bytes]:
        """Send GET key command."""
        return self.execute("GET", key)

    def delete(self, key: Union[str, bytes]) -> int:
        """Send DEL key command."""
        return self.execute("DEL", key)

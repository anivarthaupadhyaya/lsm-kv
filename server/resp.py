"""RESP2 (Redis Serialization Protocol 2) codec.

Provides parsing for client requests (arrays of bulk strings) with support
for pipelining, split byte boundaries, and protocol limits, as well as encoders
for RESP2 response data types.
"""

from typing import Union

MAX_BULK_LEN: int = 1024 * 1024  # 1 MB
MAX_ARGS: int = 1024
NULL_BULK: bytes = b"$-1\r\n"
NULL_ARRAY: bytes = b"*-1\r\n"


class ProtocolError(Exception):
    """Raised when RESP input is malformed or violates protocol limits."""
    pass


def simple_string(s: str | bytes) -> bytes:
    """Encode a RESP2 simple string (+<string>\\r\\n)."""
    if isinstance(s, str):
        raw = s.encode("utf-8")
    elif isinstance(s, (bytes, bytearray)):
        raw = bytes(s)
    else:
        raise TypeError(f"Expected str or bytes, got {type(s).__name__}")
    if b"\r" in raw or b"\n" in raw:
        raise ValueError("Simple string cannot contain CR or LF characters")
    return b"+" + raw + b"\r\n"


def error(msg: str | bytes) -> bytes:
    """Encode a RESP2 error (-<message>\\r\\n)."""
    if isinstance(msg, str):
        raw = msg.encode("utf-8")
    elif isinstance(msg, (bytes, bytearray)):
        raw = bytes(msg)
    else:
        raise TypeError(f"Expected str or bytes, got {type(msg).__name__}")
    if b"\r" in raw or b"\n" in raw:
        raise ValueError("Error message cannot contain CR or LF characters")
    return b"-" + raw + b"\r\n"


def integer(val: int) -> bytes:
    """Encode a RESP2 integer (:<int>\\r\\n)."""
    if not isinstance(val, int) or isinstance(val, bool):
        raise TypeError(f"Expected int, got {type(val).__name__}")
    return f":{val}\r\n".encode("ascii")


def bulk_string(data: str | bytes | bytearray | None) -> bytes:
    """Encode a RESP2 bulk string ($<len>\\r\\n<data>\\r\\n or $-1\\r\\n for null)."""
    if data is None:
        return NULL_BULK
    if isinstance(data, str):
        raw = data.encode("utf-8")
    elif isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
    else:
        raise TypeError(f"Expected str, bytes, or None, got {type(data).__name__}")
    return f"${len(raw)}\r\n".encode("ascii") + raw + b"\r\n"


def array(items: list[bytes] | None) -> bytes:
    """Encode a RESP2 array (*<len>\\r\\n<items> or *-1\\r\\n for null)."""
    if items is None:
        return NULL_ARRAY
    return f"*{len(items)}\r\n".encode("ascii") + b"".join(items)


def parse(
    buffer: bytes,
    max_bulk_len: int = MAX_BULK_LEN,
    max_args: int = MAX_ARGS,
) -> tuple[list[bytes], int] | None:
    """Parse a single command from buffer.

    Requests must be RESP arrays of bulk strings (*N\\r\\n$len\\r\\n...).

    Returns:
        (command_as_list_of_bytes, bytes_consumed) if a full command is parsed.
        None if buffer holds an incomplete request.

    Raises:
        ProtocolError if the input is malformed or exceeds limits.
    """
    if isinstance(buffer, (bytearray, memoryview)):
        buffer = bytes(buffer)
    elif not isinstance(buffer, bytes):
        raise TypeError(f"Expected bytes-like object, got {type(buffer).__name__}")

    if not buffer:
        return None

    if buffer[0] != ord(b"*"):
        raise ProtocolError(f"Expected '*', got {buffer[0:1]!r}")

    crlf_pos = buffer.find(b"\r\n")
    if crlf_pos == -1:
        part = buffer[1:]
        check_part = part[:-1] if part.endswith(b"\r") else part
        if len(part) > 32:
            raise ProtocolError("Array length header too long without CRLF")
        if check_part and not check_part.isdigit():
            raise ProtocolError(f"Malformed array length: {check_part!r}")
        return None

    array_len_bytes = buffer[1:crlf_pos]
    if not array_len_bytes.isdigit():
        raise ProtocolError(f"Malformed array length: {array_len_bytes!r}")

    num_args = int(array_len_bytes)
    if num_args > max_args:
        raise ProtocolError(f"Too many arguments: {num_args} > {max_args}")

    cursor = crlf_pos + 2
    args: list[bytes] = []

    for _ in range(num_args):
        if cursor >= len(buffer):
            return None

        if buffer[cursor] != ord(b"$"):
            raise ProtocolError(f"Expected '$', got {buffer[cursor:cursor+1]!r}")

        bulk_crlf = buffer.find(b"\r\n", cursor + 1)
        if bulk_crlf == -1:
            part = buffer[cursor + 1:]
            check_part = part[:-1] if part.endswith(b"\r") else part
            if len(part) > 32:
                raise ProtocolError("Bulk length header too long without CRLF")
            if check_part and not check_part.isdigit():
                raise ProtocolError(f"Malformed bulk length: {check_part!r}")
            return None

        len_bytes = buffer[cursor + 1 : bulk_crlf]
        if not len_bytes.isdigit():
            raise ProtocolError(f"Malformed bulk length: {len_bytes!r}")

        bulk_len = int(len_bytes)
        if bulk_len > max_bulk_len:
            raise ProtocolError(
                f"Bulk string length {bulk_len} exceeds maximum allowed size ({max_bulk_len} bytes)"
            )

        data_start = bulk_crlf + 2
        data_end = data_start + bulk_len
        total_bulk_end = data_end + 2

        if len(buffer) < total_bulk_end:
            if len(buffer) > data_end and buffer[data_end] != ord(b"\r"):
                raise ProtocolError("Missing CRLF after bulk string data")
            return None

        if buffer[data_end:total_bulk_end] != b"\r\n":
            raise ProtocolError(
                f"Expected CRLF after bulk string data, got {buffer[data_end:total_bulk_end]!r}"
            )

        args.append(buffer[data_start:data_end])
        cursor = total_bulk_end

    return args, cursor

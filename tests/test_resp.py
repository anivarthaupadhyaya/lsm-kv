import pytest

from server.resp import (
    MAX_ARGS,
    MAX_BULK_LEN,
    NULL_ARRAY,
    NULL_BULK,
    ProtocolError,
    array,
    bulk_string,
    error,
    integer,
    parse,
    simple_string,
)


def test_parse_complete_command():
    # Single argument
    res = parse(b"*1\r\n$4\r\nPING\r\n")
    assert res == ([b"PING"], 14)

    # Multiple arguments
    raw = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nhello\r\n"
    res = parse(raw)
    assert res == ([b"SET", b"foo", b"hello"], len(raw))

    # Empty bulk string argument
    raw_empty = b"*2\r\n$3\r\nSET\r\n$0\r\n\r\n"
    res = parse(raw_empty)
    assert res == ([b"SET", b""], len(raw_empty))

    # Binary payload containing null bytes and internal \r\n
    bin_payload = b"a\r\nb\x00c"
    raw_bin = b"*2\r\n$4\r\nECHO\r\n$" + str(len(bin_payload)).encode("ascii") + b"\r\n" + bin_payload + b"\r\n"
    res = parse(raw_bin)
    assert res == ([b"ECHO", bin_payload], len(raw_bin))

    # Bytearray and memoryview input
    assert parse(bytearray(raw)) == ([b"SET", b"foo", b"hello"], len(raw))
    assert parse(memoryview(raw)) == ([b"SET", b"foo", b"hello"], len(raw))


def test_parse_split_byte_by_byte():
    # Test standard command split at every byte boundary
    cmd = b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n"
    for i in range(len(cmd)):
        prefix = cmd[:i]
        assert parse(prefix) is None, f"Expected None at length {i}, got result for {prefix!r}"
    assert parse(cmd) == ([b"GET", b"mykey"], len(cmd))

    # Test binary payload containing \r\n split byte by byte
    bin_payload = b"foo\r\nbar"
    bin_cmd = b"*2\r\n$4\r\nECHO\r\n$" + str(len(bin_payload)).encode("ascii") + b"\r\n" + bin_payload + b"\r\n"
    for i in range(len(bin_cmd)):
        prefix = bin_cmd[:i]
        assert parse(prefix) is None, f"Expected None at length {i} for binary cmd"
    assert parse(bin_cmd) == ([b"ECHO", bin_payload], len(bin_cmd))


def test_parse_two_pipelined_commands():
    cmd1 = b"*1\r\n$4\r\nPING\r\n"
    cmd2 = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    buf = cmd1 + cmd2

    # Parse first command
    res1 = parse(buf)
    assert res1 is not None
    cmd, consumed = res1
    assert cmd == [b"PING"]
    assert consumed == len(cmd1)

    # Slice and parse second command
    buf = buf[consumed:]
    res2 = parse(buf)
    assert res2 is not None
    cmd, consumed = res2
    assert cmd == [b"GET", b"foo"]
    assert consumed == len(cmd2)

    # Remaining buffer is empty
    buf = buf[consumed:]
    assert parse(buf) is None


def test_parse_pipelined_with_partial_second_command():
    cmd1 = b"*1\r\n$4\r\nPING\r\n"
    cmd2 = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    buf = cmd1 + cmd2[:6]

    res1 = parse(buf)
    assert res1 is not None
    cmd, consumed = res1
    assert cmd == [b"PING"]
    assert consumed == len(cmd1)

    # Second command is incomplete
    buf = buf[consumed:]
    assert parse(buf) is None

    # Complete the second command
    buf += cmd2[6:]
    res2 = parse(buf)
    assert res2 == ([b"GET", b"foo"], len(cmd2))


def test_parse_malformed_input():
    # Does not start with '*'
    with pytest.raises(ProtocolError):
        parse(b"+PING\r\n")
    with pytest.raises(ProtocolError):
        parse(b"PING\r\n")
    with pytest.raises(ProtocolError):
        parse(b":10\r\n")
    with pytest.raises(ProtocolError):
        parse(b"$4\r\nPING\r\n")

    # Malformed array count
    with pytest.raises(ProtocolError):
        parse(b"*abc\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*-1\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*bad")

    # Element not starting with '$'
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n+OK\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n:123\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n*1\r\n$1\r\na\r\n")

    # Malformed bulk length
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$abc\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$-1\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$\r\n")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$bad")

    # Corrupted CRLF after bulk string data
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$3\r\nFOOXX")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$3\r\nFOO\rX")
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$3\r\nFOOX")

    # Exceeding max arguments
    with pytest.raises(ProtocolError):
        parse(b"*1025\r\n")

    # Non-bytes input
    with pytest.raises(TypeError):
        parse("not bytes")  # type: ignore


def test_parse_oversized_bulk_string():
    # Exceeds default limit of 1 MB (1048576)
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$1048577\r\n")

    # Exactly 1 MB is accepted (returns None since payload is incomplete)
    assert parse(b"*1\r\n$1048576\r\n") is None

    # Custom max_bulk_len
    with pytest.raises(ProtocolError):
        parse(b"*1\r\n$15\r\n123456789012345\r\n", max_bulk_len=10)


def test_encoders_simple_string():
    assert simple_string("OK") == b"+OK\r\n"
    assert simple_string(b"PONG") == b"+PONG\r\n"
    assert simple_string("hello world") == b"+hello world\r\n"

    with pytest.raises(ValueError):
        simple_string("line1\r\nline2")
    with pytest.raises(ValueError):
        simple_string("line1\nline2")
    with pytest.raises(ValueError):
        simple_string("line1\rline2")
    with pytest.raises(TypeError):
        simple_string(123)  # type: ignore


def test_encoders_error():
    assert error("ERR unknown command") == b"-ERR unknown command\r\n"
    assert error(b"WRONGTYPE") == b"-WRONGTYPE\r\n"

    with pytest.raises(ValueError):
        error("ERR\r\nfoo")
    with pytest.raises(ValueError):
        error("ERR\nfoo")
    with pytest.raises(TypeError):
        error(None)  # type: ignore


def test_encoders_integer():
    assert integer(0) == b":0\r\n"
    assert integer(1000) == b":1000\r\n"
    assert integer(-42) == b":-42\r\n"

    with pytest.raises(TypeError):
        integer(True)  # type: ignore
    with pytest.raises(TypeError):
        integer(False)  # type: ignore
    with pytest.raises(TypeError):
        integer("100")  # type: ignore


def test_encoders_bulk_string():
    assert bulk_string(b"foobar") == b"$6\r\nfoobar\r\n"
    assert bulk_string("foobar") == b"$6\r\nfoobar\r\n"
    assert bulk_string(b"") == b"$0\r\n\r\n"
    assert bulk_string("") == b"$0\r\n\r\n"
    assert bulk_string(None) == NULL_BULK
    assert bulk_string(None) == b"$-1\r\n"

    # Binary safe
    assert bulk_string(b"foo\r\n\x00bar") == b"$10\r\nfoo\r\n\x00bar\r\n"

    with pytest.raises(TypeError):
        bulk_string(123)  # type: ignore


def test_encoders_array():
    assert array([bulk_string(b"GET"), bulk_string(b"k")]) == b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
    assert array([]) == b"*0\r\n"
    assert array(None) == NULL_ARRAY
    assert array(None) == b"*-1\r\n"

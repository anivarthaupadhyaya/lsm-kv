import struct
import zlib
import pytest

from lsm.bloom import BloomFilter, HEADER_FORMAT, HEADER_SIZE


def test_bloom_filter_sizing_formulas():
    """Verify bit-array size m and hash count k match theoretical formulas."""
    # n=10000, p=0.01 -> m=95851, k=7
    bf1 = BloomFilter(expected_items=10000, fp_rate=0.01)
    assert bf1.m == 95851
    assert bf1.k == 7
    assert len(bf1._bit_array) == (95851 + 7) // 8
    assert bf1.byte_size == HEADER_SIZE + len(bf1._bit_array)

    # n=1000, p=0.05 -> m=6236, k=4
    bf2 = BloomFilter(expected_items=1000, fp_rate=0.05)
    assert bf2.m == 6236
    assert bf2.k == 4

    # n=500, p=0.001 -> m=7189, k=10
    bf3 = BloomFilter(expected_items=500, fp_rate=0.001)
    assert bf3.m == 7189
    assert bf3.k == 10


def test_no_false_negatives_10000_keys():
    """Verify zero false negatives across 10,000 added keys."""
    n = 10000
    bf = BloomFilter(expected_items=n, fp_rate=0.01)

    keys = [f"item_key_{i:06d}".encode("utf-8") for i in range(n)]
    for key in keys:
        bf.add(key)

    # All inserted keys MUST be reported as might_contain
    for key in keys:
        assert bf.might_contain(key) is True
        assert (key in bf) is True


def test_false_positive_rate_10000_non_members():
    """Verify measured false-positive rate on 10,000 non-members is under 2x target."""
    n = 10000
    target_fp = 0.01
    bf = BloomFilter(expected_items=n, fp_rate=target_fp)

    # Populate with members
    for i in range(n):
        bf.add(f"member_{i:06d}".encode("utf-8"))

    # Test non-members
    false_positives = 0
    non_member_count = 10000
    for i in range(non_member_count):
        non_member_key = f"non_member_{i:06d}".encode("utf-8")
        if bf.might_contain(non_member_key):
            false_positives += 1

    measured_fp_rate = false_positives / non_member_count

    # Must be under 2x target (0.02)
    assert measured_fp_rate < 2 * target_fp
    # Should realistically have some false positives around 1%
    assert measured_fp_rate > 0.001


def test_serialization_round_trip():
    """Verify to_bytes() and from_bytes() preserve all filter state and members."""
    bf = BloomFilter(expected_items=500, fp_rate=0.02)
    test_keys = [b"apple", b"banana", b"cherry", "date", "elderberry"]
    for k in test_keys:
        bf.add(k)

    raw_data = bf.to_bytes()
    assert len(raw_data) == HEADER_SIZE + (bf.m + 7) // 8

    # Unpack header directly and verify contents
    m, k, crc = struct.unpack_from(HEADER_FORMAT, raw_data, 0)
    assert m == bf.m
    assert k == bf.k
    assert crc == zlib.crc32(bf._bit_array)

    # Reconstruct via from_bytes
    restored = BloomFilter.from_bytes(raw_data)
    assert restored.m == bf.m
    assert restored.k == bf.k
    assert restored._bit_array == bf._bit_array

    for k in test_keys:
        assert restored.might_contain(k) is True

    # Re-serialization matches byte-for-byte
    assert restored.to_bytes() == raw_data


def test_corrupted_payload_byte_rejected():
    """Verify corrupted byte in the bit-array payload is rejected with clear error."""
    bf = BloomFilter(expected_items=200, fp_rate=0.01)
    bf.add(b"test_key")
    raw = bytearray(bf.to_bytes())

    # Flip a bit in the last byte of the payload
    raw[-1] ^= 0x01
    with pytest.raises(ValueError, match="checksum mismatch"):
        BloomFilter.from_bytes(raw)

    # Flip a bit in the first byte of payload
    raw = bytearray(bf.to_bytes())
    raw[HEADER_SIZE] ^= 0x80
    with pytest.raises(ValueError, match="checksum mismatch"):
        BloomFilter.from_bytes(raw)


def test_corrupted_checksum_rejected():
    """Verify corrupted checksum in header raises clear ValueError."""
    bf = BloomFilter(expected_items=200, fp_rate=0.01)
    bf.add(b"test_key")
    raw = bytearray(bf.to_bytes())

    # Checksum is located at bytes 8..11
    raw[10] ^= 0xFF
    with pytest.raises(ValueError, match="checksum mismatch"):
        BloomFilter.from_bytes(raw)


def test_truncated_data_rejected():
    """Verify truncated data raises ValueError."""
    bf = BloomFilter(expected_items=100, fp_rate=0.01)
    bf.add(b"key")
    raw = bf.to_bytes()

    # Truncated header
    with pytest.raises(ValueError, match="data too short"):
        BloomFilter.from_bytes(raw[:6])

    with pytest.raises(ValueError, match="data too short"):
        BloomFilter.from_bytes(b"")

    # Truncated payload
    with pytest.raises(ValueError, match="payload length mismatch"):
        BloomFilter.from_bytes(raw[:-3])


def test_trailing_extra_bytes_rejected():
    """Verify extra trailing bytes raise ValueError."""
    bf = BloomFilter(expected_items=100, fp_rate=0.01)
    raw = bf.to_bytes()

    with pytest.raises(ValueError, match="payload length mismatch"):
        BloomFilter.from_bytes(raw + b"garbage_bytes")


def test_corrupted_m_and_k_rejected():
    """Verify zero or invalid m and k values are rejected."""
    # Construct invalid header with m=0
    bad_m = struct.pack(HEADER_FORMAT, 0, 7, 12345) + b"\x00" * 10
    with pytest.raises(ValueError, match="invalid bit-array size"):
        BloomFilter.from_bytes(bad_m)

    # Construct invalid header with k=0
    bad_k = struct.pack(HEADER_FORMAT, 100, 0, 12345) + b"\x00" * 13
    with pytest.raises(ValueError, match="invalid hash count"):
        BloomFilter.from_bytes(bad_k)


def test_key_types_and_handling():
    """Verify str and bytes can be added and checked interchangeably."""
    bf = BloomFilter(expected_items=100)
    bf.add("str_key")
    assert bf.might_contain(b"str_key") is True
    assert bf.might_contain("str_key") is True

    bf.add(b"bytes_key")
    assert bf.might_contain("bytes_key") is True
    assert bf.might_contain(b"bytes_key") is True

    # Missing key
    assert bf.might_contain("never_added") is False

    # Invalid key type
    with pytest.raises(TypeError, match="Key must be bytes or str"):
        bf.add(12345)  # type: ignore

    with pytest.raises(TypeError, match="Key must be bytes or str"):
        bf.might_contain(None)  # type: ignore


def test_invalid_init_parameters():
    """Verify constructor raises ValueError for invalid expected_items or fp_rate."""
    with pytest.raises(ValueError, match="expected_items must be positive"):
        BloomFilter(expected_items=0)

    with pytest.raises(ValueError, match="expected_items must be positive"):
        BloomFilter(expected_items=-10)

    with pytest.raises(ValueError, match="fp_rate must be between 0 and 1"):
        BloomFilter(expected_items=100, fp_rate=0.0)

    with pytest.raises(ValueError, match="fp_rate must be between 0 and 1"):
        BloomFilter(expected_items=100, fp_rate=1.0)

    with pytest.raises(ValueError, match="fp_rate must be between 0 and 1"):
        BloomFilter(expected_items=100, fp_rate=-0.5)

    with pytest.raises(TypeError, match="data must be bytes-like"):
        BloomFilter.from_bytes("not_bytes")  # type: ignore

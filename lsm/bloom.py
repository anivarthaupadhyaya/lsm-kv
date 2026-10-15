"""Bloom Filter module for probabilistic set membership in LSM-Tree.

Bit-array sizing:
    m = -n * ln(p) / (ln 2)^2
Hash count:
    k = (m / n) * ln 2

Serialization Format:
    Header (12 bytes, big-endian):
        m:        4 bytes uint32 (bit array size in bits)
        k:        4 bytes uint32 (number of hash functions)
        checksum: 4 bytes uint32 (CRC32 checksum of bit-array payload)
    Payload:
        bit_array: (m + 7) // 8 bytes
"""

import hashlib
import math
import struct
import zlib
from typing import Optional, Union

HEADER_FORMAT = ">III"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


class BloomFilter:
    """Probabilistic set membership filter using Kirsch-Mitzenmacher double hashing.

    Computes optimal bit-array size `m` and hash count `k` based on expected items `n`
    and target false-positive rate `p`. Uses blake2b for double hashing (h1 + i*h2).
    """

    def __init__(self, expected_items: int = 1000, fp_rate: float = 0.01) -> None:
        if not isinstance(expected_items, (int, float)) or expected_items <= 0:
            raise ValueError(f"expected_items must be positive, got {expected_items}")
        if not (0.0 < fp_rate < 1.0):
            raise ValueError(f"fp_rate must be between 0 and 1 exclusive, got {fp_rate}")

        self.expected_items = int(expected_items)
        self.fp_rate = float(fp_rate)

        # m = -n·ln(p)/(ln 2)²
        n = float(self.expected_items)
        p = self.fp_rate
        m_calc = -n * math.log(p) / (math.log(2) ** 2)
        self.m: int = max(1, int(math.ceil(m_calc)))

        # k = (m/n)·ln 2
        k_calc = (self.m / n) * math.log(2)
        self.k: int = max(1, int(round(k_calc)))

        self._num_bytes = (self.m + 7) // 8
        self._bit_array = bytearray(self._num_bytes)

    @property
    def byte_size(self) -> int:
        """Total serialized size in bytes including 12-byte header."""
        return HEADER_SIZE + self._num_bytes

    @staticmethod
    def _to_bytes(key: Union[bytes, str]) -> bytes:
        if isinstance(key, str):
            return key.encode("utf-8")
        if isinstance(key, (bytes, bytearray, memoryview)):
            return bytes(key)
        raise TypeError(f"Key must be bytes or str, got {type(key).__name__}")

    def add(self, key: Union[bytes, str]) -> None:
        """Inserts a key into the Bloom filter."""
        key_bytes = self._to_bytes(key)
        digest = hashlib.blake2b(key_bytes, digest_size=16).digest()
        h1, h2 = struct.unpack(">QQ", digest)
        if h2 == 0:
            h2 = 1

        for i in range(self.k):
            idx = (h1 + i * h2) % self.m
            self._bit_array[idx >> 3] |= (1 << (idx & 7))

    def might_contain(self, key: Union[bytes, str]) -> bool:
        """Tests whether a key might be in the set.

        Returns False if the key is definitely absent,
        or True if the key might be present.
        """
        key_bytes = self._to_bytes(key)
        digest = hashlib.blake2b(key_bytes, digest_size=16).digest()
        h1, h2 = struct.unpack(">QQ", digest)
        if h2 == 0:
            h2 = 1

        for i in range(self.k):
            idx = (h1 + i * h2) % self.m
            if not (self._bit_array[idx >> 3] & (1 << (idx & 7))):
                return False
        return True

    def __contains__(self, key: Union[bytes, str]) -> bool:
        return self.might_contain(key)

    def to_bytes(self) -> bytes:
        """Serializes the Bloom filter into binary bytes with header and CRC32 checksum."""
        checksum = zlib.crc32(self._bit_array)
        header = struct.pack(HEADER_FORMAT, self.m, self.k, checksum)
        return header + bytes(self._bit_array)

    @classmethod
    def from_bytes(cls, data: Union[bytes, bytearray, memoryview]) -> "BloomFilter":
        """Deserializes a Bloom filter from bytes, verifying header and CRC32 checksum."""
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data must be bytes-like, got {type(data).__name__}")

        if len(data) < HEADER_SIZE:
            raise ValueError(
                f"Corrupted BloomFilter: data too short ({len(data)} < {HEADER_SIZE} bytes header)"
            )

        m, k, stored_checksum = struct.unpack_from(HEADER_FORMAT, data, 0)
        if m <= 0:
            raise ValueError(f"Corrupted BloomFilter: invalid bit-array size m={m}")
        if k <= 0:
            raise ValueError(f"Corrupted BloomFilter: invalid hash count k={k}")

        expected_payload_len = (m + 7) // 8
        payload = data[HEADER_SIZE:]
        if len(payload) != expected_payload_len:
            raise ValueError(
                f"Corrupted BloomFilter: payload length mismatch "
                f"(expected {expected_payload_len} bytes for m={m}, got {len(payload)})"
            )

        computed_checksum = zlib.crc32(payload)
        if computed_checksum != stored_checksum:
            raise ValueError(
                f"Corrupted BloomFilter: checksum mismatch "
                f"(stored 0x{stored_checksum:08x}, computed 0x{computed_checksum:08x})"
            )

        bf = cls.__new__(cls)
        bf.m = m
        bf.k = k
        bf._num_bytes = expected_payload_len
        bf._bit_array = bytearray(payload)
        bf.expected_items = max(1, int(round(m * math.log(2) / k)))
        bf.fp_rate = (1.0 - math.exp(-k * bf.expected_items / m)) ** k
        return bf

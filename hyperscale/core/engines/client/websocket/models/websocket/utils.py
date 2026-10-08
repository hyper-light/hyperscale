import hashlib
import os
import struct
from base64 import b64encode
from base64 import encodebytes as base64encode
from typing import Any, Tuple

from .constants import HEADER_LENGTH_INDEX, WEBSOCKET_ACCEPT_GUID


def create_sec_websocket_key():
    randomness = os.urandom(16)
    return base64encode(randomness).decode("utf-8").strip()


def pack_hostname(hostname):
    # IPv6 address
    if ":" in hostname:
        return "[" + hostname + "]"

    return hostname


def get_header_bits(raw_headers: bytes):
    b1 = raw_headers[0]
    fin = b1 >> 7 & 1
    rsv1 = b1 >> 6 & 1
    rsv2 = b1 >> 5 & 1
    rsv3 = b1 >> 4 & 1
    opcode = b1 & 0xF
    b2 = raw_headers[1]
    has_mask = b2 >> 7 & 1
    length_bits = b2 & 0x7F

    header_bits = (fin, rsv1, rsv2, rsv3, opcode, has_mask, length_bits)

    return header_bits


async def get_message_buffer_size(header_bits: Tuple[int], connection: Any):
    bits = header_bits[HEADER_LENGTH_INDEX]
    length_bits = bits & 0x7F
    length = 0
    if length_bits == 0x7E:
        v = await connection.readexactly(2)
        length = struct.unpack("!H", v)[0]
    elif length_bits == 0x7F:
        v = await connection.readexactly(8)
        length = struct.unpack("!Q", v)[0]
    else:
        length = length_bits

    return length


def websocket_accept(key: str) -> bytes:
    """The Sec-WebSocket-Accept a server must answer ``key`` with (RFC 6455, 4.2.2)."""
    return b64encode(hashlib.sha1(key.encode() + WEBSOCKET_ACCEPT_GUID).digest())


def mask_payload(payload: bytes, mask: bytes) -> bytes:
    """``payload`` XORed with the repeating four-byte ``mask`` (RFC 6455, 5.3), one big-integer XOR."""
    length = len(payload)
    if length == 0:
        return b""

    repeated = (mask * ((length + 3) // 4))[:length]
    return (int.from_bytes(payload, "little") ^ int.from_bytes(repeated, "little")).to_bytes(length, "little")


def encode_frame(opcode: int, payload: bytes, final: bool = True) -> bytes:
    """
    One client frame (RFC 6455, 5.2), masked as every client frame must be,
    with a fresh mask from a strong entropy source (5.3).
    """
    length = len(payload)
    first_byte = (0x80 if final else 0x00) | opcode

    if length < 126:
        header = bytes((first_byte, 0x80 | length))

    elif length < 65536:
        header = bytes((first_byte, 0x80 | 126)) + length.to_bytes(2, "big")

    else:
        header = bytes((first_byte, 0x80 | 127)) + length.to_bytes(8, "big")

    mask = os.urandom(4)
    return header + mask + mask_payload(payload, mask)

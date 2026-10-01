from __future__ import annotations

import struct

from hyperscale.distributed.ledger.storage_format.unrecognized_storage_format_error import (
    UnrecognizedStorageFormatError,
)

_VERSION_STRUCT = struct.Struct(">I")


class StorageFormat:
    """A persisted format's identity: a 4-byte magic and a version,
    written as an 8-byte header ahead of the format's bytes.

    Bytes without the expected header are refused
    (``UnrecognizedStorageFormatError``): this node cannot assume what a
    disk holds, so data it did not write in this format is never
    interpreted as if it had.
    """

    __slots__ = ("_magic", "_version", "_header")

    def __init__(self, magic: bytes, version: int) -> None:
        if len(magic) != 4:
            raise ValueError(f"magic must be 4 bytes, got {magic!r}")
        self._magic = magic
        self._version = version
        self._header = magic + _VERSION_STRUCT.pack(version)

    @property
    def header(self) -> bytes:
        return self._header

    @property
    def header_size(self) -> int:
        return len(self._header)

    def encode(self, payload: bytes) -> bytes:
        return self._header + payload

    def decode(self, data: bytes) -> bytes:
        """The payload after a valid header."""
        self.validate(data)
        return data[self.header_size :]

    def validate(self, data: bytes) -> None:
        if len(data) < self.header_size:
            raise UnrecognizedStorageFormatError(
                f"{len(data)} bytes, shorter than the {self.header_size}-byte format header"
            )
        if data[:4] != self._magic:
            raise UnrecognizedStorageFormatError(f"magic {data[:4]!r}, expected {self._magic!r}")
        (version,) = _VERSION_STRUCT.unpack(data[4 : self.header_size])
        if version != self._version:
            raise UnrecognizedStorageFormatError(f"format version {version}, expected {self._version}")

    def is_torn_header(self, data: bytes) -> bool:
        """Whether ``data`` is a strict prefix of the header -- a crash
        while writing a new file's header, before anything followed it."""
        return len(data) < self.header_size and self._header.startswith(data)

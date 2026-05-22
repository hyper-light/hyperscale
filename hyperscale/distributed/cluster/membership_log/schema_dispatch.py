"""
SchemaDispatcher — (entry_type, schema_version) → encoder/decoder.

AD-52 §14 mandates versioned log entries so old followers can apply new
entries and vice versa within a two-major-release window. The dispatch
table is the single source of truth for which version pairs are known
and how to encode/decode each one.

The actual serialization is delegated to the existing AD-25 wire-protocol
machinery (msgspec). This module just owns the version registry.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class DispatchKey:
    entry_type: str
    schema_version: int


@dataclass(frozen=True, slots=True)
class DispatchEntry:
    encode: Callable[[Any], bytes]
    decode: Callable[[bytes], Any]


class SchemaDispatcher:
    """
    Mutable registry of (entry_type, schema_version) → encode/decode
    pairs. Looked up at apply time per AD-52 §14:

        encoded = dispatcher.encode(entry)
        decoded = dispatcher.decode("AddLearner", 1, payload_bytes)

    Unknown (entry_type, schema_version) pairs raise. The apply layer
    treats unknown-schema entries as "future schema we don't speak yet"
    and refuses to apply them — AD-52 §14 protocol-versioning interlock.
    """

    __slots__ = ("_by_key", "_default_versions")

    def __init__(self) -> None:
        self._by_key: dict[DispatchKey, DispatchEntry] = {}
        self._default_versions: dict[str, int] = {}

    def register(
        self,
        entry_type: str,
        schema_version: int,
        encode: Callable[[Any], bytes],
        decode: Callable[[bytes], Any],
        default: bool = False,
    ) -> None:
        """
        Register a (type, version) decoder. When default=True, this
        version becomes the writer's default for this entry_type.
        """
        if schema_version < 1:
            raise ValueError("schema_version must be >= 1")
        key = DispatchKey(entry_type=entry_type, schema_version=schema_version)
        self._by_key[key] = DispatchEntry(encode=encode, decode=decode)
        if default:
            self._default_versions[entry_type] = schema_version

    def encoder_for(self, entry_type: str, schema_version: int | None = None):
        """
        Encode-side lookup. If schema_version is None, the default writer
        version is used (registered via register(..., default=True)).
        """
        resolved_version = (
            schema_version
            if schema_version is not None
            else self._default_versions.get(entry_type)
        )
        if resolved_version is None:
            raise KeyError(
                f"no default schema_version registered for {entry_type!r}"
            )
        key = DispatchKey(entry_type=entry_type, schema_version=resolved_version)
        entry = self._by_key.get(key)
        if entry is None:
            raise KeyError(
                f"no encoder for ({entry_type!r}, schema_version={resolved_version})"
            )
        return entry.encode, resolved_version

    def decoder_for(self, entry_type: str, schema_version: int):
        key = DispatchKey(entry_type=entry_type, schema_version=schema_version)
        entry = self._by_key.get(key)
        if entry is None:
            raise KeyError(
                f"no decoder for ({entry_type!r}, schema_version={schema_version})"
            )
        return entry.decode

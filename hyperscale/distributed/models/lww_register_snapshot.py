"""Serialized form of an ``LWWRegister`` (``LWWRegister.to_dict``)."""

from __future__ import annotations

from typing import Generic, TypedDict, TypeVar

ValueT = TypeVar("ValueT")


class LWWRegisterSnapshot(TypedDict, Generic[ValueT]):
    """The value, Lamport timestamp and writer node id of an ``LWWRegister``."""

    value: ValueT | None
    timestamp: int
    node_id: str

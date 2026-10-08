"""Structured-logging record produced by ``SwimError.to_dict``."""

from typing import TypedDict


class SwimErrorRecord(TypedDict):
    """Fixed-shape dictionary form of a ``SwimError``."""

    error_type: str
    message: str
    category: str
    severity: str
    context: dict[str, object]
    cause: str | None

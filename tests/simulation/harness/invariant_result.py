"""Outcome of one continuous-invariant evaluation."""

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class InvariantResult:
    """Outcome of one invariant evaluation."""

    holds: bool
    detail: str = ""

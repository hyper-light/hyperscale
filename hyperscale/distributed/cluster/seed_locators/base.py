"""
SeedLocator — the protocol every concrete locator implements.

Designed as a Protocol so callers can compose locators without inheriting
from a shared base class (AD-1: composition over inheritance).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .resolved_address import ResolvedAddress


@runtime_checkable
class SeedLocator(Protocol):
    """
    A SeedLocator resolves a URI to zero or more ResolvedAddresses.
    Implementations MUST be safe to call resolve() concurrently from
    multiple tasks.

    Empty resolution is not an error (AD-52 §2: "the locator may produce
    results later"). Implementations should return [] rather than raise.

    refresh_required() returns True when the locator's underlying source
    has changed since the last resolve() (mtime bump for files, exec
    interval elapsed, etc.). The resolver consults this on every refresh
    tick to avoid unnecessary work when nothing has changed.
    """

    @property
    def uri(self) -> str:
        """The original URI string."""
        ...

    @property
    def scheme(self) -> str:
        """One of: 'tcp', 'dns', 'dns-srv', 'file', 'exec'."""
        ...

    async def resolve(self) -> list[ResolvedAddress]:
        """
        Resolve the locator. Empty list means "no addresses right now."
        MUST NOT raise on transient failures (DNS NXDOMAIN, file not
        present, exec failure) — those return []. SHOULD raise only on
        configuration-level errors (malformed URI, security violation,
        permissions error on exec://).
        """
        ...

    async def refresh_required(self) -> bool:
        """
        True if a fresh resolve() would yield different results than the
        last one. Default-true implementations are correct but cause
        wasted work; locators with cheap change-detection (mtime watch)
        should implement this precisely.
        """
        ...

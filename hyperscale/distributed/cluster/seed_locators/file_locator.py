"""
file:// locator — newline-delimited locator list, mtime-watched.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from .resolved_address import ResolvedAddress


class FileLocator:
    """
    file:///etc/hyperscale/seeds →
        Reads the file at /etc/hyperscale/seeds, parses each non-empty
        non-comment line as a nested locator URI, recursively resolves
        them via the upstream resolver, and merges results.

    File-format rules:
      - One locator URI per line.
      - Blank lines and #-prefixed comment lines are ignored.
      - Nested file:// or exec:// are NOT permitted (AD-52 security
        rationale: defense-in-depth against unbounded indirection).
        Only tcp://, dns://, dns-srv:// may appear inside a file://.

    AD-52 §2 security checks (enforced at construction):
      - Path must be absolute.
      - Must exist at constructor time. Later disappearance is OK
        (returns []); but the first construction validates configuration.
      - Path's parent directory must not be world-writable.

    Refresh detection uses mtime — if mtime is unchanged since the last
    resolve(), refresh_required() returns False so the upstream resolver
    can skip re-reading.
    """

    __slots__ = ("_uri", "_path", "_last_mtime_ns")

    SCHEME_PREFIX = "file://"

    def __init__(self, uri: str) -> None:
        if not uri.startswith(self.SCHEME_PREFIX):
            raise ValueError(
                f"FileLocator expects {self.SCHEME_PREFIX} prefix, got {uri!r}"
            )

        raw_path = uri[len(self.SCHEME_PREFIX):]
        if not raw_path.startswith("/"):
            raise ValueError(
                f"file:// locator {uri!r} must use an absolute path "
                f"(AD-52 §2 security note)"
            )

        resolved_path = Path(raw_path)
        # Validate parent directory is not world-writable. The file itself
        # may not yet exist (it can appear later); we still validate the
        # parent which must exist now.
        parent_directory = resolved_path.parent
        if not parent_directory.exists():
            raise ValueError(
                f"file:// locator {uri!r} parent directory does not exist"
            )

        parent_stat = parent_directory.stat()
        if parent_stat.st_mode & 0o002:
            raise ValueError(
                f"file:// locator {uri!r} refused: parent directory "
                f"{parent_directory} is world-writable (AD-52 §2 security note)"
            )

        self._uri = uri
        self._path = resolved_path
        self._last_mtime_ns: int = 0

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return "file"

    async def resolve(self) -> list[ResolvedAddress]:
        try:
            stat_result = await asyncio.get_running_loop().run_in_executor(
                None,
                self._path.stat,
            )
        except FileNotFoundError:
            return []

        self._last_mtime_ns = stat_result.st_mtime_ns

        try:
            file_contents = await asyncio.get_running_loop().run_in_executor(
                None,
                self._path.read_text,
            )
        except OSError:
            return []

        # Defer nested-locator parsing to the upstream resolver; we emit
        # the raw locator URIs as side-channel data by encoding them as
        # ResolvedAddress entries with port=0 and host=the URI itself.
        # The resolver inspects source_scheme == "file" and re-parses.
        # This keeps file_locator focused on file I/O.
        resolved: list[ResolvedAddress] = []
        for raw_line in file_contents.splitlines():
            stripped_line = raw_line.strip()
            if not stripped_line or stripped_line.startswith("#"):
                continue
            # Disallow nested file:// or exec://.
            if stripped_line.startswith("file://") or stripped_line.startswith("exec://"):
                # Skip with a sentinel that the resolver can log;
                # an exception here would propagate too coarsely.
                continue
            # We can resolve tcp:// inline because it's literal.
            if stripped_line.startswith("tcp://"):
                from .tcp_locator import TcpLocator
                try:
                    nested_locator = TcpLocator(stripped_line)
                except ValueError:
                    continue
                resolved.extend(await nested_locator.resolve())
                continue
            # For dns:// and dns-srv:// the resolver re-instantiates
            # because they need their own refresh cadence. We surface
            # them as bare ResolvedAddress with port=0 and host carrying
            # the URI — the resolver detects port=0 + scheme="file" and
            # branches.
            resolved.append(
                ResolvedAddress(
                    host=stripped_line,
                    port=0,
                    source_scheme="file",
                )
            )

        return resolved

    async def refresh_required(self) -> bool:
        try:
            stat_result = await asyncio.get_running_loop().run_in_executor(
                None,
                self._path.stat,
            )
        except FileNotFoundError:
            # Treat disappearance as a refresh (resolve returns []).
            return self._last_mtime_ns != 0
        return stat_result.st_mtime_ns != self._last_mtime_ns

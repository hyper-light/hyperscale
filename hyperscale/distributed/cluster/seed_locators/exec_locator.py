"""
exec:// locator — execute a command, parse stdout as a locator list.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import time

from .resolved_address import ResolvedAddress


class ExecLocator:
    """
    exec:///opt/get-seeds.sh →
        Executes /opt/get-seeds.sh, parses stdout (one locator URI per
        line), and emits them. Refresh is interval-bounded (default 60s).

    AD-52 §2 security checks (enforced at construction):
      - Path must be absolute (no shell expansion of $PATH).
      - The executable's parent directories must not be world-writable.
      - The executable itself must be owned by the process user.
      - No arguments are forwarded — only the bare path is invoked.
        Operators who need arguments wrap the binary in a small script.

    Like file://, nested file:// and exec:// in the output are rejected.
    """

    __slots__ = (
        "_uri",
        "_path",
        "_refresh_interval_seconds",
        "_last_resolve_monotonic",
    )

    SCHEME_PREFIX = "exec://"
    DEFAULT_REFRESH_INTERVAL_SECONDS: float = 60.0
    SUBPROCESS_TIMEOUT_SECONDS: float = 30.0

    def __init__(
        self,
        uri: str,
        refresh_interval_seconds: float | None = None,
    ) -> None:
        if not uri.startswith(self.SCHEME_PREFIX):
            raise ValueError(
                f"ExecLocator expects {self.SCHEME_PREFIX} prefix, got {uri!r}"
            )

        raw_path = uri[len(self.SCHEME_PREFIX):]
        if not raw_path.startswith("/"):
            raise ValueError(
                f"exec:// locator {uri!r} must use an absolute path "
                f"(AD-52 §2 security note)"
            )

        resolved_path = Path(raw_path)
        if not resolved_path.exists():
            raise ValueError(
                f"exec:// locator {uri!r} does not exist"
            )
        if not resolved_path.is_file():
            raise ValueError(
                f"exec:// locator {uri!r} is not a regular file"
            )

        file_stat = resolved_path.stat()
        if file_stat.st_uid != os.geteuid():
            raise ValueError(
                f"exec:// locator {uri!r} refused: not owned by the "
                f"hyperscale process user (uid={os.geteuid()}); "
                f"file owner uid={file_stat.st_uid}"
            )
        if file_stat.st_mode & 0o002:
            raise ValueError(
                f"exec:// locator {uri!r} refused: file is world-writable"
            )

        for parent_dir in resolved_path.parents:
            parent_stat = parent_dir.stat()
            if parent_stat.st_mode & 0o002:
                raise ValueError(
                    f"exec:// locator {uri!r} refused: parent directory "
                    f"{parent_dir} is world-writable (AD-52 §2 security note)"
                )
            if parent_dir == Path("/"):
                break

        self._uri = uri
        self._path = resolved_path
        self._refresh_interval_seconds = (
            refresh_interval_seconds
            if refresh_interval_seconds is not None
            else self.DEFAULT_REFRESH_INTERVAL_SECONDS
        )
        self._last_resolve_monotonic: float = 0.0

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def scheme(self) -> str:
        return "exec"

    async def resolve(self) -> list[ResolvedAddress]:
        # No arguments forwarded; exec the bare path. Stdin closed.
        try:
            subprocess_instance = await asyncio.create_subprocess_exec(
                str(self._path),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except (PermissionError, FileNotFoundError):
            return []

        try:
            stdout_bytes, _stderr_bytes = await asyncio.wait_for(
                subprocess_instance.communicate(),
                timeout=self.SUBPROCESS_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            subprocess_instance.kill()
            try:
                await subprocess_instance.wait()
            except Exception:
                pass
            return []

        self._last_resolve_monotonic = time.monotonic()

        if subprocess_instance.returncode != 0:
            # Non-zero exit means "no addresses right now" per AD-52 §2.
            return []

        stdout_text = stdout_bytes.decode("utf-8", errors="replace")
        resolved: list[ResolvedAddress] = []
        for raw_line in stdout_text.splitlines():
            stripped_line = raw_line.strip()
            if not stripped_line or stripped_line.startswith("#"):
                continue
            # Disallow nested exec:// and file:// — defense in depth.
            if stripped_line.startswith("exec://") or stripped_line.startswith("file://"):
                continue
            if stripped_line.startswith("tcp://"):
                from .tcp_locator import TcpLocator
                try:
                    nested_locator = TcpLocator(stripped_line)
                except ValueError:
                    continue
                resolved.extend(await nested_locator.resolve())
                continue
            # As with file://, surface non-literal nested locators as
            # raw URIs the resolver re-parses.
            resolved.append(
                ResolvedAddress(
                    host=stripped_line,
                    port=0,
                    source_scheme="exec",
                )
            )

        return resolved

    async def refresh_required(self) -> bool:
        if self._last_resolve_monotonic == 0.0:
            return True
        elapsed = time.monotonic() - self._last_resolve_monotonic
        return elapsed >= self._refresh_interval_seconds

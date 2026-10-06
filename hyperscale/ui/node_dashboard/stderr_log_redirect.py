import asyncio
import io
import os
import pathlib
import sys

# The process's standard error descriptor (POSIX and the Windows CRT alike).
STDERR_DESCRIPTOR = 2


def open_log_file(log_path: pathlib.Path) -> io.FileIO:
    """Open ``log_path`` for appending, creating its directory if missing."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return open(log_path, "ab", buffering=0)


class StderrLogRedirect:
    """While a node dashboard renders, the process's standard error goes to
    the node's log file instead of the terminal.

    A node logs to stderr, and so do its executor processes, which inherit
    the descriptor: on the dashboard's screen every such line would tear a
    frame. The redirect moves descriptor 2 itself -- not just
    ``sys.stderr`` -- so the node's log streams (which duplicate descriptor
    2 when they open), its children and any direct write all land in the
    file. Enter it before anything that logs is opened; on exit stderr is
    the terminal again, so an error the command ends with is printed there.

    When ``enabled`` is false (no dashboard renders) it does nothing:
    stderr stays where it is.
    """

    def __init__(self, log_path: pathlib.Path, enabled: bool) -> None:
        self._log_path = log_path
        self._enabled = enabled
        self._log_file: io.FileIO | None = None
        self._saved_stderr_descriptor: int | None = None

    async def __aenter__(self) -> "StderrLogRedirect":
        if not self._enabled:
            return self

        loop = asyncio.get_running_loop()
        self._log_file = await loop.run_in_executor(None, open_log_file, self._log_path)
        sys.stderr.flush()
        self._saved_stderr_descriptor = os.dup(STDERR_DESCRIPTOR)
        os.dup2(self._log_file.fileno(), STDERR_DESCRIPTOR)
        return self

    async def __aexit__(self, *exception_info: object) -> None:
        if self._saved_stderr_descriptor is None:
            return

        sys.stderr.flush()
        os.dup2(self._saved_stderr_descriptor, STDERR_DESCRIPTOR)
        os.close(self._saved_stderr_descriptor)
        self._saved_stderr_descriptor = None
        await asyncio.get_running_loop().run_in_executor(None, self._log_file.close)

"""
A process whose standard stream is redirected to a regular file still logs.

The logger attached its stdout/stderr writer with asyncio's pipe
transport, which accepts only pipes, sockets and character devices --
so ``hyperscale ... 2> run.log``, or a test runner's file-backed capture,
failed logger setup with "Pipe transport is only for pipes, sockets and
character devices" and took the caller (a workflow run) down with it.

Driven as a real child process with its standard streams redirected to
a regular file, logging through the real LoggerStream: every entry
reaches the file, in order, and the process exits cleanly. The same
script over a pipe proves the pipe path is unchanged.
"""

import subprocess
import sys
from pathlib import Path

ENTRY_COUNT = 50

CHILD_SCRIPT = f"""
import asyncio

from hyperscale.logging.config.logging_config import LoggingConfig
from hyperscale.logging.models import Entry, LogLevel
from hyperscale.logging.streams.logger_stream import LoggerStream


async def main() -> None:
    LoggingConfig().update(log_level="info")
    stream = LoggerStream(name="redirected")
    await stream.initialize()
    for entry_index in range({ENTRY_COUNT}):
        await stream.log(Entry(message=f"entry-{{entry_index}}", level=LogLevel.INFO), template="{{message}}")
    await stream.close()


asyncio.run(main())
"""


def expected_lines() -> list[str]:
    return [f"entry-{entry_index}" for entry_index in range(ENTRY_COUNT)]


def logged_lines(output: str) -> list[str]:
    return [line for line in output.splitlines() if line.startswith("entry-")]


def test_logging_to_a_stream_redirected_to_a_regular_file(tmp_path: Path) -> None:
    output_path = tmp_path / "run.log"
    with output_path.open("wb") as output_file:
        completed = subprocess.run(
            [sys.executable, "-c", CHILD_SCRIPT],
            stdout=output_file,
            stderr=output_file,
            timeout=60,
        )

    output = output_path.read_text()
    assert completed.returncode == 0, output
    assert logged_lines(output) == expected_lines()


def test_logging_to_a_pipe_is_unchanged() -> None:
    completed = subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60,
    )

    output = completed.stdout.decode()
    assert completed.returncode == 0, output
    assert logged_lines(output) == expected_lines()

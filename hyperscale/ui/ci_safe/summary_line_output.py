"""Writing the CI-safe form of a terminal UI: append-only lines of plain
ASCII, shared by the node dashboards and a run's progress."""

import os
from typing import BinaryIO

# What a summary line becomes on the wire: plain ASCII whatever the locale,
# a character outside it written as "?".
SUMMARY_ENCODING = "ascii"


def write_summary_line(output: BinaryIO, line: bytes) -> None:
    """Write and flush one summary line. A reader that has gone away
    (EPIPE) leaves the output pointed at the null device, so the process's
    own final flush of it cannot fail as well -- the remedy the Python
    documentation gives for SIGPIPE (library/signal, "Note on SIGPIPE") --
    and the error is raised for the caller to report."""
    try:
        output.write(line)
        output.flush()

    except BrokenPipeError:
        null_device = os.open(os.devnull, os.O_WRONLY)
        os.dup2(null_device, output.fileno())
        os.close(null_device)
        raise


def format_duration(total_seconds: float) -> str:
    """``total_seconds`` as hours, minutes and whole seconds: ``1h02m03s``."""
    minutes, seconds = divmod(int(total_seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{seconds:02d}s"

"""
The terminal UI with stdout redirected to a regular file.

asyncio's pipe transport accepts only pipes, sockets and character devices,
so a run whose stdout was a file (``hyperscale run workflow -o ci > run.log``)
failed: "Pipe transport is only for pipes, sockets and character devices".

* a regular file gets the buffered off-loop writer, a pipe keeps the pipe
  transport;
* everything written before close reaches the file once the writer is
  closed and waited on.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.logging.streams.regular_file_stream_writer import RegularFileStreamWriter
from hyperscale.ui.components.terminal.terminal import Terminal
from hyperscale.ui.components.terminal.writer import Writer


async def test_a_regular_file_gets_the_file_writer_and_a_pipe_keeps_the_transport(tmp_path: Path) -> None:
    terminal = Terminal.__new__(Terminal)
    terminal._loop = asyncio.get_running_loop()

    with open(tmp_path / "run.log", "w") as regular_file:
        assert isinstance(await terminal._create_writer(regular_file), RegularFileStreamWriter)

    read_end, write_end = os.pipe()
    pipe_file = os.fdopen(write_end, "w")
    try:
        pipe_writer = await terminal._create_writer(pipe_file)
        assert isinstance(pipe_writer, Writer)
        pipe_writer.close()
        await pipe_writer.wait_closed()

    finally:
        os.close(read_end)


async def test_what_was_written_before_close_reaches_the_file(tmp_path: Path) -> None:
    log_path = tmp_path / "run.log"
    with open(log_path, "wb") as log_file:
        writer = RegularFileStreamWriter(log_file.fileno(), asyncio.get_running_loop())
        writer.write(b"first frame\n")
        await writer.drain()
        writer.write(b"final frame\n")
        writer.close()
        await writer.wait_closed()

    assert log_path.read_bytes() == b"first frame\nfinal frame\n"

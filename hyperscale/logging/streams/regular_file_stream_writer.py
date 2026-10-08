import asyncio
import os


class RegularFileStreamWriter:
    """
    The stream writer for a standard stream redirected to a regular file.

    asyncio's pipe transport accepts only pipes, sockets and character
    devices, so a process whose stdout or stderr is a file (``> run.log``,
    a test runner's capture file) could not attach its logger at all.
    Writes are buffered and drained off the event loop -- a regular file
    write blocks -- in the order they were written.
    """

    __slots__ = ("_file_descriptor", "_loop", "_buffer", "_drain_lock", "_closing")

    def __init__(self, file_descriptor: int, loop: asyncio.AbstractEventLoop) -> None:
        self._file_descriptor = file_descriptor
        self._loop = loop
        self._buffer = bytearray()
        self._drain_lock = asyncio.Lock()
        self._closing = False

    def write(self, data: bytes) -> None:
        """Buffer ``data``; ``drain()`` writes it."""
        if self._closing:
            raise RuntimeError("write to a closed log stream")
        self._buffer.extend(data)

    async def drain(self) -> None:
        """Write everything buffered so far to the file."""
        async with self._drain_lock:
            if not self._buffer:
                return
            pending = bytes(self._buffer)
            self._buffer.clear()
            await self._loop.run_in_executor(None, self._write_all, pending)

    def _write_all(self, data: bytes) -> None:
        written_bytes = 0
        while written_bytes < len(data):
            written_bytes += os.write(self._file_descriptor, data[written_bytes:])

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._closing = True

    async def wait_closed(self) -> None:
        """Write what was buffered before ``close()``, as a closed transport
        flushes before its process may exit."""
        await self.drain()

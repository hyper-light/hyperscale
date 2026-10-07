import asyncio


class TerminalProtocol(asyncio.Protocol):
    """The terminal's write pipe protocol, with flow control.

    A terminal that reads slower than frames are written (a busy emulator,
    a pty whose reader lags) fills the pipe: the transport pauses the
    protocol, and ``Writer.drain`` waits until it resumes rather than
    letting frames pile up in the transport's buffer. Its close waiter
    completes once the transport has flushed and closed, so a terminal
    closed before its process exits loses none of what it wrote (the
    cursor it shows again, last of all).
    """

    def __init__(self) -> None:
        self._can_write = asyncio.Event()
        self._can_write.set()
        self._closed: asyncio.Future[None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._closed = asyncio.get_running_loop().create_future()

    def pause_writing(self) -> None:
        self._can_write.clear()

    def resume_writing(self) -> None:
        self._can_write.set()

    def connection_lost(self, exc: Exception | None) -> None:
        # A lost connection never resumes: release any writer waiting on it.
        self._can_write.set()
        if self._closed is not None and not self._closed.done():
            self._closed.set_result(None)

    async def _drain_helper(self) -> None:
        await self._can_write.wait()

    def _get_close_waiter(self, stream: object) -> asyncio.Future[None]:
        return self._closed


def patch_transport_close(
    transport: asyncio.Transport,
    loop: asyncio.AbstractEventLoop,
):
    def close(*args, **kwargs):
        try:
            transport.close()

        except Exception:
            pass

    return close

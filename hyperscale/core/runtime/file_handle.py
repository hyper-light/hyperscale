from typing import Protocol


class FileHandle(Protocol):
    """The subset of an open binary file the write paths consume.

    All IO methods are async: a buffered ``write`` can trigger a
    flush-to-OS, ``flush``/``close`` always can, and reads always
    touch the device — none of them may run on the event loop.
    ``closed`` is pure in-memory state and stays synchronous.
    """

    @property
    def closed(self) -> bool: ...

    def tell(self) -> int: ...

    def close_sync(self) -> None:
        """Synchronous close for emergency teardown paths ONLY (e.g. a
        logger ``abort()`` running outside any event loop). May block
        briefly in the real implementation; everything else must use
        ``close``."""
        ...

    async def write(self, data: bytes) -> int: ...

    async def read(self, size: int = -1) -> bytes: ...

    async def readline(self) -> bytes: ...

    async def seek(self, offset: int, whence: int = 0) -> int: ...

    async def flush(self) -> None: ...

    async def close(self) -> None: ...

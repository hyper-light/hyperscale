class RealFileHandle:
    """A stdlib file object with its IO dispatched off-loop.

    Only ``RealFilesystem`` constructs these; ``RealFilesystem.fsync``
    relies on the extra ``fileno()`` accessor beyond the ``FileHandle``
    Protocol surface, and each handle dispatches through its owning
    filesystem's dedicated executor.
    """

    __slots__ = ("_file", "_run")

    def __init__(self, file, run) -> None:
        self._file = file
        self._run = run

    @property
    def closed(self) -> bool:
        return self._file.closed

    def fileno(self) -> int:
        return self._file.fileno()

    def tell(self) -> int:
        return self._file.tell()

    def close_sync(self) -> None:
        self._file.close()

    async def write(self, data: bytes) -> int:
        return await self._run(self._file.write, data)

    async def read(self, size: int = -1) -> bytes:
        return await self._run(self._file.read, size)

    async def readline(self) -> bytes:
        return await self._run(self._file.readline)

    async def seek(self, offset: int, whence: int = 0) -> int:
        return await self._run(self._file.seek, offset, whence)

    async def flush(self) -> None:
        await self._run(self._file.flush)

    async def close(self) -> None:
        await self._run(self._file.close)


from typing import (
    Self,
    Type,
    Any,
    AsyncIterator,
)
from .request_ranges import request_ranges
from .types import SFTPFileObj

class LocalFile:
    """An async wrapper around local file I/O"""

    def __init__(self, file: SFTPFileObj):
        self._file = file

    async def __aenter__(self) -> Self: # pragma: no cover
        """Allow LocalFile to be used as an async context manager"""

        return self

    async def __aexit__(
        self,
        _exc_type: Type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: Any,
    ) -> bool:
        """Wait for file close when used as an async context manager"""

        await self.close()
        return False

    def request_ranges(self, offset: int, length: int) -> AsyncIterator[tuple[int, int]]:
        """Return data ranges containing data in a local file"""

        return request_ranges(self._file, offset, length)

    async def read(self, size: int, offset: int) -> bytes:
        """Read data from the local file"""

        self._file.seek(offset)
        return self._file.read(size)

    async def write(self, data: bytes, offset: int) -> int:
        """Write data to the local file"""

        self._file.seek(offset)
        return self._file.write(data)

    async def close(self) -> None:
        """Close the local file"""

        self._file.close()
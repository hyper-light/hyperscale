from typing import Protocol, AsyncIterator, Self, Type
from .types import TracebackType


class SFTPFileProtocol(Protocol):
    """Protocol for accessing a file via an SFTP server"""

    async def __aenter__(self) -> Self:
        """Allow SFTPFileProtocol to be used as an async context manager"""

    async def __aexit__(self, _exc_type: Type[BaseException] | None,
                        _exc_value: BaseException| None,
                        _traceback: TracebackType | None) -> bool:
        """Wait for file close when used as an async context manager"""

    def request_ranges(self, offset: int, length: int) -> \
            AsyncIterator[tuple[int, int]]:
        """Return file ranges containing data"""

    async def read(self, size: int, offset: int) -> bytes:
        """Read data from the local file"""

    async def write(self, data: bytes, offset: int) -> int:
        """Write data to the local file"""

    async def close(self) -> None:
        """Close the local file"""
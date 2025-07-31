from typing import Protocol, AnyStr



class AsyncFileProtocol(Protocol[AnyStr]):
    """Protocol for an async file"""

    async def read(self, n: int = -1) -> AnyStr:
        """Read from an async file"""

    async def write(self, data: AnyStr) -> None:
        """Write to an async file"""

    async def close(self) -> None:
        """Close an async file"""
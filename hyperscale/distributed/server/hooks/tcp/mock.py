from abc import ABC, abstractmethod
from typing import TypeVar


T = TypeVar("T")


class TCPServer(ABC):
    
    @abstractmethod
    async def send_tcp(
        self,
        addr: tuple[str, int],
        target: str,
        res: T,
        tmeout: int | float | None = None
    ):
        pass

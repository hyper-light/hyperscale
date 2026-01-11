from abc import ABC, abstractmethod
from typing import TypeVar


T = TypeVar("T")


class UDPServer(ABC):
    
    @abstractmethod
    async def send_udp(
        self,
        addr: tuple[str, int],
        target: str,
        res: T,
        tmeout: int | float | None = None
    ):
        pass

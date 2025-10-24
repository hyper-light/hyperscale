import asyncio
from typing import TypeVar, Generic


T = TypeVar("T")


class ClientState(Generic[T]):
    """
    Shared servers state that is available between all protocol instances.
    """

    def __init__(self) -> None:
        self.total_requests = 0
        self.connections: set[T] = set()
        self.tasks: set[asyncio.Task[None]] = set()
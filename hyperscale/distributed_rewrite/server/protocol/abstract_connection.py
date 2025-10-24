import asyncio
from abc import ABC, abstractmethod
from .receive_buffer import ReceiveBuffer


class AbstractConnection(ABC):
    waiting_for_data: asyncio.Event

    @abstractmethod
    def read_client_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass

    @abstractmethod
    def read_server_udp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass
    
    @abstractmethod
    def read_client_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass

    @abstractmethod
    def read_server_tcp(
        self,
        data: bytes,
        transport: asyncio.Transport,
        next_data: asyncio.Future,
    ):
        pass
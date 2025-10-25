import asyncio
from abc import ABC, abstractmethod
from .receive_buffer import ReceiveBuffer


class AbstractConnection(ABC):
    tcp_client_waiting_for_data: asyncio.Event
    tcp_server_waiting_for_data: asyncio.Event
    udp_client_waiting_for_data: asyncio.Event
    udp_server_waiting_for_data: asyncio.Event

    @abstractmethod
    def read_udp(
        self,
        data: ReceiveBuffer,
        transport: asyncio.Transport,
    ):
        pass
    
    @abstractmethod
    def read_client_tcp(
        self,
        data: ReceiveBuffer,
        transport: asyncio.Transport,
    ):
        pass

    @abstractmethod
    def read_server_tcp(
        self,
        data: ReceiveBuffer,
        transport: asyncio.Transport,
    ):
        pass
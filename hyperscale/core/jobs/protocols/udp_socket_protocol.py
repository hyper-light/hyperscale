import asyncio
from typing import Callable, Tuple


class UDPSocketProtocol(asyncio.DatagramProtocol):
    def __init__(self, callback: Callable[[bytes, Tuple[str, int]], bytes]):
        super().__init__()
        self.callback = callback

    def connection_made(self, transport) -> str:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        # Here is where you would push message to whatever methods/classes you want.
        # data: Message = pickle.loads(lzma.decompress(unpacked))
        self.callback(data, addr)

    def connection_lost(self, exc: Exception | None) -> None:
        pass

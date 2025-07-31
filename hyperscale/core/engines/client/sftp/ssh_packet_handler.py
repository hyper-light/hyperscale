from typing import Mapping, Awaitable, Callable, Any
from .packet import SSHPacket
from .types import MaybeAwait


PacketHandler = Callable[[Any, int, int, 'SSHPacket'], MaybeAwait[None]]


class SSHPacketHandler:
    """Parent class for SSH packet handlers"""

    _packet_handlers: Mapping[int, PacketHandler] = {}

    def process_packet(self, pkttype: int, pktid: int,
                       packet: SSHPacket) -> bool | Awaitable[None]:
        """Log and process a received packet"""

        if pkttype in self._packet_handlers:
            return self._packet_handlers[pkttype](self, pkttype,
                                                  pktid, packet) or True
        else:
            return False

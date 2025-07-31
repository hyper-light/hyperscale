from typing import Generic, AnyStr, Callable, TYPE_CHECKING
from .packet import String, UInt32
from .ssh_forward_channel import SSHForwardChannel

if TYPE_CHECKING:
    from .ssh_tcp_session import SSHTCPSession



SSHTCPSessionFactory = Callable[[], SSHTCPSession[AnyStr]]


class SSHTCPChannel(SSHForwardChannel, Generic[AnyStr]):
    """SSH TCP channel"""

    async def _open_tcp(self, session_factory: SSHTCPSessionFactory[AnyStr],
                        chantype: bytes, host: str, port: int, orig_host: str,
                        orig_port: int) -> SSHTCPSession[AnyStr]:
        """Open a TCP channel"""

        self.set_extra_info(peername=('', 0),
                            local_peername=(orig_host, orig_port),
                            remote_peername=(host, port))

        return await self._open_forward(session_factory, chantype,
                                             String(host), UInt32(port),
                                             String(orig_host),
                                             UInt32(orig_port))

    async def connect(self, session_factory: SSHTCPSessionFactory[AnyStr],
                     host: str, port: int, orig_host: str, orig_port: int) -> \
            SSHTCPSession[AnyStr]:
        """Create a new outbound TCP session"""

        return await self._open_tcp(session_factory, b'direct-tcpip',
                                    host, port, orig_host, orig_port)

    async def accept(self, session_factory: SSHTCPSessionFactory[AnyStr],
                     host: str, port: int, orig_host: str,
                     orig_port: int) -> SSHTCPSession[AnyStr]:
        """Create a new forwarded TCP session"""

        return await self._open_tcp(session_factory, b'forwarded-tcpip',
                                    host, port, orig_host, orig_port)

    def set_inbound_peer_names(self, dest_host: str, dest_port: int,
                               orig_host: str, orig_port: int) -> None:
        """Set local and remote peer names for inbound connections"""

        self.set_extra_info(peername=('', 0),
                            local_peername=(dest_host, dest_port),
                            remote_peername=(orig_host, orig_port))

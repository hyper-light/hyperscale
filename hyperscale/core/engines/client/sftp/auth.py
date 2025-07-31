import asyncio
from typing import Awaitable, TYPE_CHECKING
from .ssh_public_key import SigningKey
from .ssh_packet_handler import SSHPacketHandler


if TYPE_CHECKING:
    from .ssh_connection import SSHConnection


class Auth(SSHPacketHandler):
    """Parent class for authentication"""

    def __init__(self, conn: SSHConnection, coro: Awaitable[None]):
        self._conn = conn
        self._coro: asyncio.Task[None] | None = conn.create_task(coro)

    def send_packet(self, pkttype: int, *args: bytes,
                    trivial: bool = True) -> None:
        """Send an auth packet"""

        self._conn.send_userauth_packet(pkttype, *args, handler=self,
                                        trivial=trivial)


    def create_task(self, coro: Awaitable[None]) -> None:
        """Create an asynchronous auth task"""

        self.cancel()
        self._coro = self._conn.create_task(coro)

    def cancel(self) -> None:
        """Cancel any authentication in progress"""

        if self._coro: # pragma: no branch
            self._coro.cancel()
            self._coro = None


class ClientAuth(Auth):
    """Parent class for client authentication"""

    _conn: 'SSHClientConnection'

    def __init__(self, conn: 'SSHClientConnection', method: bytes):
        self._method = method

        super().__init__(conn, self._start())

    async def _start(self) -> None:
        """Abstract method for starting client authentication"""

        # Provided by subclass
        raise NotImplementedError

    def auth_succeeded(self) -> None:
        """Callback when auth succeeds"""

    def auth_failed(self) -> None:
        """Callback when auth fails"""

    async def send_request(self, *args: bytes,
                           key: SigningKey | None = None,
                           trivial: bool = True) -> None:
        """Send a user authentication request"""

        await self._conn.send_userauth_request(self._method, *args, key=key,
                                               trivial=trivial)

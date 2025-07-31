from typing import Self, Type, TYPE_CHECKING
from .types import TracebackType


if TYPE_CHECKING:
    from .ssh_connection import SSHConnection


class SSHListener:
    """SSH listener for inbound connections"""

    def __init__(self) -> None:
        self._tunnel: SSHConnection | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, _exc_type: Type[BaseException],
                        _exc_value: BaseException,
                        _traceback: TracebackType) -> bool:
        self.close()
        await self.wait_closed()
        return False

    def get_port(self) -> int:
        """Return the port number being listened on

           This method returns the port number that the remote listener
           was bound to. When the requested remote listening port is `0`
           to indicate a dynamic port, this method can be called to
           determine what listening port was selected. This function
           only applies to TCP listeners.

           :returns: The port number being listened on

        """

        # pylint: disable=no-self-use

        return 0

    def set_tunnel(self, tunnel: SSHConnection) -> None:
        """Set tunnel associated with listener"""

        self._tunnel = tunnel

    def close(self) -> None:
        """Stop listening for new connections

           This method can be called to stop listening for connections.
           Existing connections will remain open.

        """

        if self._tunnel:
            self._tunnel.close()

    async def wait_closed(self) -> None:
        """Wait for the listener to close

           This method is a coroutine which waits for the associated
           listeners to be closed.

        """

        if self._tunnel:
            await self._tunnel.wait_closed()
            self._tunnel = None

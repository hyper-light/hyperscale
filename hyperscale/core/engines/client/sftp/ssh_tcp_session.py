from typing import Any, AnyStr
from .ssh_session import SSHSession


class SSHTCPSession(SSHSession[AnyStr]):
    """SSH TCP session handler

       Applications should subclass this when implementing a handler for
       SSH direct or forwarded TCP connections.

       SSH client applications wishing to open a direct connection should call
       :meth:`create_connection() <SSHClientConnection.create_connection>`
       on their :class:`SSHClientConnection`, passing in a factory which
       returns instances of this class.

       Server applications wishing to allow direct connections should
       implement the coroutine :meth:`connection_requested()
       <SSHServer.connection_requested>` on their :class:`SSHServer`
       object and have it return instances of this class.

       Server applications wishing to allow connection forwarding back
       to the client should implement the coroutine :meth:`server_requested()
       <SSHServer.server_requested>` on their :class:`SSHServer` object
       and call :meth:`create_connection()
       <SSHServerConnection.create_connection>` on their
       :class:`SSHServerConnection` for each new connection, passing it a
       factory which returns instances of this class.

       When a connection is successfully opened, :meth:`session_started`
       will be called, after which the application can begin sending data.
       Received data will be passed to the :meth:`data_received` method.

    """

    def connection_made(self, chan: Any) -> None:
        """Called when a channel is opened successfully

           This method is called when a channel is opened successfully. The
           channel parameter should be stored if needed for later use.

           :param chan:
               The channel which was successfully opened.
           :type chan: :class:`SSHTCPChannel`

        """

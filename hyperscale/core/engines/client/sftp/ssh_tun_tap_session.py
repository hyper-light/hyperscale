from typing import Any
from .ssh_session import SSHSession



class SSHTunTapSession(SSHSession[bytes]):
    """SSH TUN/TAP session handler

       Applications should subclass this when implementing a handler for
       SSH TUN/TAP tunnels.

       SSH client applications wishing to open a tunnel should call
       :meth:`create_tun() <SSHClientConnection.create_tun>` or
       :meth:`create_tap() <SSHClientConnection.create_tap>` on their
       :class:`SSHClientConnection`, passing in a factory which returns
       instances of this class.

       Server applications wishing to allow tunnel connections should
       implement the coroutine :meth:`tun_requested()
       <SSHServer.tun_requested>` or :meth:`tap_requested()
       <SSHServer.tap_requested>` on their :class:`SSHServer` object
       and have it return instances of this class.

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
           :type chan: :class:`SSHTunTapChannel`

        """
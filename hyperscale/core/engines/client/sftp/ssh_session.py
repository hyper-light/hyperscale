from typing import Generic, TypeVar, AnyStr, Any
from .types import DataType


T = TypeVar('T')

class SSHSession(Generic[AnyStr]):
    """SSH session handler"""

    # pylint: disable=no-self-use,unused-argument

    def connection_made(self, chan: Any) -> None:
        """Called when a channel is opened successfully"""

    def connection_lost(self, exc: Exception | None) -> None:
        """Called when a channel is closed

           This method is called when a channel is closed. If the channel
           is shut down cleanly, *exc* will be `None`. Otherwise, it
           will be an exception explaining the reason for the channel close.

           :param exc:
               The exception which caused the channel to close, or
               `None` if the channel closed cleanly.
           :type exc: :class:`Exception`

        """

    def session_started(self) -> None:
        """Called when the session is started

           This method is called when a session has started up. For
           client and server sessions, this will be called once a
           shell, exec, or subsystem request has been successfully
           completed. For TCP and UNIX domain socket sessions, it will
           be called immediately after the connection is opened.

        """

    def data_received(self, data: AnyStr, datatype: DataType) -> None:
        """Called when data is received on the channel

           This method is called when data is received on the channel.
           If an encoding was specified when the channel was created,
           the data will be delivered as a string after decoding with
           the requested encoding. Otherwise, the data will be delivered
           as bytes.

           :param data:
               The data received on the channel
           :param datatype:
               The extended data type of the data, from :ref:`extended
               data types <ExtendedDataTypes>`
           :type data: `str` or `bytes`

        """

    def eof_received(self) -> bool:
        """Called when EOF is received on the channel

           This method is called when an end-of-file indication is received
           on the channel, after which no more data will be received. If this
           method returns `True`, the channel remains half open and data
           may still be sent. Otherwise, the channel is automatically closed
           after this method returns. This is the default behavior for
           classes derived directly from :class:`SSHSession`, but not when
           using the higher-level streams API. Because input is buffered
           in that case, streaming sessions enable half-open channels to
           allow applications to respond to input read after an end-of-file
           indication is received.

        """

        return False # pragma: no cover

    def pause_writing(self) -> None:
        """Called when the write buffer becomes full

           This method is called when the channel's write buffer becomes
           full and no more data can be sent until the remote system
           adjusts its window. While data can still be buffered locally,
           applications may wish to stop producing new data until the
           write buffer has drained.

        """

    def resume_writing(self) -> None:
        """Called when the write buffer has sufficiently drained

           This method is called when the channel's send window reopens
           and enough data has drained from the write buffer to allow the
           application to produce more data.

        """
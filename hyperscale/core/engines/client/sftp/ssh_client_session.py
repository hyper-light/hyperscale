
from typing import AnyStr, Any
from .ssh_session import SSHSession


class SSHClientSession(SSHSession[AnyStr]):
    """SSH client session handler

       Applications should subclass this when implementing an SSH client
       session handler. The functions listed below should be implemented
       to define application-specific behavior. In particular, the standard
       `asyncio` protocol methods such as :meth:`connection_made`,
       :meth:`connection_lost`, :meth:`data_received`, :meth:`eof_received`,
       :meth:`pause_writing`, and :meth:`resume_writing` are all supported.
       In addition, :meth:`session_started` is called as soon as the SSH
       session is fully started, :meth:`xon_xoff_requested` can be used to
       determine if the server wants the client to support XON/XOFF flow
       control, and :meth:`exit_status_received` and
       :meth:`exit_signal_received` can be used to receive session exit
       information.

    """

    # pylint: disable=no-self-use,unused-argument

    def connection_made(self, chan: Any) -> None:
        """Called when a channel is opened successfully

           This method is called when a channel is opened successfully. The
           channel parameter should be stored if needed for later use.

           :param chan:
               The channel which was successfully opened.
           :type chan: :class:`SSHClientChannel`

        """

    def xon_xoff_requested(self, client_can_do: bool) -> None:
        """XON/XOFF flow control has been enabled or disabled

           This method is called to notify the client whether or not
           to enable XON/XOFF flow control. If client_can_do is
           `True` and output is being sent to an interactive
           terminal the application should allow input of Control-S
           and Control-Q to pause and resume output, respectively.
           If client_can_do is `False`, Control-S and Control-Q
           should be treated as normal input and passed through to
           the server. Non-interactive applications can ignore this
           request.

           By default, this message is ignored.

           :param client_can_do:
               Whether or not to enable XON/XOFF flow control
           :type client_can_do: `bool`

        """

    def exit_status_received(self, status: int) -> None:
        """A remote exit status has been received for this session

           This method is called when the shell, command, or subsystem
           running on the server terminates and returns an exit status.
           A zero exit status generally means that the operation was
           successful. This call will generally be followed by a call
           to :meth:`connection_lost`.

           By default, the exit status is ignored.

           :param status:
               The exit status returned by the remote process
           :type status: `int`

        """

    def exit_signal_received(self, signal: str, core_dumped: bool,
                             msg: str, lang: str) -> None:
        """A remote exit signal has been received for this session

           This method is called when the shell, command, or subsystem
           running on the server terminates abnormally with a signal.
           A more detailed error may also be provided, along with an
           indication of whether the remote process dumped core. This call
           will generally be followed by a call to :meth:`connection_lost`.

           By default, exit signals are ignored.

           :param signal:
               The signal which caused the remote process to exit
           :param core_dumped:
               Whether or not the remote process dumped core
           :param msg:
               Details about what error occurred
           :param lang:
               The language the error message is in
           :type signal: `str`
           :type core_dumped: `bool`
           :type msg: `str`
           :type lang: `str`

        """
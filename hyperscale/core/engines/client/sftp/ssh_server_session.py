from typing import AnyStr, Any, Mapping
from .ssh_session import SSHSession


class SSHServerSession(SSHSession[AnyStr]):
    """SSH server session handler

       Applications should subclass this when implementing an SSH server
       session handler. The functions listed below should be implemented
       to define application-specific behavior. In particular, the
       standard `asyncio` protocol methods such as :meth:`connection_made`,
       :meth:`connection_lost`, :meth:`data_received`, :meth:`eof_received`,
       :meth:`pause_writing`, and :meth:`resume_writing` are all supported.
       In addition, :meth:`pty_requested` is called when the client requests a
       pseudo-terminal, one of :meth:`shell_requested`, :meth:`exec_requested`,
       or :meth:`subsystem_requested` is called depending on what type of
       session the client wants to start, :meth:`session_started` is called
       once the SSH session is fully started, :meth:`terminal_size_changed` is
       called when the client's terminal size changes, :meth:`signal_received`
       is called when the client sends a signal, and :meth:`break_received`
       is called when the client sends a break.

    """

    # pylint: disable=no-self-use,unused-argument

    def connection_made(self, chan: Any) -> None:
        """Called when a channel is opened successfully

           This method is called when a channel is opened successfully. The
           channel parameter should be stored if needed for later use.

           :param chan:
               The channel which was successfully opened.
           :type chan: :class:`SSHServerChannel`

        """

    def pty_requested(self, term_type: str,
                      term_size: tuple[int, int, int, int],
                      term_modes: Mapping[int, int]) -> bool:
        """A pseudo-terminal has been requested

           This method is called when the client sends a request to allocate
           a pseudo-terminal with the requested terminal type, size, and
           POSIX terminal modes. This method should return `True` if the
           request for the pseudo-terminal is accepted. Otherwise, it should
           return `False` to reject the request.

           By default, requests to allocate a pseudo-terminal are accepted
           but nothing is done with the associated terminal information.
           Applications wishing to use this information should implement
           this method and have it return `True`, or call
           :meth:`get_terminal_type() <SSHServerChannel.get_terminal_type>`,
           :meth:`get_terminal_size() <SSHServerChannel.get_terminal_size>`,
           or :meth:`get_terminal_mode() <SSHServerChannel.get_terminal_mode>`
           on the :class:`SSHServerChannel` to get the information they need
           after a shell, command, or subsystem is started.

           :param term_type:
               Terminal type to set for this session
           :param term_size:
               Terminal size to set for this session provided as a
               tuple of four `int` values: the width and height of the
               terminal in characters followed by the width and height
               of the terminal in pixels
           :param term_modes:
               POSIX terminal modes to set for this session, where keys
               are taken from :ref:`POSIX terminal modes <PTYModes>` with
               values defined in section 8 of :rfc:`RFC 4254 <4254#section-8>`.
           :type term_type: `str`
           :type term_size: tuple of 4 `int` values
           :type term_modes: `dict`

           :returns: A `bool` indicating if the request for a
                     pseudo-terminal was allowed or not

        """

        return True # pragma: no cover

    def terminal_size_changed(self, width: int, height: int,
                              pixwidth: int, pixheight: int) -> None:
        """The terminal size has changed

           This method is called when a client requests a
           pseudo-terminal and again whenever the the size of
           he client's terminal window changes.

           By default, this information is ignored, but applications
           wishing to use the terminal size can implement this method
           to get notified whenever it changes.

           :param width:
               The width of the terminal in characters
           :param height:
               The height of the terminal in characters
           :param pixwidth: (optional)
               The width of the terminal in pixels
           :param pixheight: (optional)
               The height of the terminal in pixels
           :type width: `int`
           :type height: `int`
           :type pixwidth: `int`
           :type pixheight: `int`

        """

    def shell_requested(self) -> bool:
        """The client has requested a shell

           This method should be implemented by the application to
           perform whatever processing is required when a client makes
           a request to open an interactive shell. It should return
           `True` to accept the request, or `False` to reject it.

           If the application returns `True`, the :meth:`session_started`
           method will be called once the channel is fully open. No output
           should be sent until this method is called.

           By default this method returns `False` to reject all requests.

           :returns: A `bool` indicating if the shell request was
                     allowed or not

        """

        return False # pragma: no cover

    def exec_requested(self, command: str) -> bool:
        """The client has requested to execute a command

           This method should be implemented by the application to
           perform whatever processing is required when a client makes
           a request to execute a command. It should return `True` to
           accept the request, or `False` to reject it.

           If the application returns `True`, the :meth:`session_started`
           method will be called once the channel is fully open. No output
           should be sent until this method is called.

           By default this method returns `False` to reject all requests.

           :param command:
               The command the client has requested to execute
           :type command: `str`

           :returns: A `bool` indicating if the exec request was
                     allowed or not

        """

        return False # pragma: no cover

    def subsystem_requested(self, subsystem: str) -> bool:
        """The client has requested to start a subsystem

           This method should be implemented by the application to
           perform whatever processing is required when a client makes
           a request to start a subsystem. It should return `True` to
           accept the request, or `False` to reject it.

           If the application returns `True`, the :meth:`session_started`
           method will be called once the channel is fully open. No output
           should be sent until this method is called.

           By default this method returns `False` to reject all requests.

           :param subsystem:
               The subsystem to start
           :type subsystem: `str`

           :returns: A `bool` indicating if the request to open the
                     subsystem was allowed or not

        """

        return False # pragma: no cover

    def break_received(self, msec: int) -> bool:
        """The client has sent a break

           This method is called when the client requests that the
           server perform a break operation on the terminal. If the
           break is performed, this method should return `True`.
           Otherwise, it should return `False`.

           By default, this method returns `False` indicating that
           no break was performed.

           :param msec:
               The duration of the break in milliseconds
           :type msec: `int`

           :returns: A `bool` to indicate if the break operation was
                     performed or not

        """

        return False # pragma: no cover

    def signal_received(self, signal: str) -> None:
        """The client has sent a signal

           This method is called when the client delivers a signal
           on the channel.

           By default, signals from the client are ignored.

           :param signal:
               The name of the signal received
           :type signal: `str`

        """

    def soft_eof_received(self) -> None:
        """The client has sent a soft EOF

           This method is called by the line editor when the client
           send a soft EOF (Ctrl-D on an empty input line).

           By default, soft EOF will trigger an EOF to an outstanding
           read call but still allow additional input to be received
           from the client after that.

        """

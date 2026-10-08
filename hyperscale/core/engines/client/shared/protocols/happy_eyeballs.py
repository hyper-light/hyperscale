import asyncio
import socket
from typing import List, Sequence, Tuple

# RFC 8305 section 8: the recommended Connection Attempt Delay when no
# round-trip history is available.
CONNECTION_ATTEMPT_DELAY_SECONDS = 0.25

SocketConfig = Tuple[int, int, int, str, Tuple]


def _open_socket(socket_config: SocketConfig) -> socket.socket:
    family, _, proto, _, _ = socket_config

    connecting_socket = socket.socket(family, socket.SOCK_STREAM, proto)

    try:
        connecting_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connecting_socket.setblocking(False)

    except BaseException:
        connecting_socket.close()
        raise

    return connecting_socket


async def connect_first_responding(
    loop: asyncio.AbstractEventLoop,
    socket_configs: Sequence[SocketConfig],
    attempt_delay: float = CONNECTION_ATTEMPT_DELAY_SECONDS,
) -> Tuple[socket.socket, int]:
    """
    Connect a TCP socket to whichever of ``socket_configs`` answers first.

    Attempts start in order. The next one starts as soon as the current
    one fails, or once it has not connected within ``attempt_delay``;
    attempts already started keep running (RFC 8305 section 5). The first
    to connect wins, and every other attempt is cancelled and its socket
    closed before this returns.

    Returns the connected, non-blocking socket and the index of its
    config. If every attempt fails, raises the error.
    """
    if not socket_configs:
        raise ConnectionError("No addresses to connect to")

    if len(socket_configs) == 1:
        # One address: nothing to race.
        connecting_socket = _open_socket(socket_configs[0])

        try:
            await loop.sock_connect(connecting_socket, socket_configs[0][4])

        except BaseException:
            connecting_socket.close()
            raise

        return connecting_socket, 0

    winner: asyncio.Future = loop.create_future()
    attempts: List[asyncio.Task] = []
    errors: List[Exception] = []
    connected: Tuple[socket.socket, int] | None = None

    async def attempt(index: int, socket_config: SocketConfig):
        connecting_socket = _open_socket(socket_config)

        try:
            await loop.sock_connect(connecting_socket, socket_config[4])

        except asyncio.CancelledError:
            connecting_socket.close()
            raise

        except Exception as error:
            connecting_socket.close()
            errors.append(error)
            return

        if winner.done():
            # Another attempt connected first.
            connecting_socket.close()
            return

        winner.set_result((connecting_socket, index))

    try:
        last_index = len(socket_configs) - 1

        for index, socket_config in enumerate(socket_configs):
            current_attempt = loop.create_task(attempt(index, socket_config))
            attempts.append(current_attempt)

            if index < last_index:
                await asyncio.wait(
                    (winner, current_attempt),
                    timeout=attempt_delay,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if winner.done():
                    break

        while winner.done() is False and (
            running := [task for task in attempts if task.done() is False]
        ):
            await asyncio.wait(
                (winner, *running),
                return_when=asyncio.FIRST_COMPLETED,
            )

        if winner.done():
            connected = winner.result()
            return connected

        if len(errors) == 1:
            raise errors[0]

        raise OSError(
            f"Multiple exceptions: {', '.join(str(error) for error in errors)}"
        )

    finally:
        for task in attempts:
            if task.done() is False:
                task.cancel()

        await asyncio.gather(*attempts, return_exceptions=True)

        if connected is None and winner.done():
            # Cancelled after an attempt won: that socket belongs to no one.
            winner.result()[0].close()

        # A raised error's traceback holds this frame (and each attempt's
        # frame, through its closure): release their hold on the errors.
        errors = None

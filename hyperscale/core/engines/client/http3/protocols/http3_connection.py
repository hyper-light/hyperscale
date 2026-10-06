from __future__ import annotations

import asyncio
from ssl import SSLContext
from typing import Iterator, Optional, Sequence, Tuple

from .quic_protocol import QuicProtocol
from .udp_connection import UDPConnection


class HTTP3Connection:
    def __init__(self, reset_connections: bool = False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.protocol: Optional[QuicProtocol] = None
        self.server_name: Optional[str] = None
        self.connected = False
        # The scheme and address (as requested) the open connection serves.
        self.target: Tuple[str, str] | None = None
        self.reset_connections = reset_connections
        self.pending = 0
        self._connection_factory = UDPConnection()

    async def make_connection(
        self,
        dns_address: str,
        port: int,
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        server_name: str = None,
        timeout: Optional[float] = None,
        ssl: Optional[SSLContext] = None,
    ) -> None:
        if (
            self.connected is False
            or self.dns_address != dns_address
            or self.reset_connections
        ):
            try:
                self.protocol = await asyncio.wait_for(
                    self._connection_factory.create_http3(
                        socket_config=socket_config, 
                        server_name=server_name,
                        ssl=ssl,
                    ),
                    timeout=timeout,
                )

                self.connected = True

                self.dns_address = dns_address
                self.port = port

            except asyncio.TimeoutError:
                raise Exception("Connection timed out.")

            except ConnectionResetError:
                raise Exception("Connection reset.")

            except Exception as e:
                raise e

    async def connect_to_any(
        self,
        target: Tuple[str, str],
        addresses: Sequence[Tuple[str, Tuple[int, int, int, str, Tuple[str, int]]]],
        port: int,
        address_rotation: Iterator[int],
        server_name: str = None,
        timeout: Optional[float] = None,
        ssl: Optional[SSLContext] = None,
    ) -> Tuple[Optional[str], Optional[Tuple[int, int, int, str, Tuple[str, int]]], bool]:
        """
        Reuse this connection's open QUIC connection to ``target``, the
        scheme and address the request names. Otherwise open a new one,
        trying the host's ``addresses`` one at a time from the next offset
        in ``address_rotation`` so a pool's connections spread across all
        of them; the first to complete its handshake wins.

        ``ssl`` is the engine's TLS context: its verify mode decides whether
        a new connection checks the server's certificate.

        Returns the address and socket config of a new connection (``None``
        for both on reuse), and whether the connection is new.
        """
        # A QUIC connection that has begun closing -- the server closed it,
        # it idled out, or it failed -- carries no new request: it is
        # replaced, not reused.
        if (
            self.connected
            and self.reset_connections is False
            and self.target == target
            and self.protocol.quic._close_event is None
        ):
            return None, None, False

        if self.connected:
            # Close the open connection first: make_connection keeps it when
            # the new address shares its IP (another port's server would
            # answer), and replacing it would leave its socket open.
            self.reset()

        if not addresses:
            raise ConnectionError(f"No addresses to connect to for {server_name}")

        offset = next(address_rotation) % len(addresses)
        connection_error: Exception | None = None

        for address, socket_config in (*addresses[offset:], *addresses[:offset]):
            try:
                await self.make_connection(
                    address,
                    port,
                    socket_config,
                    server_name=server_name,
                    timeout=timeout,
                    ssl=ssl,
                )

            except Exception as err:
                # Close this attempt's socket before trying the next address.
                connection_error = err
                self.reset()
                continue

            self.server_name = server_name
            self.target = target

            return address, socket_config, True

        try:
            raise connection_error

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    def close(self):
        if self.protocol:
            try:
                self.protocol.close()

            except Exception:
                pass

            self.protocol = None

        self._connection_factory.close()

    def reset(self):
        self.connected = False
        self.target = None

        if self.protocol:
            try:
                self.protocol.close()

            except Exception:
                pass

            self.protocol = None

        self._connection_factory.reset()

from __future__ import annotations

import asyncio
import ssl
from typing import Iterator, Optional, Sequence, Tuple

from hyperscale.core.engines.client.shared.protocols import (
    Reader,
    Writer,
)
from hyperscale.core.engines.client.shared.protocols.happy_eyeballs import SocketConfig

from .dtls import do_patch
from .udp import UDPConnection as UDP

do_patch()


class UDPConnection:
    def __init__(self, reset_connections: bool = False) -> None:
        self.dns_address: str = None
        self.port: int = None
        self.ip_addr = None
        self.lock = asyncio.Lock()

        self.reader: Reader = None
        self.writer: Writer = None

        self.connected = False
        self.reset_connections = reset_connections
        self.pending = 0
        self._connection_factory = UDP()

    async def make_connection(
        self,
        dns_address: str,
        port: int,
        socket_config: Tuple[int, int, int, int, Tuple[int, int]],
        tls: Optional[ssl.SSLContext] = None,
    ) -> None:
        if (
            self.connected is False
            or self.dns_address != dns_address
            or self.reset_connections
        ):
            try:
                reader, writer = await self._connection_factory.create_udp(
                    socket_config, tls=tls
                )

                self.connected = True

                self.reader = reader
                self.writer = writer

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
        addresses: Sequence[Tuple[str, SocketConfig]],
        port: int,
        address_rotation: Iterator[int],
        tls: Optional[ssl.SSLContext] = None,
    ) -> Tuple[str, SocketConfig, bool]:
        """
        Reuse this connection's socket when it already targets one of the
        host's ``addresses`` on ``port``. Otherwise open a new one from the
        next offset in ``address_rotation``, so a pool's connections spread
        across all of the addresses, falling over to the next address when
        one fails. A UDP connect has no handshake to race: it only fixes
        the peer.

        Returns the address and socket config used, and whether the socket
        is new.
        """
        if self.connected and self.reset_connections is False and self.port == port:
            for address, socket_config in addresses:
                if address == self.dns_address:
                    return address, socket_config, False

        if not addresses:
            raise ConnectionError("No addresses to connect to")

        if self.connected:
            # The socket targets a different host: close it first.
            self.reset()

        offset = next(address_rotation) % len(addresses)
        connection_error: Optional[Exception] = None

        for address, socket_config in [*addresses[offset:], *addresses[:offset]]:
            try:
                reader, writer = await self._connection_factory.create_udp(
                    socket_config, tls=tls
                )

            except Exception as error:
                # Close this attempt's socket before trying the next address.
                connection_error = error
                self._connection_factory.reset()
                continue

            self.reader = reader
            self.writer = writer

            self.connected = True
            self.dns_address = address
            self.port = port

            return address, socket_config, True

        try:
            raise connection_error

        finally:
            # The error's traceback holds this frame: release the frame's
            # hold on the error, or the two keep each other alive as garbage.
            connection_error = None

    def close(self):
        if self.reader:
            self.reader = None

        if self.writer:
            self.writer.clear()

        try:
            self._connection_factory.close()
        except Exception:
            pass

    def reset(self):
        self.connected = False

        if self.reader:
            self.reader = None

        if self.writer:
            self.writer.clear()

        self._connection_factory.reset()

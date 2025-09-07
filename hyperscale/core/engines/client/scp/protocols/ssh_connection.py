import asyncio
import pathlib
from typing import Any

from hyperscale.core.engines.client.ssh.protocol.connection import (
    TunnelConnectorProtocol,
    open_tunnel,
    open_proxy,
    canonicalize_host,
    SSHClientConnection,
    SSHClientConnectionOptions,
)



class SSHConnection:

    def __init__(self):
        self.connected: bool = False
        self.connection: SSHClientConnection | None = None

        self.lock = asyncio.Lock()
        self._path: str| pathlib.Path | None = None
        self._command: str | None = None
        self._loop = asyncio.get_event_loop()
        self._options: SSHClientConnectionOptions | None = None


    async def connect(
        self,
        socket_config: tuple[str | int | tuple[str, int ], ...],
        config: tuple[str, ...] = (),
        **kwargs: dict[str, Any],
    ) -> SSHClientConnection:
        
        family, _, _, _, address = socket_config
        host, port = address
        
        options = SSHClientConnectionOptions(
            options=None,
            **kwargs,
        )

        options.waiter = self._loop.create_future()

        canon_host = await canonicalize_host(self._loop, options)

        host = canon_host if canon_host else host
        canonical = bool(canon_host)
        final = options.config.has_match_final()

        if canonical or final:
            options.update(host=host, reload=True, canonical=canonical, final=final)

        tunnel: TunnelConnectorProtocol | None = options.tunnel
        family = options.family
        local_addr = options.local_addr
        proxy_command = options.proxy_command
        free_conn = True


        new_tunnel = await open_tunnel(tunnel, options, config)

        try:
            
            if new_tunnel:
                # pylint: disable=broad-except
                try:
                    _, tunnel_session = await new_tunnel.create_connection(
                        lambda: SSHClientConnection(
                            self._loop,
                            options,
                            wait='auth',
                        ),
                        host, port)
                except Exception:
                    new_tunnel.close()
                    await new_tunnel.wait_closed()
                    raise
                else:
                    conn: SSHClientConnection = tunnel_session
                    conn.set_tunnel(new_tunnel)
            elif tunnel:

                _, tunnel_session = await tunnel.create_connection(
                    lambda: SSHClientConnection(
                        self._loop,
                        options,
                        wait='auth',
                    ),
                    host, port)

                conn: SSHClientConnection = tunnel_session
            elif proxy_command:
                conn = await open_proxy(
                    self._loop,
                    proxy_command, 
                    lambda: SSHClientConnection(
                        self._loop,
                        options,
                        wait='auth',
                    ),
                    )
            else:

                _, session = await self._loop.create_connection(
                    lambda: SSHClientConnection(
                        self._loop,
                        options,
                        wait='auth',
                    ),
                    host,
                    port,
                    family=family,
                    flags=0,
                    local_addr=local_addr,
                )

                conn: SSHClientConnection = session
        except asyncio.CancelledError:
            options.waiter.cancel()
            raise

        conn.set_extra_info(host=host, port=port)

        try:
            await options.waiter
            free_conn = False
            return conn
        finally:
            if free_conn:
                conn.abort()
                await conn.wait_closed()

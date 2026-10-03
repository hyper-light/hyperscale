"""
The GraphQL engine completes requests.

``_execute`` unpacked four values from the HTTP engine's
``_connect_to_url_location``, which returns five (the trace span), so
every GraphQL request failed with "too many values to unpack" -- and
left the connection it had already opened behind, open after close().

Driven through the real engine against a local HTTP server that counts
its open connections.
"""

import asyncio
import json

from hyperscale.core.engines.client.graphql import MercurySyncGraphQLConnection
from hyperscale.core.engines.client.setup_clients import setup_client

VUS = 2
REQUEST_COUNT = 3
BODY = json.dumps({"data": {"ok": True}}).encode()


class GraphQLServer:
    """Answers each request with a GraphQL JSON body."""

    def __init__(self) -> None:
        self.open_connections: set[asyncio.StreamWriter] = set()

    async def answer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.open_connections.add(writer)
        try:
            while await reader.readuntil(b"\r\n\r\n"):
                writer.write(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    + f"Content-Length: {len(BODY)}\r\n\r\n".encode()
                    + BODY
                )
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self.open_connections.discard(writer)
            writer.close()


async def wait_until_closed(server: GraphQLServer) -> None:
    """Closed sockets are observed by the server on its next read."""
    for _ in range(100):
        if not server.open_connections:
            return
        await asyncio.sleep(0.01)


async def serve(server: GraphQLServer) -> tuple[asyncio.Server, str]:
    listener = await asyncio.start_server(server.answer, "127.0.0.1", 0)
    return listener, f"http://127.0.0.1:{listener.sockets[0].getsockname()[1]}/graphql"


async def test_queries_succeed_and_close_leaves_no_connection_open() -> None:
    server = GraphQLServer()
    listener, url = await serve(server)
    engine = setup_client(MercurySyncGraphQLConnection(), VUS)
    try:
        for _ in range(REQUEST_COUNT):
            response = await engine.query(url, "query { ok }")
            assert response.status == 200, response.status_message
            assert json.loads(response.content) == {"data": {"ok": True}}

        engine.close()
        await wait_until_closed(server)
        assert not server.open_connections
    finally:
        listener.close()
        listener.close_clients()
        await listener.wait_closed()


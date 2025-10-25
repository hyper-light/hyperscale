import asyncio
import msgspec
from pydantic import BaseModel, StrictStr
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer


class Request(msgspec.Struct):
    data: str


class Response(msgspec.Struct):
    data: str


async def test(message: Request) -> Response:
    return Response(data=message.data)


async def run():
    server = MercurySyncBaseServer(
        '127.0.0.1',
        8667,
        8668,
        Env(),
    )

    server.tcp_server_request_models[b'test'] = Request
    server.udp_server_request_models[b'test'] = Request
    server.tcp_handlers[b'test'] = test
    server.udp_handlers[b'test'] = test

    await server.start_server()

    loop = asyncio.get_event_loop()
    waiter = loop.create_future()

    await waiter

    await server.shutdown()


asyncio.run(run())
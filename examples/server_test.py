import asyncio
import msgspec
from pydantic import BaseModel, StrictStr
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer


class Request(msgspec.Struct):
    data: str


class Response(msgspec.Struct):
    data: str



class TestServer(MercurySyncBaseServer):

    @udp.client()
    async def send(self, message: Request) -> Response:
        return Response(data=message.data)
    
    @udp.server()
    async def test(self, message: Request) -> Response:
        return Response(data=message.data)

async def run():
    server = TestServer(
        '127.0.0.1',
        8667,
        8668,
        Env(),
    )

    await server.start_server()
    
    loop = asyncio.get_event_loop()
    waiter = loop.create_future()

    await waiter

    await server.shutdown()


asyncio.run(run())
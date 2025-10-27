import asyncio
import time
import msgspec
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.models import Message
from hyperscale.distributed_rewrite.server import tcp, udp
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer

class Request(msgspec.Struct):
    data: str


class Response(msgspec.Struct):
    data: str



class TestServer(MercurySyncBaseServer):

    @udp.client()
    async def send(self, message: Request) -> Message[Response]:
        return await self.send_udp_with_message(
            ('127.0.0.1', 8668),
            message,
        )
    
    @udp.server()
    async def test(self, message: Message[Request]) -> Response:
        return Response(data=message.data.data)

async def run():
    server = TestServer(
        '127.0.0.1',
        8669,
        8670,
        Env(),
    )

    await server.start_server()

    resp = await server.send(Request(data='hello!'))

    print(resp)

    # start = time.monotonic()

    # idx = 0
    
    # while time.monotonic() - start < 60:
        

    #     idx += 1

    #     await asyncio.sleep(0)

    # print('Completed: ', idx)

    await server.shutdown()


asyncio.run(run())
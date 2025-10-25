import asyncio
import time
import msgspec
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.server.server.mercury_sync_base_server import MercurySyncBaseServer


class Request(msgspec.Struct):
    data: str


class Response(msgspec.Struct):
    data: str


async def run():
    server = MercurySyncBaseServer(
        '127.0.0.1',
        8669,
        8670,
        Env(),
    )

    server.tcp_client_response_models[b'test'] = Response
    server.udp_client_response_models[b'test'] = Response
    await server.start_server()

    start = time.monotonic()

    idx = 0
    
    while time.monotonic() - start < 60:
        resp = await server.send_udp_client_data(
            b'test',
            ('127.0.0.1', 8668),
            Request(
                data='Hello!',
            )
        )

        idx += 1

        await asyncio.sleep(0)

    print('Completed: ', idx)

    await server.shutdown()


asyncio.run(run())
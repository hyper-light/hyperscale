import asyncio
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.server.protocol import ReceiveBuffer

from .mercury_sync_base_server import MercurySyncBaseServer


class MercurySyncServer(MercurySyncBaseServer):


    def __init__(
        self,
        host: str,
        tcp_port: int,
        udp_port: int,
        env: Env,
    ):
        super().__init__(
            host,
            tcp_port,
            udp_port,
            env,
        )
    
    async def process_tcp_server_call(
        self,
        data: ReceiveBuffer,
        transport: asyncio.Transport,
        next_data: asyncio.Future
    ):
        pass

    async def process_udp_server_call(
        self,
        data: ReceiveBuffer,
        transport: asyncio.Transport,
        next_data: asyncio.Future
    ):
        pass
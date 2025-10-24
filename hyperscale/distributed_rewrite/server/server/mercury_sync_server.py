from hyperscale.distributed_rewrite.env import Env

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

    def read_client_tcp(self, data, transport, next_data):
        return super().read_client_tcp(data, transport, next_data)
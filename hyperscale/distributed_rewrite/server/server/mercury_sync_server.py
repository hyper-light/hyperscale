import random
from typing import TypeVar
from hyperscale.distributed_rewrite.env import Env
from hyperscale.distributed_rewrite.models import (
    Ack,
    Confirm,
    Eject,
    Join,
    Leave,
    Message,
    Nack,
    Probe,
)

from hyperscale.distributed_rewrite.server import tcp, udp
from .mercury_sync_base_server import MercurySyncBaseServer


T = TypeVar("T", bin)


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

    def select_udp_node_subset(self):
        required = random.randrange(1, len(self._udp_client_addrs))
        return random.choices(self._udp_client_addrs, k=required)

    @udp.client()
    async def send_ack(
        self,
        ack: Ack,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            random.choice(self._udp_client_addrs),
            ack,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_ack(self, ack: Message[Ack]) -> Ack:
        return Ack(
            node=(self._host, self._udp_port),
        )
    
    @udp.client()
    async def send_confirm(
        self,
        addr: tuple[str, int],
        confirm: Confirm,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            addr,
            confirm,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_confirm(self, confirm: Message[Confirm]) -> Ack:
        return Ack(
            node=(self._host, self._udp_port),
        )

    @udp.client()
    async def send_join(
        self,
        addr: tuple[str, int],
        join: Join,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            addr,
            join,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_join(self, join: Message[Join]) -> Ack:
        return Ack(
            node=(self._host, self._udp_port),
        )
    
    @udp.client()
    async def send_eject(
        self,
        addr: tuple[str, int],
        eject: Eject,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            addr,
            eject,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_eject(self, eject: Message[Eject]) -> Message[Ack]:
        return Ack(
            node=(self._host, self._udp_port),
        )
    
    @udp.client()
    async def send_leave(
        self,
        addr: tuple[str, int],
        leave: Leave,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            addr,
            leave,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_leave(self, leave: Message[Leave]) -> Ack:
        return Ack(
            node=(self._host, self._udp_port),
        )
    
    @udp.client()
    async def send_nack(
        self, 
        addr: tuple[str, int], 
        nack: Nack,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            addr,
            nack,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_nack(self, nack: Message[Nack]) -> Ack:
        return Ack(
            node=(self._host, self._udp_port),
        )
    
    @udp.client()
    async def send_probe(
        self,
        addr: tuple[str, int],
        probe: Probe,
        timeout: int | float | None = None,
    ) -> Message[Ack]:
        return await self.send_udp_with_message(
            addr,
            probe,
            timeout=timeout,
        )
    
    @udp.server()
    async def ack_probe(self, probe: Message[Probe]) -> Ack:
        return Ack(
            node=(self._host, self._udp_port),
        )
    
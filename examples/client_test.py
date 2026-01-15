import asyncio
import msgspec
from collections import defaultdict
from typing import Literal
from pydantic import BaseModel, StrictStr
from hyperscale.distributed.env import Env
from hyperscale.distributed.server import tcp, udp, task
from hyperscale.distributed.server.server.mercury_sync_base_server import MercurySyncBaseServer

Message = Literal[b'ack', b'nack', b'join', b'leave', b'probe']
Status = Literal[b'JOIN', b'OK', b'SUSPECT', b'DEAD']

Nodes = dict[tuple[str, int], set[tuple[int, Status]]]
Ctx = dict[Literal['nodes'], Nodes]


class TestServer(MercurySyncBaseServer[Ctx]):


    def get_other_nodes(self, node: tuple[str, int]):
        target_host, target_port = node
        nodes: Nodes = self._context.read('nodes')

        return [
            (
                host,
                port,
            ) for host, port in list(nodes.keys()) if target_host != host and target_port != port
        ]
    
    async def send_if_ok(
        self,
        node: tuple[str, int],
        message: bytes,
    ):
        timeout = await self._context.read('current_timeout')
        _, status = node[-1]
        if status == b'OK':
            self._tasks.run(
                self.send,
                node,
                message,
                timeout=timeout,
            )


    async def poll_node(self, target: tuple[str, int]):
        status: Status = await self._context.read_with_lock(target)
        while self._running and status == b'OK':
            await self.send_if_ok(
                target, 
                b'ack>' + target,
            )

            await asyncio.sleep(
                self._context.read('udp_poll_interval', 1)
            )

            status = await self._context.read_with_lock(target)

    async def increase_failure_detector(self):
        max_timeout = self._context.read('max_probe_timeout')
        await self._context.update_with_lock(
            'current_timeout',
            lambda timeout: min(max_timeout, timeout + 1),
        )

    async def decrease_failure_detector(self):
        min_timeout = self._context.read('min_probe_timeout')
        await self._context.update_with_lock(
            'current_timeout',
            lambda timeout: max(min_timeout, timeout - 1),
        )

    @udp.send('receive')
    async def send(
        self,
        addr: tuple[str, int],
        message: bytes,
        timeout: int | None = None,
    ) -> bytes:
        return (
            addr,
            message,
            timeout,
        )
    
    @udp.handle('receive')
    async def handle(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        return data

    
    @udp.receive()
    async def receive(
        self,
        addr: tuple[str, int],
        data: Message,
        clock_time: int,
    ) -> bytes:
        try:

            parsed = data.split(b'>', maxsplit=1)
            message = data

            target: tuple[str, int] | None = None
            target_addr: bytes | None = None
            source_addr = f'{addr[0]}:{addr[1]}'
            if len(parsed) > 1:
                message, target_addr = parsed
                host, port = target_addr.decode().split(':', maxsplit=1)
                target = (host, int(port))

            match message:
                case b'ack' | b'nack':

                    if target not in nodes:
                        await self.increase_failure_detector()
                        return b'nack>' + self._udp_addr_slug

                    return b'ack>' + self._udp_addr_slug
                
                case b'join':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'ack' + b'>' + self._udp_addr_slug
                        
                        self._context.write(target, b'OK')

                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'OK'))
                        # self._task_runner.run(
                        #     self.poll_node,
                        #     target,
                        # )

                        return b'ack>' + self._udp_addr_slug

                case b'leave':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            return b'leave>' + self._udp_addr_slug
                        

                        if target not in nodes:
                            await self.increase_failure_detector()
                            return b'nack>' + self._udp_addr_slug
                        
                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])

                        nodes[target].put_nowait((clock_time, b'DEAD'))
                        self._context.write('nodes', nodes)

                        return b'ack>' + self._udp_addr_slug
                
                case b'probe':
                    async with self._context.with_value(target):
                        nodes: Nodes = self._context.read('nodes')

                        if self.udp_target_is_self(target):
                            # Refute
                            await self.increase_failure_detector()
                            return b'ack>' + self._udp_addr_slug
                        
                        if target not in nodes:
                            # We missed something
                            return b'nack>' + self._udp_addr_slug
                        
                        timeout = await self._context.read('current_timeout')

                        # Tell the suspect node to forward an ack.
                        self._tasks.run(
                            self.send,
                            target,
                            b'ack>' + source_addr.encode(),
                            timeout=timeout,
                        )
                        
                        # Broadcast the suspicion
                        others = self.get_other_nodes(target)
                        await asyncio.gather(*[
                            self.send_if_ok(
                                node,
                                message + b'>' + target_addr,

                            ) for node in others
                        ])
                            
                        return b'ack'
                    
                case _:
                    return b'nack'
                
        except Exception:
            import traceback
            print(traceback.format_exc())


async def run():
    server = TestServer(
        '127.0.0.1',
        8669,
        8670,
        Env(),
    )

    await server.start_server()

    idx = 0

    host, port = server.udp_address

    message = f'join>{host}:{port}'.encode()
    
    resp = await server.send(
        ('127.0.0.1', 8668),
        message,
    )

    print(resp)

    loop = asyncio.get_event_loop()
    waiter = loop.create_future()

    await waiter


    print('Completed: ', idx)

    print(server.udp_time)

    await server.shutdown()


asyncio.run(run())
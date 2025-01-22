import asyncio
import ctypes
import functools
import multiprocessing
import signal
import socket
import warnings
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.context import SpawnContext
from typing import Dict, List


from hyperscale.core.jobs.graphs.remote_graph_controller import (
    RemoteGraphController,
)
from hyperscale.core.jobs.models import Env
from hyperscale.logging import Logger, Entry, LogLevel


def set_process_name():
    try:
        libc = ctypes.CDLL("libc.so.6")
        progname = ctypes.c_char_p.in_dll(
            libc, "__progname_full"
        )  # refer to the source code of glibc

        new_name = b"hyperscale"
        # for `ps` command:
        # Environment variables are already copied to the Python program zone.
        # We can get environment variables by using `os.environ`,
        # hence we can ignore both reallocation and movement.
        libc.strcpy(progname, ctypes.c_char_p(new_name))
        # for `top` command and `/proc/self/comm`:
        buff = ctypes.create_string_buffer(len(new_name) + 1)
        buff.value = new_name
        libc.prctl(15, ctypes.byref(buff), 0, 0, 0)

    except Exception:
        pass

    except OSError:
        pass


def abort_server(server: RemoteGraphController):
    try:
        server.abort()

    except Exception:
        pass

    except asyncio.CancelledError:
        pass


async def run_server(
    leader_address: tuple[str, int],
    server: RemoteGraphController,
    worker_socket: socket.socket,
    cert_path: str | None = None,
    key_path: str | None = None,
):
    try:
        await server.start_server(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=worker_socket,
        )

        try:
            await server.connect_client(leader_address)
            await server.acknowledge_start(leader_address)

        except Exception:
            pass

        await server.run_forever()
        await server.close()

    except Exception:
        server.abort()

    except KeyboardInterrupt:
        server.abort()

    await server.wait_for_socket_shutdown()


def run_thread(
    leader_address: tuple[str, int],
    socket: socket.socket,
    worker_env: Dict[str, str | int | float | bool | None],
    cert_path: str | None = None,
    key_path: str | None = None,
):
    try:
        import uvloop

        uvloop.install()

    except ImportError:
        pass

    import asyncio

    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    host, port = socket.getsockname()

    env = Env(**worker_env)

    server = RemoteGraphController(host, port, env)

    for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
        loop.add_signal_handler(
            getattr(
                signal,
                signame,
            ),
            lambda signame=signame: server.abort(),
        )

    try:
        loop.run_until_complete(
            run_server(
                leader_address,
                server,
                socket,
                cert_path=cert_path,
                key_path=key_path,
            )
        )

    except Exception:
        abort_server(server)

    except asyncio.CancelledError:
        abort_server(server)

    except KeyboardInterrupt:
        abort_server(server)

    try:
        loop.close()

    except Exception:
        pass


class LocalServerPool:
    def __init__(
        self,
        pool_size: int,
    ) -> None:
        self._pool_size = pool_size
        self._context: SpawnContext | None = None
        self._executor: ProcessPoolExecutor | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._pool_task: asyncio.Task | None = None
        self._run_future: asyncio.Future | None = None
        self._logger = Logger()

    async def setup(self):
        self._context = multiprocessing.get_context("spawn")
        self._executor = ProcessPoolExecutor(
            max_workers=self._pool_size,
            mp_context=self._context,
            initializer=set_process_name,
        )

        async with self._logger.context(
            name='local_server_pool',
            path='hyperscale.main.log.json',
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            
            await ctx.log(Entry(
                message='Creating interrupt handlers for local server pool',
                level=LogLevel.TRACE,
            ))

            self._loop = asyncio.get_event_loop()

            for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
                self._loop.add_signal_handler(
                    getattr(
                        signal,
                        signame,
                    ),
                    self.abort,
                )

            await ctx.log(Entry(
                message='Created interrupt handlers for local server pool',
                level=LogLevel.TRACE,
            ))

    async def run_pool(
        self,
        leader_address: tuple[str, int],
        sockets: List[socket.socket],
        env: Env,
        cert_path: str | None = None,
        key_path: str | None = None,
    ):
        async with self._logger.context(
            name='local_server_pool',
            path='hyperscale.main.log.json',
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            
            leader_host, leader_port = leader_address
            
            await ctx.log(Entry(
                message=f'Creating server pool with {self._pool_size} workers and leader at {leader_host}:{leader_port}',
                level=LogLevel.DEBUG
            ))

            self._pool_task = asyncio.gather(
                *[
                    self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            run_thread,
                            leader_address,
                            socket,
                            env.model_dump(),
                            cert_path=cert_path,
                            key_path=key_path,
                        ),
                    )
                    for socket in sockets
                ],
                return_exceptions=True,
            )

    async def shutdown(self):

        async with self._logger.context(
            name='local_server_pool',
            path='hyperscale.main.log.json',
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            
            await ctx.log(Entry(
                message='Server pool received shutdown request',
                level=LogLevel.DEBUG
            ))

            try:
                self._pool_task.set_result(None)

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

            except asyncio.InvalidStateError:
                pass

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")

                await self._loop.run_in_executor(
                    None,
                    functools.partial(
                        self._executor.shutdown,
                        wait=True,
                        cancel_futures=True,
                    ),
                )

            await ctx.log(Entry(
                message='Server pool successfully shutdown',
                level=LogLevel.DEBUG,
            ))

    def abort(self):
        try:
            self._pool_task.set_result(None)

        except Exception:
            pass

        except asyncio.CancelledError:
            pass

        except asyncio.InvalidStateError:
            pass

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            self._executor.shutdown()

import asyncio
import functools
import multiprocessing
import signal
import socket
import warnings
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import active_children
from multiprocessing.context import SpawnContext
from typing import Dict, List

import psutil

from hyperscale.core.jobs.graphs.remote_graph_controller import (
    RemoteGraphController,
)
from hyperscale.core.jobs.models import Env


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
        await asyncio.sleep(1)

        try:

            await server.connect_client(leader_address)
            await server.acknowledge_start(leader_address)

        except Exception:
            pass

        await server.run_forever()
        await server.close()

    except Exception:
        pass


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

    def setup(self):
        self._context = multiprocessing.get_context("spawn")
        self._executor = ProcessPoolExecutor(
            max_workers=self._pool_size,
            mp_context=self._context,
        )

        self._loop = asyncio.get_event_loop()

        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            self._loop.add_signal_handler(
                getattr(
                    signal,
                    signame,
                ),
                self.abort,
            )

    def run_pool(
        self,
        leader_address: tuple[str, int],
        sockets: List[socket.socket],
        env: Env,
        cert_path: str | None = None,
        key_path: str | None = None,
    ):
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

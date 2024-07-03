import asyncio
import functools
import multiprocessing
import signal
import warnings
from concurrent.futures import ProcessPoolExecutor
from multiprocessing.context import SpawnContext
from typing import Dict, List, Tuple

import psutil

from hyperscale.core_rewrite.jobs.graphs.remote_graph_controller import (
    RemoteGraphController,
)
from hyperscale.core_rewrite.jobs.models import Env


async def run_server(
    server: RemoteGraphController,
    worker_idx: int,
    cert_path: str | None = None,
    key_path: str | None = None,
):
    try:
        await server.start_server(cert_path=cert_path, key_path=key_path)
        await server.run_forever()
        await server.close()

    except asyncio.CancelledError:
        server.abort()

    except KeyboardInterrupt:
        server.abort()

    except Exception:
        server.abort()
        import traceback

        print(traceback.format_exc())


def abort(server: RemoteGraphController, run_task: asyncio.Future):
    server.abort()


def run_thread(
    host: str,
    port: int,
    worker_env: Dict[str, str | int | float | bool | None],
    worker_idx: int,
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

    env = Env(**worker_env)

    server = RemoteGraphController(host, port, env)

    for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
        loop.add_signal_handler(
            getattr(
                signal,
                signame,
            ),
            lambda signame=signame: abort(
                server,
            ),
        )

    try:
        loop.run_until_complete(
            run_server(
                server,
                worker_idx,
                cert_path=cert_path,
                key_path=key_path,
            )
        )

    except Exception:
        try:
            server.abort()

        except Exception:
            pass

        except asyncio.InvalidStateError:
            pass

        except asyncio.CancelledError:
            pass

    except asyncio.CancelledError:
        try:
            server.abort()

        except Exception:
            pass

        except asyncio.InvalidStateError:
            pass

        except asyncio.CancelledError:
            pass


class LocalServerPool:
    def __init__(
        self,
        pool_size: int = psutil.cpu_count(logical=False),
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
        ip_range: List[Tuple[str, int]],
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
                        address[0],
                        address[1],
                        env.model_dump(),
                        worker_idx,
                        cert_path=cert_path,
                        key_path=key_path,
                    ),
                )
                for worker_idx, address in enumerate(ip_range)
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
            self._executor.shutdown(cancel_futures=True, wait=False)

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
            self._executor.shutdown(cancel_futures=True, wait=False)

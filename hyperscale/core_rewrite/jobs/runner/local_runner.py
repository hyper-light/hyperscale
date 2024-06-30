import asyncio
import os
import signal
import psutil
from typing import List

from hyperscale.core_rewrite.graph import Graph, Workflow
from hyperscale.core_rewrite.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core_rewrite.jobs.models import Env

from .local_server_pool import LocalServerPool


def abort(
    manager: RemoteGraphManager,
    server: LocalServerPool,
):

    try:
        manager.abort()
        server.abort()

    except Exception:
        pass

    except asyncio.CancelledError:
        pass

    


class LocalRunner:
    def __init__(
        self,
        host: str,
        port: int,
        env: Env | None = None,
        workers: int = psutil.cpu_count(logical=False),
    ) -> None:
        if env is None:
            env = Env(
                MERCURY_SYNC_AUTH_SECRET=os.getenv(
                    "MERCURY_SYNC_AUTH_SECRET", "hyperscalelocal"
                ),
            )

        self._env = env
        self.host = host
        self.port = port
        self._workers = workers

        self._remote_manger = RemoteGraphManager()
        self._server_pool = LocalServerPool(pool_size=self._workers)
        self._pool_task: asyncio.Task | None = None

    async def run(
        self,
        test_name: str,
        workflows: List[Workflow],
        cert_path: str | None = None,
        key_path: str | None = None,
        timeout: int | float | str | None = None,
    ):
        loop = asyncio.get_event_loop()
        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            loop.add_signal_handler(
                getattr(
                    signal,
                    signame,
                ),
                lambda signame=signame: abort(
                    self._remote_manger,
                    self._server_pool,
                ),
            )
            
        try:
            if self._workers <= 1:
                graph = Graph(test_name, workflows)
                return await graph.run()

            else:
                base_worker_port = self.port + 2
                worker_port_range = [
                    port
                    for port in range(
                        base_worker_port,
                        base_worker_port + (self._workers * 2),
                        2,
                    )
                ]

                self._server_pool.setup()
                await self._remote_manger.start(
                    self.host,
                    self.port,
                    self._env,
                    cert_path=cert_path,
                    key_path=key_path,
                )

                worker_ips = [(self.host, port) for port in worker_port_range]

                self._server_pool.run_pool(
                    worker_ips,
                    self._env,
                    cert_path=cert_path,
                    key_path=key_path,
                )

                await self._remote_manger.connect_to_workers(
                    worker_ips,
                    cert_path=cert_path,
                    key_path=key_path,
                    timeout=timeout,
                )

                results = await self._remote_manger.execute_graph(
                    test_name,
                    workflows,
                )

                await self._remote_manger.shutdown_workers()
                await self._remote_manger.close()
                await self._server_pool.shutdown()

                return results
        except Exception:
            try:

                self._server_pool.abort()
                self._remote_manger.abort()

            except Exception:
                pass

            except asyncio.CancelledError:
                pass

        except asyncio.CancelledError:
            pass
import asyncio
import os
import signal
import socket
from multiprocessing import active_children, allow_connection_pickling, current_process
from typing import List

import psutil

from hyperscale.core_rewrite.graph import Graph, Workflow
from hyperscale.core_rewrite.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core_rewrite.jobs.models import Env
from hyperscale.core_rewrite.jobs.protocols.socket import bind_tcp_socket

from .local_server_pool import LocalServerPool

allow_connection_pickling()


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
        close_task = asyncio.current_task()

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
                worker_sockets, worker_ips = await self._bin_and_check_socket_range()

                self._server_pool.setup()
                await self._remote_manger.start(
                    self.host,
                    self.port,
                    self._env,
                    cert_path=cert_path,
                    key_path=key_path,
                )

                self._server_pool.run_pool(
                    worker_sockets,
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

                for task in asyncio.all_tasks():
                    try:
                        if task != close_task and task.cancelled() is False:
                            task.cancel()

                    except Exception:
                        pass

                    except asyncio.CancelledError:
                        pass

                return results

        except Exception:
            import traceback

            print(traceback.format_exc())
            try:
                self._remote_manger.abort()
            except Exception:
                pass

            except asyncio.CancelledError:
                pass

            try:
                self._server_pool.abort()
            except Exception:
                pass

            except asyncio.CancelledError:
                pass

            self._kill_processes()

        except asyncio.CancelledError:
            try:
                self._remote_manger.abort()
            except Exception:
                pass

            except asyncio.CancelledError:
                pass

            try:
                self._server_pool.abort()
            except Exception:
                pass

            except asyncio.CancelledError:
                pass

            close_task = asyncio.current_task()
            for task in asyncio.all_tasks():
                try:
                    if task != close_task and task.cancelled() is False:
                        task.cancel()

                except Exception:
                    pass

                except asyncio.CancelledError:
                    pass

            self._kill_processes()

    def _kill_processes(self):
        child_processes = active_children()
        for child in child_processes:
            child.kill()

        process = current_process()
        if process:
            try:
                process.kill()

            except Exception:
                pass

    async def _bin_and_check_socket_range(self):
        base_worker_port = self.port + 2
        worker_port_range = [
            port
            for port in range(
                base_worker_port,
                base_worker_port + (self._workers * 2),
                2,
            )
        ]

        worker_sockets: List[socket.socket] = []
        worker_ips: List[tuple[str, int]] = []

        for port in worker_port_range:
            testing_port = True

            while testing_port:
                try:
                    worker_socket = bind_tcp_socket(self.host, port)
                    worker_ips.append((self.host, port))

                    testing_port = False
                    worker_sockets.append(worker_socket)

                except OSError:
                    port += self._workers + 1
                    await asyncio.sleep(0.1)

        return (worker_sockets, worker_ips)

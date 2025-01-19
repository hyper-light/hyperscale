import asyncio
import os
import signal
import socket
from multiprocessing import active_children, allow_connection_pickling, current_process
from typing import List

import psutil

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.models import Env
from hyperscale.core.jobs.protocols.socket import bind_udp_socket
from hyperscale.logging import LogLevelName, LogLevel, Logger
from hyperscale.ui import HyperscaleInterface, InterfaceUpdatesController
from hyperscale.ui.actions import update_active_workflow_message

from .local_server_pool import LocalServerPool

allow_connection_pickling()


async def abort(
    manager: RemoteGraphManager,
    server: LocalServerPool,
    interface: HyperscaleInterface,
    terminal_ui_enabled: bool = True,
):
    try:
        if terminal_ui_enabled:
            await interface.abort()

    except Exception:
        pass

    except asyncio.CancelledError:
        pass

    try:
        manager.abort()
    except Exception:
        pass

    except asyncio.CancelledError:
        pass

    try:
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
        log_level: LogLevelName,
        env: Env | None = None,
        workers: int | None = None,
    ) -> None:
        if env is None:
            env = Env(
                MERCURY_SYNC_AUTH_SECRET=os.getenv(
                    "MERCURY_SYNC_AUTH_SECRET", "hyperscalelocal"
                ),
            )

        if workers is None:
            workers = psutil.cpu_count(logical=False)

        self._env = env

        self.host = host
        self.port = port
        self._workers = workers
        self._worker_connect_timeout = TimeParser(env.MERCURY_SYNC_CONNECT_SECONDS).time
        self._log_level = log_level

        updates = InterfaceUpdatesController()

        self._interface = HyperscaleInterface(updates)
        self._remote_manger = RemoteGraphManager(updates, self._workers)
        self._server_pool = LocalServerPool(self._workers)
        self._logger = Logger()
        self._pool_task: asyncio.Task | None = None

    async def run(
        self,
        test_name: str,
        workflows: List[Workflow],
        cert_path: str | None = None,
        key_path: str | None = None,
        timeout: int | float | str | None = None,
        terminal_ui_enabled: bool = True,
    ):
        loop = asyncio.get_event_loop()
        close_task = asyncio.current_task()

        for signame in ("SIGINT", "SIGTERM", "SIG_IGN"):
            sig = getattr(
                signal,
                signame,
            )

            loop.add_signal_handler(
                sig,
                lambda signame=signame: asyncio.create_task(
                    abort(
                        self._remote_manger,
                        self._server_pool,
                        self._interface,
                        terminal_ui_enabled,
                    )
                ),
            )

        self._interface.initialize(workflows)
        if terminal_ui_enabled:
            await self._interface.run()

        if timeout is None:
            timeout = self._worker_connect_timeout

        try:
            await update_active_workflow_message(
                "initializing",
                "Starting worker servers...",
            )

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
                (self.host, self.port),
                worker_sockets,
                self._env,
                cert_path=cert_path,
                key_path=key_path,
            )

            await self._remote_manger.connect_to_workers(
                worker_ips,
                timeout=timeout,
            )

            results = await self._remote_manger.execute_graph(
                test_name,
                workflows,
            )

            if terminal_ui_enabled:
                await self._interface.stop()

            await self._remote_manger.shutdown_workers()
            await self._remote_manger.close()
            await self._server_pool.shutdown()

            for socket in worker_sockets:
                try:
                    socket.close()
                    await asyncio.sleep(0)

                except Exception:
                    pass

            return results

        except Exception:
            import traceback
            print(traceback.format_exc())
            try:
                if terminal_ui_enabled:
                    await self._interface.abort()

            except Exception:
                pass
            except asyncio.CancelledError:
                pass

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

        except asyncio.CancelledError:
            try:
                if terminal_ui_enabled:
                    await self._interface.abort()

            except Exception:
                pass
            except asyncio.CancelledError:
                pass

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

    async def abort(
        self,
        terminal_ui_enabled: bool = True,
    ):
        try:
            if terminal_ui_enabled:
                await self._interface.abort()

        except Exception:
            pass
        except asyncio.CancelledError:
            pass

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

    def close(self):
        child_processes = active_children()
        for child in child_processes:
            try:
                child.kill()

            except Exception:
                pass

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
                    worker_socket = bind_udp_socket(self.host, port)
                    worker_ips.append((self.host, port))
                    worker_sockets.append(worker_socket)

                    testing_port = False

                except OSError:
                    port += self._workers + 1
                    await asyncio.sleep(0.1)

        if len(worker_sockets) < self._workers:
            raise Exception("Err. - Insufficient sockets binned.")

        return (worker_sockets, worker_ips)

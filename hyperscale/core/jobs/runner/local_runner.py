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
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    TestInfo, 
    TestFatal, 
    TestDebug,
    TestTrace
)
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

        updates = InterfaceUpdatesController()

        self._interface = HyperscaleInterface(updates)
        self._remote_manger = RemoteGraphManager(updates, self._workers)
        self._server_pool = LocalServerPool(self._workers)
        self._logger = Logger()
        self._pool_task: asyncio.Task | None = None
        self._runner_type = self.__class__.__name__

    async def run(
        self,
        test_name: str,
        workflows: List[Workflow],
        cert_path: str | None = None,
        key_path: str | None = None,
        timeout: int | float | str | None = None,
        terminal_ui_enabled: bool = True,
    ):
                
        workflow_names = [
            workflow.name for workflow in workflows
        ]

        default_config = {   
            "runner_type": self._runner_type,
            "workflows": workflow_names,
            "workers": self._workers,
            "test": test_name,
        }

        self._logger.configure(
            name='local_runner',
            path='hyperscale.main.log.json',
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                'trace': (
                    TestTrace,
                    default_config
                ),
                'debug': (
                    TestDebug,
                    default_config
                ),
                'info': (
                    TestInfo,
                    default_config
                ),
                'fatal': (
                    TestFatal,
                    default_config
                ),
            }
        )

        async with self._logger.context(name='local_runner') as ctx:
            
            await ctx.log_prepared(f'Starting {test_name} test with {self._workers} workers', name='info')

            loop = asyncio.get_event_loop()
            close_task = asyncio.current_task()

            await ctx.log_prepared(f'Setting interrupt handlers for SIGINT, SIGTERM, SIG_IGN for test {test_name}', name='trace')

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

            await ctx.log_prepared(f'Initializing UI for test {test_name}', name='trace')

            self._interface.initialize(workflows)
            if terminal_ui_enabled:
                await ctx.log_prepared(f'Hyperscale Terminal UI is enabled for test {test_name}', name='debug')
                await self._interface.run()

            else:
                await ctx.log_prepared(f'Hyperscale Terminal UI is disabled for test {test_name}', name='debug')

            if timeout is None:
                timeout = self._worker_connect_timeout

            try:
                await update_active_workflow_message(
                    "initializing",
                    "Starting worker servers...",
                )

                worker_sockets, worker_ips = await self._bin_and_check_socket_range(
                    test_name,
                    workflow_names,
                )

                await ctx.log_prepared(f'Initializing worker servers on runner type {self._runner_type} for test {test_name}', name='info')
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

                await ctx.log_prepared_batch({
                    'info': [
                        f'Successfully connected to {self._workers} workers on runner type {self._runner_type} for test {test_name}',
                        f'Beginning run for test {test_name}'
                    ]
                })

                results = await self._remote_manger.execute_graph(
                    test_name,
                    workflows,
                )

                await ctx.log_prepared(f'Completed execution of test {test_name} on runner type {self._runner_type} - shutting down', name='info')

                if terminal_ui_enabled:
                    await ctx.log_prepared(f'Stopping Hyperscale Terminal UI for test {test_name}', name='debug')

                    await self._interface.stop()

                    await ctx.log_prepared(f'Stopped Hyperscale Terminal UI for test {test_name}', name='trace')

                await ctx.log_prepared(f'Stopping Hyperscale Remote Manager for test {test_name}', name='debug')

                await self._remote_manger.shutdown_workers()
                await self._remote_manger.close()

                await ctx.log_prepared(f'Stopping Hyperscale Server Pool for test {test_name}', name='debug')

                await self._server_pool.shutdown()

                await ctx.log_prepared(f'Closing {len(worker_sockets)} sockets for test {test_name}', name='trace')
                for socket in worker_sockets:
                    host, port = socket.getsockname()
                    await ctx.log_prepared(f'Closing worker socket on {host}:{port} for test {test_name}', name='trace')

                    try:
                        socket.close()
                        await asyncio.sleep(0)

                    except Exception:
                        pass
                    
                    await ctx.log_prepared(f'Worker socket on {host}:{port} closed for test {test_name}', name='trace')

                await ctx.log_prepared(f'Exiting test {test_name}', name='info')
 
                return results

            except Exception as e:
                await ctx.log_prepared(f'Encountered fatal exception {str(e)} while running test {test_name} - aborting', name='fatal')

                try:
                    if terminal_ui_enabled:
                        await ctx.log_prepared(f'Aborting Hyperscale Terminal UI for test {test_name}', name='debug')
                        await self._interface.abort()

                except Exception as e:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Terminal UI for test {test_name}', name='trace')

                except asyncio.CancelledError:
                    pass

                import traceback
                print(traceback.format_exc())
                
                try:
                    await ctx.log_prepared(f'Aborting Hyperscale Remote Manager for test {test_name}', name='debug')
                    self._remote_manger.abort()

                except Exception as e:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Remote Manager for test {test_name}', name='trace')

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log_prepared(f'Aborting Hyperscale Server Pool for test {test_name}', name='debug')
                    self._server_pool.abort()

                except Exception:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Server Pool for test {test_name}', name='trace')
 
                except asyncio.CancelledError:
                    pass

            except asyncio.CancelledError:
                await ctx.log_prepared(f'Encountered interrupt while running test {test_name} - aborting', name='fatal')

                try:
                    if terminal_ui_enabled:
                        await ctx.log_prepared(f'Aborting Hyperscale Terminal UI for test {test_name}', name='debug')
                        await self._interface.abort()

                except Exception as e:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Terminal UI for test {test_name}', name='trace')

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log_prepared(f'Aborting Hyperscale Remote Manager for test {test_name}', name='debug')
                    self._remote_manger.abort()

                except Exception as e:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Remote Manager for test {test_name}', name='trace')

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log_prepared(f'Aborting Hyperscale Server Pool for test {test_name}', name='debug')
                    self._server_pool.abort()

                except Exception:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Server Pool for test {test_name}', name='trace')

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
        error: Exception | None = None,
        terminal_ui_enabled: bool = True,
    ):
        async with self._logger.context(
            name='local_runner',
            nested=True,
        ) as ctx:

            if error is None:
                await ctx.log_prepared(f'Runner type {self._runner_type} received a call to abort and is now aborting all running tests', name='fatal')

            else:
                await ctx.log_prepared(f'Runner type {self._runner_type} encountered exception {str(e)} is now aborting all running tests', name='fatal')

            try:
                if terminal_ui_enabled:
                    await ctx.log_prepared('Aborting Hyperscale Terminal UI', name='debug')
                    await self._interface.abort()

            except Exception as e:
                    await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Terminal UI', name='trace')

            except asyncio.CancelledError:
                pass

            try:
                self._remote_manger.abort()
                await ctx.log_prepared('Aborting Hyperscale Remote Manager', name='debug')

            except Exception as e:
                await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Remote Manager', name='trace')
                
            except asyncio.CancelledError:
                pass

            try:
                await ctx.log_prepared('Aborting Hyperscale Server Pool', name='debug')
                self._server_pool.abort()
            except Exception as e:
                await ctx.log_prepared(f'Encountered error {str(e)} aborting Hyperscale Server Pool', name='debug')
                
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

    async def _bin_and_check_socket_range(
        self,
        test_name: str,
        workflow_names: list[str]
    ):
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

        async with self._logger.context(
            name='local_runner',
            nested=True,
        ) as ctx:
            await ctx.log_prepared(f'Provisioning {self._workers} worker sockets for test {test_name}', name='debug')

            for port in worker_port_range:
                testing_port = True

                await ctx.log_prepared(f'Provisioning worker socket on port {port} for test {test_name}', name='trace')

                while testing_port:
                    try:
                        worker_socket = bind_udp_socket(self.host, port)
                        worker_ips.append((self.host, port))
                        worker_sockets.append(worker_socket)

                        await ctx.log_prepared(f'Successfully provisioned worker socket on port {port} for test {test_name}', name='trace')

                        testing_port = False

                    except OSError:
                        previous_port = port
                        port += self._workers + 1

                        await ctx.log_prepared(f'Failed to provision worker socket on port {previous_port} as socket is busy - re-attempting on port {port} for test {test_name}', name='trace')

                        await asyncio.sleep(0.1)

            if len(worker_sockets) < self._workers:
                raise Exception("Err. - Insufficient sockets binned.")
            
            await ctx.log_prepared(f'Provisioned {self._workers} worker sockets for test {test_name}', name='debug')

        return (worker_sockets, worker_ips)

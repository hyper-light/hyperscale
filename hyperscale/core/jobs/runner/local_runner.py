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

        async with self._logger.context(
            path='hyperscale.main.log.json',
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            
            await ctx.log(TestInfo(
                message=f'Starting {test_name} test with {self._workers} workers',
                runner_type=self._runner_type,
                workflows=workflow_names,
                workers=self._workers,
                test=test_name,
            ))

            loop = asyncio.get_event_loop()
            close_task = asyncio.current_task()

            await ctx.log(TestTrace(
                message=f'Setting interrupt handlers for SIGINT, SIGTERM, SIG_IGN for test {test_name}',
                runner_type=self._runner_type,
                workflows=workflow_names,
                workers=self._workers,
                test=test_name,
            ))

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

            await ctx.log(TestTrace(
                message=f'Initializing UI for test {test_name}',
                runner_type=self._runner_type,
                workflows=workflow_names,
                workers=self._workers,
                test=test_name
            ))

            self._interface.initialize(workflows)
            if terminal_ui_enabled:
                await ctx.log(TestDebug(
                    message=f'Hyperscale Terminal UI is enabled for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))

                await self._interface.run()

            else:
                await ctx.log(TestDebug(
                    message=f'Hyperscale Terminal UI is disabled for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name,
                ))

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

                await ctx.log(TestInfo(
                    message=f'Initializing worker servers on runner type {self._runner_type} for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))

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

                await ctx.batch([
                    TestInfo(
                        message=f'Successfully connected to {self._workers} workers on runner type {self._runner_type} for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ),
                    TestInfo(
                        message=f'Beginning run for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    )
                ])

                results = await self._remote_manger.execute_graph(
                    test_name,
                    workflows,
                )

                await ctx.log(TestInfo(
                    message=f'Completed execution of test {test_name} on runner type {self._runner_type} - shutting down',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))

                if terminal_ui_enabled:

                    await ctx.log(TestDebug(
                        message=f'Stopping Hyperscale Terminal UI for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                    await self._interface.stop()

                    await ctx.log(TestTrace(
                        message=f'Stopped Hyperscale Terminal UI for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))
                
                await ctx.log(TestDebug(
                    message=f'Stopping Hyperscale Remote Manager for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))

                await self._remote_manger.shutdown_workers()
                await self._remote_manger.close()

                await ctx.log(TestDebug(
                    message=f'Stopping Hyperscale Server Pool for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))


                await self._server_pool.shutdown()

                await ctx.log(TestTrace(
                    message=f'Closing {len(worker_sockets)} sockets for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))

                for socket in worker_sockets:
                    host, port = socket.getsockname()
                    await ctx.log(TestTrace(
                        message=f'Closing worker socket on {host}:{port} for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))
                    
                    try:
                        socket.close()
                        await asyncio.sleep(0)

                    except Exception:
                        pass

                    await ctx.log(TestTrace(
                        message=f'Worker socket on {host}:{port} closed for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))


                await ctx.log(TestInfo(
                    message=f'Exiting test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name,
                ))

                return results

            except Exception as e:

                await ctx.log(TestFatal(
                    message=f'Encountered fatal exception {str(e)} while running test {test_name} - aborting',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name,
                ))

                try:
                    if terminal_ui_enabled:
                        await ctx.log(TestDebug(
                            message=f'Aborting Hyperscale Terminal UI for test {test_name}',
                            runner_type=self._runner_type,
                            workflows=workflow_names,
                            workers=self._workers,
                            test=test_name
                        ))

                        await self._interface.abort()

                except Exception as e:
                    await ctx.log(TestTrace(
                        message=f'Encountered error {str(e)} aborting Hyperscale Terminal UI for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log(TestDebug(
                        message=f'Aborting Hyperscale Remote Manager for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                    self._remote_manger.abort()
                except Exception as e:
                    await ctx.log(TestTrace(
                        message=f'Encountered error {str(e)} aborting Hyperscale Remote Manager for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log(TestDebug(
                        message=f'Aborting Hyperscale Server Pool for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                    self._server_pool.abort()
                except Exception:
                    await ctx.log(TestDebug(
                        message=f'Encountered error {str(e)} aborting Hyperscale Server Pool for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                except asyncio.CancelledError:
                    pass

            except asyncio.CancelledError:
                await ctx.log(TestFatal(
                    message=f'Encountered interrupt while running test {test_name} - aborting',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name,
                ))

                try:
                    if terminal_ui_enabled:
                        await ctx.log(TestDebug(
                            message=f'Aborting Hyperscale Terminal UI for test {test_name}',
                            runner_type=self._runner_type,
                            workflows=workflow_names,
                            workers=self._workers,
                            test=test_name
                        ))

                        await self._interface.abort()

                except Exception as e:
                    await ctx.log(TestTrace(
                        message=f'Encountered error {str(e)} aborting Hyperscale Terminal UI for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log(TestDebug(
                        message=f'Aborting Hyperscale Remote Manager for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                    self._remote_manger.abort()
                except Exception as e:
                    await ctx.log(TestTrace(
                        message=f'Encountered error {str(e)} aborting Hyperscale Remote Manager for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                except asyncio.CancelledError:
                    pass

                try:
                    await ctx.log(TestDebug(
                        message=f'Aborting Hyperscale Server Pool for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

                    self._server_pool.abort()
                except Exception:
                    await ctx.log(TestDebug(
                        message=f'Encountered error {str(e)} aborting Hyperscale Server Pool for test {test_name}',
                        runner_type=self._runner_type,
                        workflows=workflow_names,
                        workers=self._workers,
                        test=test_name
                    ))

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

        async with self._logger.context(nested=True) as ctx:

            await ctx.log(TestDebug(
                message=f'Provisioning {self._workers} worker sockets for test {test_name}',
                runner_type=self._runner_type,
                workflows=workflow_names,
                workers=self._workers,
                test=test_name
            ))

            for port in worker_port_range:
                testing_port = True

                await ctx.log(TestTrace(
                    message=f'Provisioning worker socket on port {port} for test {test_name}',
                    runner_type=self._runner_type,
                    workflows=workflow_names,
                    workers=self._workers,
                    test=test_name
                ))

                while testing_port:
                    try:
                        worker_socket = bind_udp_socket(self.host, port)
                        worker_ips.append((self.host, port))
                        worker_sockets.append(worker_socket)

                        await ctx.log(TestTrace(
                            message=f'Successfully provisioned worker socket on port {port} for test {test_name}',
                            runner_type=self._runner_type,
                            workflows=workflow_names,
                            workers=self._workers,
                            test=test_name,
                        ))

                        testing_port = False

                    except OSError:
                        previous_port = port
                        port += self._workers + 1

                        await ctx.log(TestTrace(
                            message=f'Failed to provision worker socket on port {previous_port} as socket is busy - re-attempting on port {port} for test {test_name}',
                            runner_type=self._runner_type,
                            workflows=workflow_names,
                            workers=self._workers,
                            test=test_name,
                        ))

                        await asyncio.sleep(0.1)

            if len(worker_sockets) < self._workers:
                raise Exception("Err. - Insufficient sockets binned.")
            

            await ctx.log(TestDebug(
                message=f'Provisioned {self._workers} worker sockets for test {test_name}',
                runner_type=self._runner_type,
                workflows=workflow_names,
                workers=self._workers,
                test=test_name
            ))

        return (worker_sockets, worker_ips)

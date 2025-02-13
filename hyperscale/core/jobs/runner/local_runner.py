import asyncio
import os
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import (
    ProcessError,
    active_children,
)
from typing import List

import psutil

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.models import Env, TerminalMode
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    TestDebug,
    TestFatal,
    TestInfo,
    TestTrace,
)
from hyperscale.ui import HyperscaleInterface, InterfaceUpdatesController
from hyperscale.ui.actions import update_active_workflow_message

from .local_server_pool import LocalServerPool


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

        self._updates = InterfaceUpdatesController()

        self._interface = HyperscaleInterface(self._updates)
        self._remote_manger = RemoteGraphManager(self._updates, self._workers)
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
        terminal_mode: TerminalMode = "full",
    ):
        workflow_names = [workflow.name for workflow in workflows]

        default_config = {
            "runner_type": self._runner_type,
            "workflows": workflow_names,
            "workers": self._workers,
            "test": test_name,
        }

        self._logger.configure(
            name="local_runner",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (TestTrace, default_config),
                "debug": (TestDebug, default_config),
                "info": (TestInfo, default_config),
                "fatal": (TestFatal, default_config),
            },
        )

        async with self._logger.context(name="local_runner") as ctx:
            await ctx.log_prepared(
                f"Starting {test_name} test with {self._workers} workers", name="info"
            )

            await ctx.log_prepared(
                f"Setting interrupt handlers for SIGINT, SIGTERM, SIG_IGN for test {test_name}",
                name="trace",
            )
            await ctx.log_prepared(
                f"Initializing UI for test {test_name}", name="trace"
            )

            self._interface.initialize(
                workflows,
                terminal_mode=terminal_mode,
            )

            if terminal_mode in ["ci", "full"]:
                await ctx.log_prepared(
                    f"Hyperscale Terminal UI is enabled for test {test_name}",
                    name="debug",
                )
                await self._interface.run()

            else:
                await ctx.log_prepared(
                    f"Hyperscale Terminal UI is disabled for test {test_name}",
                    name="debug",
                )

            if timeout is None:
                timeout = self._worker_connect_timeout

            try:
                await update_active_workflow_message(
                    "initializing",
                    "Starting worker servers...",
                )

                worker_ips = self._bin_and_check_socket_range()

                await ctx.log_prepared(
                    f"Initializing worker servers on runner type {self._runner_type} for test {test_name}",
                    name="info",
                )
                await self._server_pool.setup()

                await self._remote_manger.start(
                    self.host,
                    self.port,
                    self._env,
                    cert_path=cert_path,
                    key_path=key_path,
                )

                await self._server_pool.run_pool(
                    (self.host, self.port),
                    worker_ips,
                    self._env,
                    cert_path=cert_path,
                    key_path=key_path,
                )

                await self._remote_manger.connect_to_workers(
                    worker_ips,
                    timeout=timeout,
                )

                await ctx.log_prepared_batch(
                    {
                        "info": [
                            f"Successfully connected to {self._workers} workers on runner type {self._runner_type} for test {test_name}",
                            f"Beginning run for test {test_name}",
                        ]
                    }
                )

                results = await self._remote_manger.execute_graph(
                    test_name,
                    workflows,
                )

                await ctx.log_prepared(
                    f"Completed execution of test {test_name} on runner type {self._runner_type} - shutting down",
                    name="info",
                )

                if terminal_mode in ["ci", "full"]:
                    await ctx.log_prepared(
                        f"Stopping Hyperscale Terminal UI for test {test_name}",
                        name="debug",
                    )

                    await self._interface.stop()

                    await ctx.log_prepared(
                        f"Stopped Hyperscale Terminal UI for test {test_name}",
                        name="trace",
                    )

                await ctx.log_prepared(
                    f"Stopping Hyperscale Remote Manager for test {test_name}",
                    name="debug",
                )

                await self._remote_manger.shutdown_workers()
                await self._remote_manger.close()

                loop = asyncio.get_event_loop()
                children = await loop.run_in_executor(None, active_children)

                await asyncio.gather(
                    *[loop.run_in_executor(None, child.kill) for child in children]
                )

                await ctx.log_prepared(
                    f"Stopping Hyperscale Server Pool for test {test_name}",
                    name="debug",
                )
                await self._server_pool.shutdown()

                await ctx.log_prepared(f"Exiting test {test_name}", name="info")

                return results

            except (
                Exception,
                KeyboardInterrupt,
                ProcessError,
                asyncio.TimeoutError,
                asyncio.CancelledError,
                BrokenProcessPool,
            ) as err:
                if isinstance(err, asyncio.CancelledError):
                    await ctx.log_prepared(
                        f"Encountered interrupt while running test {test_name} - aborting",
                        name="fatal",
                    )

                else:
                    await ctx.log_prepared(
                        f"Encountered fatal exception {str(err)} while running test {test_name} - aborting",
                        name="fatal",
                    )

                try:
                    if terminal_mode in ["ci", "full"]:
                        await ctx.log_prepared(
                            f"Aborting Hyperscale Terminal UI for test {test_name}",
                            name="debug",
                        )
                        await self._interface.stop()

                except Exception as e:
                    await ctx.log_prepared(
                        f"Encountered error {str(e)} aborting Hyperscale Terminal UI for test {test_name}",
                        name="trace",
                    )

                except asyncio.CancelledError:
                    pass

                if not isinstance(
                    err,
                    (
                        KeyboardInterrupt,
                        ProcessError,
                        asyncio.CancelledError,
                        BrokenProcessPool,
                    ),
                ):
                    await self._remote_manger.shutdown_workers()

                try:
                    await ctx.log_prepared(
                        f"Aborting Hyperscale Remote Manager for test {test_name}",
                        name="debug",
                    )
                    await self._remote_manger.close()

                except Exception as e:
                    await ctx.log_prepared(
                        f"Encountered error {str(e)} aborting Hyperscale Remote Manager for test {test_name}",
                        name="trace",
                    )

                except asyncio.CancelledError:
                    pass
                
                loop = asyncio.get_event_loop()
                children = await loop.run_in_executor(None, active_children)

                await asyncio.gather(
                    *[loop.run_in_executor(None, child.kill) for child in children]
                )

                try:
                    await ctx.log_prepared(
                        f"Aborting Hyperscale Server Pool for test {test_name}",
                        name="debug",
                    )
                    await self._server_pool.shutdown()

                except Exception as e:
                    await ctx.log_prepared(
                        f"Encountered error {str(e)} aborting Hyperscale Server Pool for test {test_name}",
                        name="trace",
                    )

                except asyncio.CancelledError:
                    pass

                return err

    async def abort(
        self,
        error: Exception | None = None,
        terminal_mode: TerminalMode = "full",
    ):
        async with self._logger.context(
            name="local_runner",
        ) as ctx:
            if error is None:
                await ctx.log_prepared(
                    f"Runner type {self._runner_type} received a call to abort and is now aborting all running tests",
                    name="fatal",
                )

            else:
                await ctx.log_prepared(
                    f"Runner type {self._runner_type} encountered exception {str(error)} is now aborting all running tests",
                    name="fatal",
                )

            try:
                if terminal_mode in ["ci", "full"]:
                    await ctx.log_prepared(
                        "Aborting Hyperscale Terminal UI", name="debug"
                    )
                    await self._interface.abort()

            except Exception as e:
                await ctx.log_prepared(
                    f"Encountered error {str(e)} aborting Hyperscale Terminal UI",
                    name="trace",
                )

            except asyncio.CancelledError:
                pass

            try:
                self._remote_manger.abort()
                await ctx.log_prepared(
                    "Aborting Hyperscale Remote Manager", name="debug"
                )

            except Exception as e:
                await ctx.log_prepared(
                    f"Encountered error {str(e)} aborting Hyperscale Remote Manager",
                    name="trace",
                )

            except asyncio.CancelledError:
                pass

            try:
                await ctx.log_prepared("Aborting Hyperscale Server Pool", name="debug")
                self._server_pool.abort()
            except Exception as e:
                await ctx.log_prepared(
                    f"Encountered error {str(e)} aborting Hyperscale Server Pool",
                    name="debug",
                )

            except asyncio.CancelledError:
                pass

    def _bin_and_check_socket_range(self):
        base_worker_port = self.port + self._workers
        return [
            (
                self.host,
                port,
            )
            for port in range(
                base_worker_port,
                base_worker_port + (self._workers**2),
                self._workers,
            )
        ]

import asyncio
import os
from concurrent.futures.process import BrokenProcessPool
from multiprocessing import (
    ProcessError,
    active_children,
)
from typing import Any

import psutil

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.models import Env
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    WorkflowDebug,
    WorkflowError,
    WorkflowFatal,
    WorkflowInfo,
    WorkflowTrace,
)
from hyperscale.ui import InterfaceUpdatesController
from .local_server_pool import LocalServerPool


class ServerRunner:
    def __init__(
        self,
        host: str,
        port: int,
        env: Env | None = None,
        workers: int | None = None,
        cert_path: str | None = None,
        key_path: str | None = None,
    ) -> None:
        if env is None:
            env = Env(
                MERCURY_SYNC_AUTH_SECRET=os.getenv(
                    "MERCURY_SYNC_AUTH_SECRET", "hyperscale-dev-secret-change-in-prod"
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

        self._remote_manger = RemoteGraphManager(self._updates, self._workers)
        self._server_pool = LocalServerPool(self._workers)
        self._logger = Logger()
        self._pool_task: asyncio.Task | None = None
        self._runner_type = self.__class__.__name__
        self._cert_path = cert_path
        self._key_path = key_path

    async def start(
        self,
        timeout: int | float | str | None = None,
    ):

        if timeout is None:
            timeout = self._worker_connect_timeout
        
        worker_ips = self._bin_and_check_socket_range()

        await self._server_pool.setup()

        await self._remote_manger.start(
            self.host,
            self.port,
            self._env,
            cert_path=self._cert_path,
            key_path=self._key_path,
        )

        await self._server_pool.run_pool(
            (self.host, self.port),
            worker_ips,
            self._env,
            cert_path=self._cert_path,
            key_path=self._key_path,
        )

        await self._remote_manger.connect_to_workers(
            worker_ips,
            timeout=timeout,
        )

    async def get_status_update(self, workflow: str):
        return await self._remote_manger.get_workflow_update(workflow)     

    async def run(
        self,
        run_id: int,
        workflow: Workflow,
        workflow_context: dict[str, Any],
        vus: int,
        threads: int,
        timeout: int | float | str | None = None,
    ):
        workflow_names = workflow.name

        default_config = {
            "workflow": workflow.name,
            "run_id": run_id,
            "workers": threads,
            "workflow_vus": workflow.vus,
            "duration": workflow.duration,
        }

        workflow_slug = workflow.name.lower()

        self._logger.configure(
            name=f"{workflow_slug}_logger",
            path="hyperscale.leader.log.json",
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (WorkflowTrace, default_config),
                "debug": (
                    WorkflowDebug,
                    default_config,
                ),
                "info": (
                    WorkflowInfo,
                    default_config,
                ),
                "error": (
                    WorkflowError,
                    default_config,
                ),
                "fatal": (
                    WorkflowFatal,
                    default_config,
                ),
            },
        )

        async with self._logger.context(name="local_runner") as ctx:

            if timeout is None:
                timeout = self._worker_connect_timeout

            try:

                results = await self._remote_manger.execute_workflow(
                    run_id,
                    workflow,
                    workflow_context,
                    vus,
                    threads,
                )

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
                        f"Encountered interrupt while running - aborting",
                        name="fatal",
                    )

                else:
                    await ctx.log_prepared(
                        f"Encountered fatal exception {str(err)} while running - aborting",
                        name="fatal",
                    )

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
                        f"Aborting Hyperscale Remote Manager",
                        name="debug",
                    )
                    await self._remote_manger.close()

                except Exception as e:
                    await ctx.log_prepared(
                        f"Encountered error {str(e)} aborting Hyperscale Remote Manager",
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
                        f"Aborting Hyperscale Server Pool",
                        name="debug",
                    )
                    await self._server_pool.shutdown()

                except Exception as e:
                    await ctx.log_prepared(
                        f"Encountered error {str(e)} aborting Hyperscale Server Pool",
                        name="trace",
                    )

                except asyncio.CancelledError:
                    pass

                return err
            
    async def stop(self):

        try:


            async with self._logger.context(
                name="local_runner",
            ) as ctx:
                
                await ctx.log_prepared(
                    f"Stopping Hyperscale Remote Manager",
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
                    f"Stopping Hyperscale Server Pool",
                    name="debug",
                )
                await self._server_pool.shutdown()

                await ctx.log_prepared(f"Exiting...", name="info")

        except Exception as err:
            await self.abort(error=err)


    async def abort(
        self,
        error: Exception | None = None,
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

import asyncio
import os
import psutil
import functools
import multiprocessing
from concurrent.futures.process import BrokenProcessPool, ProcessPoolExecutor
from multiprocessing import active_children, ProcessError
from hyperscale.core.jobs.models import (
    JobContext,
    ReceivedReceipt,
    Response,
    WorkflowJob,
    WorkflowResults,
    WorkflowStatusUpdate,
    Env
)
from hyperscale.core.jobs.graphs import WorkflowRunner
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.state import Context
from hyperscale.logging import Logger, Entry, LogLevel, LoggingConfig
from hyperscale.logging.hyperscale_logging_models import (
    RunTrace,
    RunDebug,
    RunInfo,
    RunError,
    RunFatal,
    StatusUpdate
)
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.runner.local_server_pool import set_process_name, run_thread
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.ui import InterfaceUpdatesController
from typing import Any, Tuple, TypeVar, Dict, Literal
from .servers import (
    WorkerUDPServer,
    WorkerTCPServer
)

T = TypeVar("T")

WorkflowResult = Tuple[
    int,
    WorkflowStats | Dict[str, Any | Exception],
]


NodeContextSet = Dict[int, Context]

NodeData = Dict[
    int,
    Dict[
        str,
        Dict[int, T],
    ],
]

StepStatsType = Literal[
    "total",
    "ok",
    "err",
]


StepStatsUpdate = Dict[str, Dict[StepStatsType, int]]


class DistributedWorker:

    def __init__(
        self,
        host: str,
        port: int,
        env: Env | None = None,
        workers: int | None = None,
    ):
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
        self._thread_pool_port = port + workers


        self._workers = workers
        self._worker_connect_timeout = TimeParser(env.MERCURY_SYNC_CONNECT_SECONDS).time

        self._updates = InterfaceUpdatesController()
        self._remote_manger = RemoteGraphManager(self._updates, self._workers)
        self._pool = ProcessPoolExecutor(
            max_workers=self._workers,
            mp_context=multiprocessing.get_context("spawn"),
            initializer=set_process_name,
            max_tasks_per_child=1

        )
        self._logger = Logger()
        self._pool_task: asyncio.Task | None = None
        self._worker_udp_server = WorkerUDPServer(
            host,
            port,
            env,
            self._remote_manger,
        )

        self._worker_tcp_server = WorkerTCPServer(
            host,
            port + 1,
            env,
            self._remote_manger
        )

        self._pool_task: asyncio.Future | None = None
        self._waiter: asyncio.Future | None = None
        self._loop = asyncio.get_event_loop()

        
    async def run(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        timeout: int | float | str | None = None,
    ):
        try:
            worker_ips = self._bin_and_check_socket_range()

            await self._remote_manger.start(
                self.host,
                self._thread_pool_port,
                self._env,
                cert_path=cert_path,
                key_path=key_path
            )


            await asyncio.gather(*[
                self._worker_udp_server.start_server(
                    'test.log.json',
                ),
                self._worker_tcp_server.start_server(
                    'test.log.json',
                )
            ])


            config = LoggingConfig()

            self._pool_task = asyncio.gather(
                *[
                    self._loop.run_in_executor(
                        self._pool,
                        functools.partial(
                            run_thread,
                            idx,
                            (
                                self.host,
                                self._thread_pool_port
                            ),
                            worker_ip,
                            self._env.model_dump(),
                            config.directory,
                            log_level=config.level.name.lower(),
                            cert_path=cert_path,
                            key_path=key_path,
                        ),
                    )
                    for idx, worker_ip in enumerate(worker_ips)
                ],
                return_exceptions=True,
            )

            await asyncio.gather(*[
                self._worker_udp_server.run_forever(),
                self._worker_tcp_server.run_forever()
            ])

            await self._loop.run_in_executor(
                None,
                functools.partial(
                    self._pool.shutdown,
                    wait=True,
                    cancel_futures=True
                )
            )

            self._worker_tcp_server.stop()
            self._worker_udp_server.stop()
   
            await asyncio.gather(*[
                self._worker_tcp_server.close(),
                self._worker_udp_server.close()
            ])

        except (
            Exception, 
            KeyboardInterrupt, 
            ProcessError, 
            asyncio.TimeoutError,
            asyncio.CancelledError,
            BrokenProcessPool,
        ) as e:
            try:
                await self._remote_manger.close()

            except Exception:
                pass
            
            if self._pool_task:
                try:
                    self._pool_task.set_result(None)

                except (
                    Exception,
                    asyncio.InvalidStateError,
                    asyncio.CancelledError
                ):
                    pass

            await self._loop.run_in_executor(
                None,
                functools.partial(
                     self._pool.shutdown,
                     wait=True,
                     cancel_futures=True
                )
            )
            
            self._worker_tcp_server.stop()
            self._worker_udp_server.stop()

            await asyncio.gather(*[
                self._worker_tcp_server.close(),
                self._worker_udp_server.close()
            ], return_exceptions=True)

            return e


    def _bin_and_check_socket_range(self):
        base_worker_port = self._thread_pool_port + self._workers
        return [
            (
                self.host,
                port,
            )
            for port in range(
                base_worker_port,
                base_worker_port + (self._workers ** 2),
                self._workers,
            )
        ]

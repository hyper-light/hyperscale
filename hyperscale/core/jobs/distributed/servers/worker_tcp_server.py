import asyncio
import os
import psutil
from hyperscale.core.jobs.protocols import TCPProtocol
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
from hyperscale.logging import Logger, Entry, LogLevel
from hyperscale.logging.hyperscale_logging_models import (
    RunTrace,
    RunDebug,
    RunInfo,
    RunError,
    RunFatal,
    StatusUpdate
)
from hyperscale.core.jobs.hooks import (
    receive,
    send,
    task,
)
from hyperscale.core.jobs.distributed.models import (
    TestResults,
    TestReceivedReceipt,
    WorkerStatsUpdate,
    WorkerStatsUpdateRequest,
    WorkflowRun,
)
from hyperscale.core.jobs.distributed.concurrency import BatchedSemaphore
from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.graphs.remote_graph_manager import RemoteGraphManager
from hyperscale.core.jobs.runner.local_runner import LocalRunner
from hyperscale.core.jobs.runner.local_server_pool import LocalServerPool
from hyperscale.reporting.common.results_types import (
    RunResults,
    WorkflowResultsSet,
    WorkflowContextResult,
    WorkflowStats,
)
from hyperscale.ui import HyperscaleInterface, InterfaceUpdatesController
from typing import Any, Tuple, TypeVar, Dict, Literal

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


class WorkerTCPServer(TCPProtocol[JobContext[Any], JobContext[Any]]):
    
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
        manager: RemoteGraphManager,
        workers: int,
    ):
        super().__init__(host, port, env)
        self._logfile = 'hyperscale.distributed.worker.log.json'

        self._manager = manager 
        self._workers = workers
        self._jobs_semaphore = BatchedSemaphore(
            value=workers
        )

    @send()
    async def send_results(
        self,
        node_id: str,
        results: WorkflowResults,
        run_id: int,
    ) -> Response[JobContext[None]]:
        return await self.send(
            "process_results",
            JobContext(
                results,
                run_id=run_id,
            ),
            node_id=node_id,
        )
    
    @receive()
    async def process_availability_request(
        self,
        _: int,
        job: JobContext[WorkerStatsUpdateRequest]
    ) -> JobContext[WorkerStatsUpdate]:
        return JobContext(
            WorkerStatsUpdate(
                self._node_id_base,
                self._jobs_semaphore.available,
            )
        )

    @receive()
    async def receive_new_workflow(
        self,
        shard_id: int,
        job: JobContext[WorkflowRun]
    ) -> JobContext[ReceivedReceipt]:
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        run_id = job.run_id
        task_id = self.id_generator.generate()

        self.tasks.run(
            "run_test",
            node_id,
            run_id,
            job.data,
            run_id=task_id,
        )

        return JobContext(
            ReceivedReceipt(
                job.data.workflow.name,
                self._node_id_base,
            ),
            run_id=run_id,
        )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 100),
        ),
        repeat="NEVER",
    )
    async def run_workflow(
        self,
        node_id: int,
        run_id: int,
        run: WorkflowRun
    ):
        
        await self._jobs_semaphore.acquire(run.threads)
        
        try:
            (
                workflow_name, 
                results, 
                error
            ) = await self._manager.run_workflow(
                run_id,
                run.workflow,
                run.threads,
                run.vus,
                skip_reporting=True,
            )

            workflow_status = WorkflowStatus.COMPLETED
            if error:
                workflow_status = WorkflowStatus.FAILED

            self._jobs_semaphore.release(run.threads)

            await self.send_results(
                run_id,
                WorkflowResults(
                    workflow_name,
                    results,
                    self._manager.get_run_context(run_id),
                    error,
                    workflow_status
                ),
                results=results,
            )

        except (
            Exception,
            asyncio.CancelledError,
            asyncio.TimeoutError,
            asyncio.InvalidStateError,
        ) as error:
            self._jobs_semaphore.release(run.threads)

            await self.send_results(
                node_id,
                WorkflowResults(
                    run.workflow.name,
                    None,
                    self._manager.get_run_context(run_id),
                    error,
                    WorkflowStatus.FAILED
                ),
                run_id,
            )
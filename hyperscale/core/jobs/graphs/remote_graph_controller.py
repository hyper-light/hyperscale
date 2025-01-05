import asyncio
import os
import statistics
import time
from collections import Counter, defaultdict
from socket import socket
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Set,
    Tuple,
    TypeVar,
    Literal
)

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.hooks import (
    receive,
    send,
    task,
)
from hyperscale.core.jobs.models import (
    JobContext,
    ReceivedReceipt,
    Response,
    WorkflowJob,
    WorkflowResults,
    WorkflowStatusUpdate,
)
from hyperscale.core.jobs.models.env import Env
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.jobs.protocols import UDPProtocol
from hyperscale.core.results.workflow_types import WorkflowStats
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.state import Context
from hyperscale.ui.actions import update_active_workflow_message

from .workflow_runner import WorkflowRunner

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


StepStatsUpdate = Dict[
    str,
    Dict[StepStatsType, int]
]


class RemoteGraphController(UDPProtocol[JobContext[Any], JobContext[Any]]):
    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
    ) -> None:
        super().__init__(host, port, env)

        self._workflows = WorkflowRunner(env)

        self.acknowledged_starts: set[str] = set()

        self._results: NodeData[WorkflowResult] = defaultdict(lambda: defaultdict(dict))
        self._errors: NodeData[Exception] = defaultdict(lambda: defaultdict(dict))

        self._node_context: NodeContextSet = defaultdict(dict)
        self._statuses: NodeData[WorkflowStatus] = defaultdict(
            lambda: defaultdict(dict)
        )

        self._run_workflow_expected_nodes: Dict[int, Dict[str, int]] = defaultdict(dict)

        self._completions: Dict[int, Dict[str, Set[int]]] = defaultdict(
            lambda: defaultdict(set),
        )

        self._completed_counts: Dict[int, Dict[str, Dict[int, int]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: 0),
            )
        )

        self._failed_counts: Dict[int, Dict[str, Dict[int, int]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: 0),
            )
        )

        self._step_stats: Dict[int, Dict[str, Dict[int, StepStatsUpdate]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(
                        lambda: {
                            "total": 0,
                            "ok": 0,
                            "err": 0
                        }
                    )
                )
            )
        )

        self._cpu_usage_stats: Dict[int, Dict[str, Dict[int, float]]] = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: 0))
        )

        self._memory_usage_stats: Dict[int, Dict[str, Dict[int, float]]] = defaultdict(
            lambda: defaultdict(
                lambda: defaultdict(lambda: 0),
            )
        )

        self._context_poll_rate = TimeParser(env.MERCURY_SYNC_CONTEXT_POLL_RATE).time
        self._completion_write_lock: Dict[int, Dict[str, Dict[int, asyncio.Lock]]] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(asyncio.Lock)))
        )

        self._leader_lock: asyncio.Lock | None = None

    async def start_server(
        self,
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket | None = None,
        worker_server: asyncio.Server | None = None,
    ) -> None:
        if self._leader_lock is None:
            self._leader_lock = asyncio.Lock()

        self._workflows.setup()
        return await super().start_server(
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=worker_socket,
            worker_server=worker_server,
        )

    async def connect_client(
        self,
        address: Tuple[str, int],
        cert_path: str | None = None,
        key_path: str | None = None,
        worker_socket: socket | None = None,
    ) -> None:
        self._workflows.setup()

        await super().connect_client(
            address,
            cert_path,
            key_path,
            worker_socket,
        )

    def create_run_contexts(self, run_id: int):
        self._node_context[run_id] = Context()

    def assign_context(
        self,
        run_id: int,
        workflow_name: str,
        threads: int,
    ):
        self._run_workflow_expected_nodes[run_id][workflow_name] = threads

        return self._node_context[run_id]

    async def update_context(
        self,
        run_id: int,
        context: Context,
    ):
        await self._node_context[run_id].copy(context)

    async def submit_workflow_to_workers(
        self,
        run_id: int,
        workflow: Workflow,
        context: Context,
        threads: int,
        workflow_vus: List[int],
        update_callback: Callable[[int, WorkflowStatus], Awaitable[None]],
    ):
        task_id = self.id_generator.generate()
        self.tasks.run(
            "get_latest_completed",
            run_id,
            workflow.name,
            update_callback,
            run_id=task_id,
        )

        return await asyncio.gather(
            *[
                self.submit(
                    run_id,
                    workflow,
                    workflow_vus[idx],
                    context,
                )
                for idx in range(threads)
            ]
        )
    
    async def poll_for_start(
        self,
        workers: int
    ):
        polling = True

        start = time.monotonic()
        elapsed = 0

        while polling:
            await asyncio.sleep(self._context_poll_rate)

            await self._leader_lock.acquire()

            acknowledged_starts_count = len(self.acknowledged_starts)

            if (
                acknowledged_starts_count
                >= workers
            ):
                await update_active_workflow_message(
                    'initializing',
                    f'Starting - {acknowledged_starts_count}/{workers} - threads'
                )

                break

            elapsed = time.monotonic() - start

            if elapsed > 1:
                start = time.monotonic()

                await update_active_workflow_message(
                    'initializing',
                    f'Starting - {acknowledged_starts_count}/{workers} - threads'
                )

            if self._leader_lock.locked():
                self._leader_lock.release()

        if self._leader_lock.locked():
            self._leader_lock.release()


    async def poll_for_workflow_complete(
        self,
        run_id: int,
        workflow_name: str,
    ):
        polling = True

        workflow_slug = workflow_name.lower()

        start = time.monotonic()
        elapsed = 0

        while polling:
            await asyncio.sleep(self._context_poll_rate)

            completions_count = len(self._completions[run_id][workflow_name])
            assigned_workers = self._run_workflow_expected_nodes[run_id][workflow_name]

            if completions_count >= assigned_workers:
                await update_active_workflow_message(
                    workflow_slug,
                    f'Running - {workflow_name} - {completions_count}/{assigned_workers} workers complete'
                )

                break

            elapsed = time.monotonic() - start

            if elapsed > 1:
                start = time.monotonic()

                await update_active_workflow_message(
                    workflow_slug,
                    f'Running - {workflow_name} - {completions_count}/{assigned_workers} workers complete'
                )

            if self._leader_lock.locked():
                self._leader_lock.release()
        
        if self._leader_lock.locked():
            self._leader_lock.release()

        return (
            self._results[run_id][workflow_name],
            self._node_context[run_id],
        )
    
    @send()
    async def acknowledge_start(
        self,
        leader_address: tuple[str, int],
    ):
        return await self.send(
            "receive_start_acknowledgement",
            JobContext(
                (self.host, self.port)
            ),
            target_address=leader_address
        )

    @send()
    async def submit(
        self,
        run_id: int,
        workflow: Workflow,
        vus: int,
        context: Context,
    ) -> Response[JobContext[WorkflowStatusUpdate]]:
        response: Response[JobContext[WorkflowStatusUpdate]] = await self.send(
            "start_workflow",
            JobContext(
                WorkflowJob(
                    workflow,
                    context,
                    vus,
                ),
                run_id=run_id,
            ),
        )

        (shard_id, workflow_status) = response

        if workflow_status.data:
            status = workflow_status.data.status
            workflow_name = workflow_status.data.workflow
            run_id = workflow_status.run_id

            snowflake = Snowflake.parse(shard_id)
            node_id = snowflake.instance

            self._statuses[run_id][workflow_name][node_id] = (
                WorkflowStatus.map_value_to_status(status)
            )

        return response

    @send()
    async def submit_stop_request(self):
        return await self.broadcast(
            "process_stop_request",
            JobContext(None),
        )

    @send()
    async def push_results(
        self,
        node_id: str,
        results: WorkflowResults,
        run_id: int,
    ) -> Response[JobContext[ReceivedReceipt]]:
        return await self.send(
            "process_results",
            JobContext(
                results,
                run_id=run_id,
            ),
            node_id=node_id,
        )
    
    @receive()
    async def receive_start_acknowledgement(
        self,
        _: int,
        acknowledgement: JobContext[tuple[str, int]]
    ):
        await self._leader_lock.acquire()

        host, port = acknowledgement.data

        node_addr = f'{host}:{port}'

        self.acknowledged_starts.add(node_addr)

        if self._leader_lock.locked():
            self._leader_lock.release()

    @receive()
    async def process_results(
        self,
        shard_id: int,
        workflow_results: JobContext[WorkflowResults],
    ) -> JobContext[ReceivedReceipt]:
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance
        timestamp = snowflake.timestamp

        run_id = workflow_results.run_id
        workflow_name = workflow_results.data.workflow

        results = workflow_results.data.results
        workflow_context = workflow_results.data.context
        error = workflow_results.data.error
        status = workflow_results.data.status

        await self._leader_lock.acquire()

        await asyncio.gather(
            *[
                self._node_context[run_id].update(
                    workflow_name,
                    key,
                    value,
                    timestamp=timestamp,
                )
                for _ in self.nodes
                for key, value in workflow_context.items()
            ]
        )

        self._results[run_id][workflow_name][node_id] = (
            timestamp,
            results,
        )
        self._statuses[run_id][workflow_name][node_id] = status
        self._errors[run_id][workflow_name][node_id] = Exception(error)

        self._completions[run_id][workflow_name].add(node_id)

        if self._leader_lock.locked():
            self._leader_lock.release()

        return JobContext(
            ReceivedReceipt(
                workflow_name,
                node_id,
            ),
            run_id=run_id,
        )

    @receive()
    async def process_stop_request(
        self,
        _: int,
        stop_request: JobContext[None],
    ) -> JobContext[None]:
        self.stop()

    @receive()
    async def start_workflow(
        self,
        shard_id: int,
        context: JobContext[WorkflowJob],
    ) -> JobContext[WorkflowStatusUpdate]:
        task_id = self.tasks.create_task_id()

        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        workflow_name = context.data.workflow.name

        self.tasks.run(
            "run_workflow",
            node_id,
            context.run_id,
            context.data,
            run_id=task_id,
        )

        self.tasks.run(
            "push_workflow_status_update",
            node_id,
            context.run_id,
            context.data,
            run_id=task_id,
        )

        return JobContext(
            WorkflowStatusUpdate(
                workflow_name, 
                WorkflowStatus.SUBMITTED, 
                node_id=node_id,
            ),
            run_id=context.run_id,
        )

    @receive()
    async def receive_status_update(
        self,
        shard_id: int,
        update: JobContext[WorkflowStatusUpdate],
    ) -> JobContext[ReceivedReceipt]:
        snowflake = Snowflake.parse(shard_id)
        node_id = snowflake.instance

        run_id = update.run_id
        workflow = update.data.workflow
        status = update.data.status
        completed_count = update.data.completed_count
        failed_count = update.data.failed_count

        step_stats = update.data.step_stats

        avg_cpu_usage = update.data.avg_cpu_usage
        avg_memory_usage_mb = update.data.avg_memory_usage_mb

        self._statuses[run_id][workflow][node_id] = WorkflowStatus.map_value_to_status(
            status
        )

        await self._completion_write_lock[run_id][workflow][node_id].acquire()

        self._completed_counts[run_id][workflow][node_id] = completed_count
        self._failed_counts[run_id][workflow][node_id] = failed_count
        self._step_stats[run_id][workflow][node_id] = step_stats

        self._cpu_usage_stats[run_id][workflow][node_id] = avg_cpu_usage
        self._memory_usage_stats[run_id][workflow][node_id] = avg_memory_usage_mb

        self._completion_write_lock[run_id][workflow][node_id].release()

        return JobContext(
            ReceivedReceipt(
                workflow,
                node_id,
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
        job: WorkflowJob,
    ):
        (
            run_id,
            results,
            context,
            error,
            status,
        ) = await self._workflows.run(
            run_id,
            job.workflow,
            job.context,
            job.vus,
        )

        if context is None:
            context = job.context

        await self.push_results(
            node_id,
            WorkflowResults(
                job.workflow.name,
                results,
                context,
                error,
                status,
            ),
            run_id,
        )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 10),
        ),
        trigger="MANUAL",
        repeat="ALWAYS",
        schedule="0.1s",
        max_age="1m",
        keep_policy="COUNT_AND_AGE",
    )
    async def push_workflow_status_update(
        self,
        node_id: int,
        run_id: int,
        job: WorkflowJob,
    ):
        workflow_name = job.workflow.name

        (
            status,
            completed_count,
            failed_count,
            step_stats,
        ) = self._workflows.get_running_workflow_stats(
            run_id,
            workflow_name,
        )

        avg_cpu_usage, avg_mem_usage = self._workflows.get_system_stats(
            run_id,
            workflow_name,
        )

        if status in [
            WorkflowStatus.COMPLETED,
            WorkflowStatus.REJECTED,
            WorkflowStatus.FAILED,
        ]:
            self.tasks.stop("push_workflow_status_update")

        await self.broadcast(
            "receive_status_update",
            JobContext(
                WorkflowStatusUpdate(
                    workflow_name,
                    status,
                    node_id=node_id,
                    completed_count=completed_count,
                    failed_count=failed_count,
                    step_stats=step_stats,
                    avg_cpu_usage=avg_cpu_usage,
                    avg_memory_usage_mb=avg_mem_usage,
                ),
                run_id=run_id,
            ),
        )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 10),
        ),
        trigger="MANUAL",
        repeat="ALWAYS",
        schedule="0.1s",
        keep_policy="COUNT",
    )
    async def get_latest_completed(
        self,
        run_id: int,
        workflow: str,
        update_callback: Callable[
            [WorkflowStatusUpdate],
            Awaitable[None],
        ],
    ):  
        workflow_status = WorkflowStatus.SUBMITTED

        status_counts = Counter(self._statuses[run_id][workflow].values())
        for status, count in status_counts.items():
            if count == self._run_workflow_expected_nodes[run_id][workflow]:
                workflow_status = status

                break

        completed_count = sum(self._completed_counts[run_id][workflow].values())
        failed_count = sum(self._failed_counts[run_id][workflow].values())

        step_stats: StepStatsUpdate = defaultdict(lambda: {
            "ok": 0,
            "total": 0,
            "err": 0,
        })

        for _, stats_update in self._step_stats[run_id][workflow].items():
            for hook, stats_set in stats_update.items():
                for stats_type, stat in stats_set.items():
                    step_stats[hook][stats_type] += stat

        cpu_usage_stats = self._cpu_usage_stats[run_id][workflow].values()
        avg_cpu_usage = 0
        if len(cpu_usage_stats) > 0:
            avg_cpu_usage = statistics.mean(cpu_usage_stats)

        memory_usage_stats = self._memory_usage_stats[run_id][workflow].values()
        avg_mem_usage_mb = 0
        if len(memory_usage_stats) > 0:
            avg_mem_usage_mb = statistics.mean(memory_usage_stats)

        await update_callback(
            WorkflowStatusUpdate(
                workflow,
                workflow_status,
                completed_count=completed_count,
                failed_count=failed_count,
                step_stats=step_stats,
                avg_cpu_usage=avg_cpu_usage,
                avg_memory_usage_mb=avg_mem_usage_mb,
            )
        )
    async def close(self) -> None:
        await super().close()
        await self._workflows.close()

    def abort(self) -> None:
        super().abort()
        self._workflows.abort()

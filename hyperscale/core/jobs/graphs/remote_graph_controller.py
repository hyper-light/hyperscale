import asyncio
import os
import statistics
import time
from collections import Counter, defaultdict
from socket import socket
from typing import Any, Awaitable, Callable, Dict, List, Set, Tuple, TypeVar

from hyperscale.core.engines.client.time_parser import TimeParser
from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.hooks import (
    receive,
    send,
    task,
)
from hyperscale.core.jobs.models import (
    Env,
    JobContext,
    ReceivedReceipt,
    Response,
    StepStatsUpdate,
    WorkflowCancellation,
    WorkflowCancellationStatus,
    WorkflowCancellationUpdate,
    WorkflowCompletionState,
    WorkflowJob,
    WorkflowResults,
    WorkflowStatusUpdate,
    WorkflowStopSignal
)
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.jobs.protocols import UDPProtocol
from hyperscale.core.snowflake import Snowflake
from hyperscale.core.state import Context
from hyperscale.logging.hyperscale_logging_models import (
    RunDebug,
    RunError,
    RunFatal,
    RunInfo,
    RunTrace,
    StatusUpdate,
    ServerDebug,
    ServerError,
    ServerFatal,
    ServerInfo,
    ServerTrace,
)
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.ui.actions import update_active_workflow_message, update_workflow_executions_total_rate

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


class RemoteGraphController(UDPProtocol[JobContext[Any], JobContext[Any]]):
    def __init__(
        self,
        worker_idx: int | None,
        host: str,
        port: int,
        env: Env,
    ) -> None:
        super().__init__(host, port, env)

        self._workflows = WorkflowRunner(
            env,
            worker_idx,
            self._node_id_base,
        )

        self.acknowledged_starts: set[str] = set()
        self.acknowledged_start_node_ids: set[str] = set()
        self._worker_id = worker_idx

        self._logfile = f"hyperscale.worker.{self._worker_id}.log.json"
        if worker_idx is None:
            self._logfile = "hyperscale.leader.log.json"

        self._results: NodeData[WorkflowResult] = defaultdict(lambda: defaultdict(dict))
        self._errors: NodeData[Exception] = defaultdict(lambda: defaultdict(dict))

        self._run_workflow_run_id_map: NodeData[int] = defaultdict(
            lambda: defaultdict(dict)
        )

        self._node_context: NodeContextSet = defaultdict(Context)
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

        self._step_stats: Dict[int, Dict[str, Dict[int, StepStatsUpdate]]] = (
            defaultdict(
                lambda: defaultdict(
                    lambda: defaultdict(
                        lambda: defaultdict(lambda: {"total": 0, "ok": 0, "err": 0})
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
        self._completion_write_lock: NodeData[asyncio.Lock] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(asyncio.Lock)))
        )

        self._stop_write_lock: NodeData[asyncio.Lock] = (
            defaultdict(lambda: defaultdict(lambda: defaultdict(asyncio.Lock)))
        )

        self._leader_lock: asyncio.Lock | None = None

        # Event-driven completion tracking
        self._workflow_completion_states: Dict[int, Dict[str, WorkflowCompletionState]] = defaultdict(dict)

        # Event-driven worker start tracking
        self._expected_workers: int = 0
        self._workers_ready_event: asyncio.Event | None = None


        self._stop_completion_events: Dict[int, Dict[str, asyncio.Event]] = defaultdict(dict)
        self._stop_expected_nodes: Dict[int, Dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))

        # Event-driven cancellation completion tracking
        # Tracks expected nodes and fires event when all report terminal cancellation status
        self._cancellation_completion_events: Dict[int, Dict[str, asyncio.Event]] = defaultdict(dict)
        self._cancellation_expected_nodes: Dict[int, Dict[str, set[int]]] = defaultdict(lambda: defaultdict(set))
        # Collect errors from nodes that reported FAILED status
        self._cancellation_errors: Dict[int, Dict[str, list[str]]] = defaultdict(lambda: defaultdict(list))

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

        await super().start_server(
            self._logfile,
            cert_path=cert_path,
            key_path=key_path,
            worker_socket=worker_socket,
            worker_server=worker_server,
        )

        default_config = {
            "node_id": self._node_id_base,
            "node_host": self.host,
            "node_port": self.port,
        }

        self._logger.configure(
            name=f"controller",
            path=self._logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (
                    ServerTrace,
                    default_config
                ),
                "debug": (
                    ServerDebug,
                    default_config,
                ),
                "info": (
                    ServerInfo,
                    default_config,
                ),
                "error": (
                    ServerError,
                    default_config,
                ),
                "fatal": (
                    ServerFatal,
                    default_config,
                ),
            },
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
            self._logfile,
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

    def start_controller_cleanup(self):
        self.tasks.run("cleanup_completed_runs")

    async def update_context(
        self,
        run_id: int,
        context: Context,
    ):
        async with self._logger.context(
            name=f"graph_server_{self._node_id_base}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Updating context for run {run_id}",
                name="debug",
            )

            await self._node_context[run_id].copy(context)

    async def create_context_from_external_store(
        self,
        workflow: str,
        run_id: int,
        values: dict[str, Any]
    ):

        if self._node_context.get(run_id) is not None:
            return self._node_context.get(run_id)

        context = self._node_context[run_id]
        self._node_context[run_id] = await context.from_dict(workflow, values)

        return self._node_context[run_id]

    # =========================================================================
    # Event-Driven Workflow Completion
    # =========================================================================

    def register_workflow_completion(
        self,
        run_id: int,
        workflow_name: str,
        expected_workers: int,
    ) -> WorkflowCompletionState:
        """
        Register a workflow for event-driven completion tracking.

        Returns a WorkflowCompletionState that contains:
        - completion_event: Event signaled when all workers complete
        - status_update_queue: Queue for receiving status updates
        """
        state = WorkflowCompletionState(
            expected_workers=expected_workers,
            completion_event=asyncio.Event(),
            status_update_queue=asyncio.Queue(),
            cores_update_queue=asyncio.Queue(),
            completed_count=0,
            failed_count=0,
            step_stats=defaultdict(lambda: {"total": 0, "ok": 0, "err": 0}),
            avg_cpu_usage=0.0,
            avg_memory_usage_mb=0.0,
            workers_completed=0,
            workers_assigned=expected_workers,
        )
        self._workflow_completion_states[run_id][workflow_name] = state
        return state

    def get_workflow_results(
        self,
        run_id: int,
        workflow_name: str,
    ) -> Tuple[Dict[int, WorkflowResult], Context]:
        """Get results for a completed workflow."""
        return (
            self._results[run_id][workflow_name],
            self._node_context[run_id],
        )

    def cleanup_workflow_completion(
        self,
        run_id: int,
        workflow_name: str,
    ) -> None:
        """Clean up completion state for a workflow."""
        if run_id in self._workflow_completion_states:
            self._workflow_completion_states[run_id].pop(workflow_name, None)
            if not self._workflow_completion_states[run_id]:
                self._workflow_completion_states.pop(run_id, None)

    async def submit_workflow_to_workers(
        self,
        run_id: int,
        workflow: Workflow,
        context: Context,
        threads: int,
        workflow_vus: List[int],
        node_ids: List[int] | None = None,
    ):
        """
        Submit a workflow to workers with explicit node targeting.

        Unlike the old version, this does NOT take update callbacks.
        Status updates are pushed to the WorkflowCompletionState queue
        and completion is signaled via the completion_event.

        Args:
            run_id: The run identifier
            workflow: The workflow to submit
            context: The context for the workflow
            threads: Number of workers to submit to
            workflow_vus: VUs per worker
            node_ids: Explicit list of node IDs to target (if None, uses round-robin)
        """
        task_id = self.id_generator.generate()
        default_config = {
            "node_id": self._node_id_base,
            "workflow": workflow.name,
            "run_id": run_id,
            "workflow_vus": workflow.vus,
            "duration": workflow.duration,
        }

        self._logger.configure(
            name=f"workflow_run_{run_id}",
            path=self._logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (RunTrace, default_config),
                "debug": (
                    RunDebug,
                    default_config,
                ),
                "info": (
                    RunInfo,
                    default_config,
                ),
                "error": (
                    RunError,
                    default_config,
                ),
                "fatal": (
                    RunFatal,
                    default_config,
                ),
            },
        )

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Submitting run {run_id} for workflow {workflow.name} with {threads} threads to nodes {node_ids} and {workflow.vus} VUs for {workflow.duration}",
                name="info",
            )

            # Start the status aggregation task
            self.tasks.run(
                "aggregate_status_updates",
                run_id,
                workflow.name,
                run_id=task_id,
            )


            self._stop_expected_nodes[run_id][workflow.name] = set(node_ids)
            self._stop_completion_events[run_id][workflow.name] = asyncio.Event()

            self.tasks.run(
                "wait_stop_signal",
                run_id,
                workflow.name,
            )

            # If explicit node_ids provided, target specific nodes
            # Otherwise fall back to round-robin (for backward compatibility)
            results = await asyncio.gather(
                *[
                    self.submit(
                        run_id,
                        workflow,
                        workflow_vus[idx],
                        node_id,
                        context,
                    )
                    for idx, node_id in enumerate(node_ids)
                ]
            )
            return results

    async def submit_workflow_cancellation(
        self,
        run_id: int,
        workflow_name: str,
        timeout: str = "1m",
    ) -> tuple[dict[WorkflowCancellationStatus, list[WorkflowCancellationUpdate]], list[int]]:
        """
        Submit cancellation requests to all nodes running the workflow.

        This is event-driven - use await_workflow_cancellation() to wait for
        all nodes to report terminal status.

        Args:
            run_id: The run ID of the workflow
            workflow_name: The name of the workflow
            timeout: Graceful timeout for workers to complete in-flight work

        Returns:
            Tuple of (initial_status_counts, expected_nodes):
            - initial_status_counts: Initial responses from cancellation requests
            - expected_nodes: List of node IDs that were sent cancellation requests
        """
        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Requesting cancellation for run {run_id} for workflow {workflow_name}"
            )

            # Only select nodes actually running the workflow
            expected_nodes = [
                node_id for node_id, status in self._statuses[run_id][workflow_name].items()
                if status == WorkflowStatus.RUNNING
            ]

            # Set up event-driven cancellation completion tracking
            self._cancellation_expected_nodes[run_id][workflow_name] = set(expected_nodes)
            self._cancellation_completion_events[run_id][workflow_name] = asyncio.Event()
            self._cancellation_errors[run_id][workflow_name] = []

            initial_cancellation_updates = await asyncio.gather(*[
                self.request_workflow_cancellation(
                    run_id,
                    workflow_name,
                    timeout,
                    node_id
                ) for node_id in expected_nodes
            ])

            cancellation_status_counts: dict[WorkflowCancellationStatus, list[WorkflowCancellationUpdate]] = defaultdict(list)

            for _, res in initial_cancellation_updates:
                update = res.data

                if update.error or update.status == WorkflowCancellationStatus.FAILED.value:
                    cancellation_status_counts[WorkflowCancellationStatus.FAILED].append(update)
                else:
                    cancellation_status_counts[update.status].append(update)

            return (
                cancellation_status_counts,
                expected_nodes,
            )

    async def await_workflow_cancellation(
        self,
        run_id: int,
        workflow_name: str,
        timeout: float | None = None,
    ) -> tuple[bool, list[str]]:
        """
        Wait for all nodes to report terminal cancellation status.

        This is an event-driven wait that fires when all nodes assigned to the
        workflow have reported either CANCELLED or FAILED status via
        receive_cancellation_update.

        Args:
            run_id: The run ID of the workflow
            workflow_name: The name of the workflow
            timeout: Optional timeout in seconds. If None, waits indefinitely.

        Returns:
            Tuple of (success, errors):
            - success: True if all nodes reported terminal status, False if timeout occurred.
            - errors: List of error messages from nodes that reported FAILED status.
        """
        completion_event = self._cancellation_completion_events.get(run_id, {}).get(workflow_name)

        if completion_event is None:
            # No cancellation was initiated for this workflow
            return (True, [])

        timed_out = False
        if not completion_event.is_set():
            try:
                if timeout is not None:
                    await asyncio.wait_for(completion_event.wait(), timeout=timeout)
                else:
                    await completion_event.wait()
            except asyncio.TimeoutError:
                timed_out = True

        # Collect any errors that were reported
        errors = self._cancellation_errors.get(run_id, {}).get(workflow_name, [])

        return (not timed_out, list(errors))
    
    async def await_workflow_stop(
        self,
        run_id: int,
        workflow_name: str,
        timeout: float | None = None,
    ) -> tuple[bool, list[str]]:
        """
        Wait for all nodes to report terminal cancellation status.

        This is an event-driven wait that fires when all nodes assigned to the
        workflow have reported stopped receive_stop.

        Args:
            run_id: The run ID of the workflow
            workflow_name: The name of the workflow
            timeout: Optional timeout in seconds. If None, waits indefinitely.

        Returns:
            Tuple of (success, errors):
            - success: True if all nodes reported terminal status, False if timeout occurred.
            - errors: List of error messages from nodes that reported FAILED status.
        """
        completion_event = self._stop_completion_events.get(run_id, {}).get(workflow_name)

        if completion_event is None:
            # No cancellation was initiated for this workflow
            return (True, [])

        timed_out = False
        if not completion_event.is_set():
            try:
                if timeout is not None:
                    await asyncio.wait_for(completion_event.wait(), timeout=timeout)
                else:
                    await completion_event.wait()
            except asyncio.TimeoutError:
                timed_out = True

        # Collect any errors that were reported
        errors = self._cancellation_errors.get(run_id, {}).get(workflow_name, [])

        return (not timed_out, list(errors))

    async def wait_for_workers(
        self,
        workers: int,
        timeout: float | None = None,
    ) -> bool:
        """
        Wait for all workers to acknowledge startup.

        Uses event-driven architecture - workers signal readiness via
        receive_start_acknowledgement, which sets the event when all
        workers have reported in.

        Returns True if all workers started, False if timeout occurred.
        """
        async with self._logger.context(
            name=f"graph_server_{self._node_id_base}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} waiting for {workers} workers",
                name="info",
            )

            # Initialize event-driven tracking
            self._expected_workers = workers
            self._workers_ready_event = asyncio.Event()

            # Check if workers already acknowledged (race condition prevention)
            async with self._leader_lock:
                if len(self.acknowledged_starts) >= workers:
                    await ctx.log_prepared(
                        message=f"Node {self._node_id_base} at {self.host}:{self.port} all {workers} workers already registered",
                        name="info",
                    )
                    await update_active_workflow_message(
                        "initializing",
                        f"Starting - {workers}/{workers} - threads",
                    )
                    return True

            # Wait for the event with periodic UI updates
            start_time = time.monotonic()
            last_update_time = start_time

            while not self._workers_ready_event.is_set():
                # Calculate remaining timeout
                remaining_timeout = None
                if timeout is not None:
                    elapsed = time.monotonic() - start_time
                    remaining_timeout = timeout - elapsed
                    if remaining_timeout <= 0:
                        await ctx.log_prepared(
                            message=f"Node {self._node_id_base} at {self.host}:{self.port} timed out waiting for workers",
                            name="error",
                        )
                        return False

                # Wait for event with short timeout for UI updates
                wait_time = min(1.0, remaining_timeout) if remaining_timeout else 1.0
                try:
                    await asyncio.wait_for(
                        self._workers_ready_event.wait(),
                        timeout=wait_time,
                    )
                except asyncio.TimeoutError:
                    pass  # Expected - continue to update UI

                # Update UI periodically (every second)
                current_time = time.monotonic()
                if current_time - last_update_time >= 1.0:
                    async with self._leader_lock:
                        acknowledged_count = len(self.acknowledged_starts)
                    await update_active_workflow_message(
                        "initializing",
                        f"Starting - {acknowledged_count}/{workers} - threads",
                    )
                    last_update_time = current_time

            # All workers ready
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} successfully registered {workers} workers",
                name="info",
            )
            await update_active_workflow_message(
                "initializing",
                f"Starting - {workers}/{workers} - threads",
            )

            return True

    @send()
    async def acknowledge_start(
        self,
        leader_address: tuple[str, int],
    ):
        async with self._logger.context(
            name=f"graph_client_{self._node_id_base}",
        ) as ctx:
            start_host, start_port = leader_address

            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} submitted acknowledgement for connection request from node {start_host}:{start_port}",
                name="info",
            )

            return await self.send(
                "receive_start_acknowledgement",
                JobContext((self.host, self.port)),
                target_address=leader_address,
            )

    @send()
    async def submit(
        self,
        run_id: int,
        workflow: Workflow,
        vus: int,
        target_node_id: int | None,
        context: Context,
    ) -> Response[JobContext[WorkflowStatusUpdate]]:
        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Workflow {workflow.name} run {run_id} submitting from node {self._node_id_base} at {self.host}:{self.port} to node {target_node_id}",
                name="debug",
            )

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
                node_id=target_node_id,
            )

            (shard_id, workflow_status) = response

            if workflow_status.data:
                status = workflow_status.data.status
                workflow_name = workflow_status.data.workflow
                run_id = workflow_status.run_id

                # Use full 64-bit node_id from message instead of 10-bit snowflake instance
                node_id = workflow_status.node_id

                self._statuses[run_id][workflow_name][node_id] = (
                    WorkflowStatus.map_value_to_status(status)
                )

                await ctx.log_prepared(
                    message=f"Workflow {workflow.name} run {run_id} submitted from node {self._node_id_base} at {self.host}:{self.port} to node {node_id} with status {status}",
                    name="debug",
                )

            return response

    @send()
    async def submit_stop_request(self):
        async with self._logger.context(
            name=f"graph_server_{self._node_id_base}"
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} submitting request for {len(self._node_host_map)} nodes to stop",
                name="info",
            )

            return await self.broadcast(
                "process_stop_request",
                JobContext(None),
            )

    @send()
    async def push_results(
        self,
        node_id: int,
        results: WorkflowResults,
        run_id: int,
    ) -> Response[JobContext[ReceivedReceipt]]:
        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Workflow {results.workflow} run {run_id} pushing results to Node {node_id}",
                name="debug",
            )

            return await self.send(
                "process_results",
                JobContext(
                    results,
                    run_id=run_id,
                ),
                node_id=node_id,
            )


    @send()
    async def request_workflow_cancellation(
        self,
        run_id: int,
        workflow_name: str,
        graceful_timeout: str,
        node_id: str,
    ) -> Response[JobContext[WorkflowCancellationUpdate]]:
        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Cancelling workflow {workflow_name} run {run_id}",
                name="debug",
            )

            return await self.send(
                "cancel_workflow",
                JobContext(
                    data=WorkflowCancellation(
                        workflow_name=workflow_name,
                        graceful_timeout=TimeParser(graceful_timeout).time,
                    ),
                    run_id=run_id,
                ),
                node_id=node_id,
            )

    @receive()
    async def receive_start_acknowledgement(
        self,
        shard_id: int,
        acknowledgement: JobContext[tuple[str, int]],
    ):
        async with self._logger.context(
            name=f"graph_server_{self._node_id_base}"
        ) as ctx:
            async with self._leader_lock:
                # Use full 64-bit node_id from message instead of 10-bit snowflake instance
                node_id = acknowledgement.node_id

                host, port = acknowledgement.data

                node_addr = f"{host}:{port}"

                await ctx.log_prepared(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} received start acknowledgment from Node at {host}:{port}"
                )

                self.acknowledged_starts.add(node_addr)
                self.acknowledged_start_node_ids.add(node_id)

                # Signal the event if all expected workers have acknowledged
                if (
                    self._workers_ready_event is not None
                    and len(self.acknowledged_starts) >= self._expected_workers
                ):
                    self._workers_ready_event.set()

    @receive()
    async def process_results(
        self,
        shard_id: int,
        workflow_results: JobContext[WorkflowResults],
    ) -> JobContext[ReceivedReceipt]:
        async with self._logger.context(
            name=f"workflow_run_{workflow_results.run_id}",
        ) as ctx:
            # Use full 64-bit node_id from JobContext instead of 10-bit snowflake instance
            node_id = workflow_results.node_id
            snowflake = Snowflake.parse(shard_id)
            timestamp = snowflake.timestamp

            run_id = workflow_results.run_id
            workflow_name = workflow_results.data.workflow

            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} received results for Workflow {workflow_name} run {run_id} from Node {node_id}",
                name="info",
            )

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
                    for _ in self.acknowledged_start_node_ids
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

            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} successfull registered completion for Workflow {workflow_name} run {run_id} from Node {node_id}",
                name="info",
            )

            # Check if all workers have completed and signal the completion event
            completion_state = self._workflow_completion_states.get(run_id, {}).get(workflow_name)
            completions_set = self._completions[run_id][workflow_name]
            if completion_state:
                completions_count = len(completions_set)
                completion_state.workers_completed = completions_count

                # Push cores update to the queue
                try:
                    completion_state.cores_update_queue.put_nowait((
                        completion_state.workers_assigned,
                        completions_count,
                    ))
                except asyncio.QueueFull:
                    pass

                if completions_count >= completion_state.expected_workers:
                    completion_state.completion_event.set()

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
        async with self._logger.context(
            name=f"graph_server_{self._node_id_base}"
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} received remote stop request and is shutting down",
                name="info",
            )

            self.stop()

    @receive()
    async def start_workflow(
        self,
        shard_id: int,
        context: JobContext[WorkflowJob],
    ) -> JobContext[WorkflowStatusUpdate]:
        task_id = self.tasks.create_task_id()

        # Use full 64-bit node_id from JobContext instead of 10-bit snowflake instance
        node_id = context.node_id

        workflow_name = context.data.workflow.name

        default_config = {
            "node_id": self._node_id_base,
            "workflow": context.data.workflow.name,
            "run_id": context.run_id,
            "workflow_vus": context.data.workflow.vus,
            "duration": context.data.workflow.duration,
        }

        self._logger.configure(
            name=f"workflow_run_{context.run_id}",
            path=self._logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (RunTrace, default_config),
                "debug": (
                    RunDebug,
                    default_config,
                ),
                "info": (
                    RunInfo,
                    default_config,
                ),
                "error": (
                    RunError,
                    default_config,
                ),
                "fatal": (
                    RunFatal,
                    default_config,
                ),
            },
        )

        async with self._logger.context(
            name=f"workflow_run_{context.run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Submitting workflow {context.data.workflow.name} run {context.run_id} to Workflow Runner",
                name="info",
            )

            self.tasks.run(
                "await_stop",
                context.run_id,
                node_id,
                context.data.workflow.name,
            )

            self.tasks.run(
                "run_workflow",
                node_id,
                context.run_id,
                context.data,
                run_id=task_id,
            )

            self._run_workflow_run_id_map[context.run_id][workflow_name][self._node_id_base] = task_id

            await ctx.log_prepared(
                message=f"Workflow {context.data.workflow.name} run {context.run_id} starting status update task",
                name="info",
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
    async def cancel_workflow(
        self,
        shard_id: int,
        cancelation: JobContext[WorkflowCancellation]
    ) -> JobContext[WorkflowCancellationUpdate]:

        # Use full 64-bit node_id from JobContext instead of 10-bit snowflake instance
        node_id = cancelation.node_id

        run_id = cancelation.run_id
        workflow_name = cancelation.data.workflow_name

        workflow_run_id = self._run_workflow_run_id_map[run_id][workflow_name].get(self._node_id_base)
        if workflow_run_id is None:
            return JobContext(
                data=WorkflowCancellationUpdate(
                    workflow_name=workflow_name,
                    status=WorkflowCancellationStatus.NOT_FOUND.value,
                ),
                run_id=cancelation.run_id,
            )

        self.tasks.run(
            "cancel_workflow_background",
            run_id,
            node_id,
            workflow_run_id,
            workflow_name,
            cancelation.data.graceful_timeout,
        )

        return JobContext(
            data=WorkflowCancellationUpdate(
                workflow_name=workflow_name,
                status=WorkflowCancellationStatus.REQUESTED.value,
            ),
            run_id=run_id,
        )

    @receive()
    async def receive_cancellation_update(
        self,
        shard_id: int,
        cancellation: JobContext[WorkflowCancellationUpdate]
    ) -> JobContext[WorkflowCancellationUpdate]:
        node_id = cancellation.node_id
        run_id = cancellation.run_id
        workflow_name = cancellation.data.workflow_name
        status = cancellation.data.status

        try:

            terminal_statuses = {
                WorkflowCancellationStatus.CANCELLED.value,
                WorkflowCancellationStatus.FAILED.value,
            }

            if status not in terminal_statuses:
                return JobContext(
                    data=WorkflowCancellationUpdate(
                        workflow_name=workflow_name,
                        status=status,
                    ),
                    run_id=run_id,
                )

            # Terminal status - collect errors if failed
            if status == WorkflowCancellationStatus.FAILED.value:
                error_message = cancellation.data.error
                if error_message:
                    self._cancellation_errors[run_id][workflow_name].append(
                        f"Node {node_id}: {error_message}"
                    )

            # Remove node from expected set and check for completion
            expected_nodes = self._cancellation_expected_nodes[run_id][workflow_name]
            expected_nodes.discard(node_id)

            if len(expected_nodes) == 0:
                completion_event = self._cancellation_completion_events[run_id].get(workflow_name)
                if completion_event is not None and not completion_event.is_set():
                    completion_event.set()

            return JobContext(
                data=WorkflowCancellationUpdate(
                    workflow_name=workflow_name,
                    status=status,
                ),
                run_id=run_id,
            )

        except Exception as err:
            return JobContext(
                data=WorkflowCancellationUpdate(
                    workflow_name=workflow_name,
                    status=cancellation.data.status,
                    error=str(err),
                ),
                run_id=run_id,
            )

    @receive()
    async def receive_stop(
        self,
        shard_id: int,
        stop_signal: JobContext[WorkflowStopSignal]
    ) -> JobContext[WorkflowStopSignal]:
        try:

            # Use full 64-bit node_id from JobContext instead of 10-bit snowflake instance
            node_id = stop_signal.node_id

            run_id = stop_signal.run_id
            workflow_name = stop_signal.data.workflow

            # Remove node from expected set and check for completion
            expected_nodes = self._stop_expected_nodes[run_id][workflow_name]
            expected_nodes.discard(node_id)

            if len(expected_nodes) == 0:
                completion_event = self._stop_completion_events[run_id].get(workflow_name)
                if completion_event is not None and not completion_event.is_set():
                    completion_event.set()
                    workflow_slug = workflow_name.lower()

                    await update_workflow_executions_total_rate(workflow_slug, None, False)



            return JobContext(
                data=WorkflowStopSignal(
                    workflow_name=workflow_name,
                    node_id=node_id,
                ),
                run_id=run_id,
            )

        except Exception as err:
            return JobContext(
                data=WorkflowStopSignal(
                    workflow_name=workflow_name,
                    node_id=node_id,
                ),
                run_id=run_id,
            )

    @receive()
    async def receive_status_update(
        self,
        shard_id: int,
        update: JobContext[WorkflowStatusUpdate],
    ) -> JobContext[ReceivedReceipt]:
        # Use full 64-bit node_id from JobContext instead of 10-bit snowflake instance
        node_id = update.node_id

        run_id = update.run_id
        workflow = update.data.workflow
        status = update.data.status
        completed_count = update.data.completed_count
        failed_count = update.data.failed_count

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} received status update from Node {node_id} for Workflow {workflow} run {run_id}",
                name="debug",
            )

            step_stats = update.data.step_stats

            avg_cpu_usage = update.data.avg_cpu_usage
            avg_memory_usage_mb = update.data.avg_memory_usage_mb

            self._statuses[run_id][workflow][node_id] = (
                WorkflowStatus.map_value_to_status(status)
            )

            await self._completion_write_lock[run_id][workflow][node_id].acquire()

            await ctx.log(
                StatusUpdate(
                    message=f"Node {self._node_id_base} at {self.host}:{self.port} updating running stats for Workflow {workflow} run {run_id}",
                    node_id=node_id,
                    node_host=self.host,
                    node_port=self.port,
                    completed_count=completed_count,
                    failed_count=failed_count,
                    avg_cpu=avg_cpu_usage,
                    avg_mem_mb=avg_memory_usage_mb,
                )
            )

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
        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            try:

                await ctx.log_prepared(
                    message=f"Workflow {job.workflow.name} starting run {run_id} via task on Node {self._node_id_base} at {self.host}:{self.port}",
                    name="trace",
                )

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
            except Exception as err:
                await ctx.log_prepared(
                    message=f"Workflow {job.workflow.name} run {run_id} failed with error: {err}",
                    name="error",
                )

                await self.push_results(
                    node_id,
                    WorkflowResults(
                        job.workflow.name, None, job.context, err, WorkflowStatus.FAILED
                    ),
                    run_id,
                )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 10),
        ),
        trigger="MANUAL",
        repeat="NEVER",
        keep_policy="COUNT",

    )
    async def cancel_workflow_background(
        self,
        run_id: int,
        node_id: int,
        workflow_run_id: str,
        workflow_name: str,
        timeout: int,
    ):
        try:

            self._workflows.request_cancellation()
            await asyncio.wait_for(
                self._workflows.await_cancellation(),
                timeout=timeout,
            )

            await self.send(
                "receive_cancellation_update",
                JobContext(
                    data=WorkflowCancellationUpdate(
                        workflow_name=workflow_name,
                        status=WorkflowCancellationStatus.CANCELLED.value,
                    ),
                    run_id=run_id,
                ),
                node_id=node_id,
            )

        except (
            Exception,
            asyncio.CancelledError,
            asyncio.TimeoutError,
        ) as err:
            await self.send(
                "receive_cancellation_update",
                JobContext(
                    data=WorkflowCancellationUpdate(
                        workflow_name=workflow_name,
                        status=WorkflowCancellationStatus.FAILED.value,
                        error=str(err)
                    ),
                    run_id=run_id,
                ),
                node_id=node_id,
            )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 10),
        ),
        trigger="MANUAL",
        repeat="NEVER",
        max_age="1m",
        keep_policy="COUNT_AND_AGE",          
    )
    async def wait_stop_signal(
        self,
        run_id: str,
        workflow_name: str,
    ):
        await self._stop_completion_events[run_id][workflow_name].wait()

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 10),
        ),
        trigger="MANUAL",
        repeat="NEVER",
        max_age="1m",
        keep_policy="COUNT_AND_AGE",
    )
    async def await_stop(
        self,
        run_id: str,
        node_id: str,
        workflow_name: str,
    ):
        await self._workflows.await_stop()
        await self.send(
            "receive_stop",
            JobContext(
                WorkflowStopSignal(
                    workflow_name,
                    node_id,
                ),
                run_id=run_id,
            ),
            node_id=node_id,
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

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} submitting stat updates for Workflow {workflow_name} run {run_id} to Node {node_id}",
                name="debug",
            )

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

            await self.send(
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
                node_id=node_id,
            )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 10),
        ),
        trigger="MANUAL",
        repeat="ALWAYS",
        schedule="0.05s",
        keep_policy="COUNT",
    )
    async def aggregate_status_updates(
        self,
        run_id: int,
        workflow_name: str,
    ):
        """
        Aggregates status updates from all workers and pushes to the completion state queue.

        This replaces the callback-based get_latest_completed task.
        """
        completion_state = self._workflow_completion_states.get(run_id, {}).get(workflow_name)
        if not completion_state:
            # No completion state registered, stop the task
            self.tasks.stop("aggregate_status_updates")
            return

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} aggregating status updates for Workflow {workflow_name} run {run_id}",
                name="debug",
            )

            workflow_status = WorkflowStatus.SUBMITTED

            status_counts = Counter(self._statuses[run_id][workflow_name].values())
            for status, count in status_counts.items():
                if count == completion_state.expected_workers:
                    workflow_status = status
                    break

            completed_count = sum(self._completed_counts[run_id][workflow_name].values())
            failed_count = sum(self._failed_counts[run_id][workflow_name].values())

            step_stats: StepStatsUpdate = defaultdict(
                lambda: {
                    "ok": 0,
                    "total": 0,
                    "err": 0,
                }
            )

            for _, stats_update in self._step_stats[run_id][workflow_name].items():
                for hook, stats_set in stats_update.items():
                    for stats_type, stat in stats_set.items():
                        step_stats[hook][stats_type] += stat

            cpu_usage_stats = self._cpu_usage_stats[run_id][workflow_name].values()
            avg_cpu_usage = 0
            if len(cpu_usage_stats) > 0:
                avg_cpu_usage = statistics.mean(cpu_usage_stats)

            memory_usage_stats = self._memory_usage_stats[run_id][workflow_name].values()
            avg_mem_usage_mb = 0
            if len(memory_usage_stats) > 0:
                avg_mem_usage_mb = statistics.mean(memory_usage_stats)

            workers_completed = len(self._completions[run_id][workflow_name])

            # Update the completion state
            completion_state.completed_count = completed_count
            completion_state.failed_count = failed_count
            completion_state.step_stats = step_stats
            completion_state.avg_cpu_usage = avg_cpu_usage
            completion_state.avg_memory_usage_mb = avg_mem_usage_mb
            completion_state.workers_completed = workers_completed

            # Push update to the queue (non-blocking)
            status_update = WorkflowStatusUpdate(
                workflow_name,
                workflow_status,
                completed_count=completed_count,
                failed_count=failed_count,
                step_stats=step_stats,
                avg_cpu_usage=avg_cpu_usage,
                avg_memory_usage_mb=avg_mem_usage_mb,
                workers_completed=workers_completed,
            )

            try:
                completion_state.status_update_queue.put_nowait(status_update)
            except asyncio.QueueFull:
                # Queue is full, skip this update
                pass

            # Stop the task if workflow is complete
            if completion_state.completion_event.is_set():
                self.tasks.stop("aggregate_status_updates")

    @task(
        trigger="MANUAL",
        max_age="5m",
        keep_policy="COUNT_AND_AGE",
    )
    async def cleanup_completed_runs(self) -> None:
        """
        Clean up data for workflows where all nodes have reached terminal state.

        For each (run_id, workflow_name) pair, if ALL nodes tracking that workflow
        are in terminal state (COMPLETED, REJECTED, UNKNOWN, FAILED), clean up
        that workflow's data from all data structures.
        """
        try:

            async with self._logger.context(
                name=f"controller",
            ) as ctx:

                terminal_statuses = {
                    WorkflowStatus.COMPLETED,
                    WorkflowStatus.REJECTED,
                    WorkflowStatus.UNKNOWN,
                    WorkflowStatus.FAILED,
                }

                # Data structures keyed by run_id -> workflow_name -> ...
                workflow_level_data: list[NodeData[Any]] = [
                    self._results,
                    self._errors,
                    self._run_workflow_run_id_map,
                    self._statuses,
                    self._run_workflow_expected_nodes,
                    self._completions,
                    self._completed_counts,
                    self._failed_counts,
                    self._step_stats,
                    self._cpu_usage_stats,
                    self._memory_usage_stats,
                    self._completion_write_lock,
                    self._cancellation_completion_events,
                    self._cancellation_expected_nodes,
                    self._cancellation_errors,
                ]

                # Data structures keyed only by run_id (cleaned when all workflows done)
                run_level_data = [
                    self._node_context,
                    self._workflow_completion_states,
                ]

                # Collect (run_id, workflow_name) pairs safe to clean up
                workflows_to_cleanup: list[tuple[int, str]] = []

                for run_id, workflows in list(self._statuses.items()):
                    for workflow_name, node_statuses in list(workflows.items()):
                        if node_statuses and all(
                            status in terminal_statuses
                            for status in node_statuses.values()
                        ):
                            workflows_to_cleanup.append((run_id, workflow_name))

                # Clean up each completed workflow
                for run_id, workflow_name in workflows_to_cleanup:
                    for data in workflow_level_data:
                        if run_id in data:
                            data[run_id].pop(workflow_name, None)

                # Clean up empty run_ids (including run-level data like _node_context)
                cleaned_run_ids = {run_id for run_id, _ in workflows_to_cleanup}
                for run_id in cleaned_run_ids:
                    if run_id in self._statuses and not self._statuses[run_id]:

                        workflow_level_data.extend(run_level_data)

                        for data in workflow_level_data:
                            data.pop(run_id, None)

                await ctx.log_prepared(
                    message='Completed cleanup cycle',
                    name='info'
                )

        except Exception as err:
            async with self._logger.context(
                name=f"controller",
            ) as ctx:
                await ctx.log_prepared(
                    message=f'Encountered unknown error running cleanup - {str(err)}',
                    name='error',
                )

    async def close(self) -> None:
        await super().close()
        await self._workflows.close()

    def abort(self) -> None:
        super().abort()
        self._workflows.abort()

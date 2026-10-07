import asyncio
import functools
import os
import statistics
from collections import Counter, defaultdict
from socket import socket
from typing import Any, Callable, Dict, List, Set, Tuple, TypeVar

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
    Message,
    ReceivedReceipt,
    Response,
    StepStatsUpdate,
    WorkflowCancellation,
    WorkflowCancellationStatus,
    WorkflowCancellationUpdate,
    WorkflowThrottle,
    WorkflowThrottleUpdate,
    WorkflowCompletionState,
    WorkflowJob,
    WorkflowReady,
    WorkflowRelease,
    WorkflowResults,
    WorkflowRunControl,
    WorkflowStartBarrier,
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
        *,
        loop: "asyncio.AbstractEventLoop | None" = None,
        transport_factory=None,
        on_start_acknowledged: Callable[[int], None] | None = None,
    ) -> None:
        # Phase 6 SIM seams forwarded to the ``UDPProtocol`` base: under
        # SIM each pool executor runs in its own process (multi-process
        # preserved) on an injected ``SimulationLoop`` with the
        # coordinator's ``CrossProcessTransport``; REAL passes neither and
        # binds a real UDP socket as before.
        super().__init__(
            host, port, env, loop=loop, transport_factory=transport_factory
        )

        self._workflows = WorkflowRunner(
            env,
            worker_idx,
            self._node_id_base,
            # Host-telemetry monitors sample through run_in_executor —
            # banned and non-deterministic under SIM (see WorkflowRunner).
            monitors_enabled=transport_factory is None,
            # Byte-identical replay needs a fixed step order; real runs
            # skip the per-request sorting (see WorkflowRunner).
            deterministic_step_order=transport_factory is not None,
        )

        self.acknowledged_starts: set[str] = set()
        self.acknowledged_start_node_ids: set[str] = set()
        # Told the node id of every executor ready handshake (start
        # acknowledgement) — how the leader's provisioner returns a
        # respawned executor's slot only once the replacement is ready.
        self._on_start_acknowledged: Callable[[int], None] = (
            on_start_acknowledged
            if on_start_acknowledged is not None
            else self._ignore_start_acknowledgement
        )
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

        # Synchronized run start, keyed by (run_id, workflow_name). The
        # leader holds each run's start barrier only until it releases the
        # run; a node holds its run's start gate only while the run waits
        # at it. Plain dicts: a lookup for an unknown run creates nothing.
        self._workflow_start_barriers: Dict[tuple[int, str], WorkflowStartBarrier] = {}
        self._workflow_start_gates: Dict[tuple[int, str], asyncio.Event] = {}

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

    async def seed_run_context(
        self,
        run_id: int,
        context_by_workflow: dict[str, dict[str, Any]],
    ) -> Context:
        """Start ``run_id`` from a context received from elsewhere: every
        workflow namespace of ``context_by_workflow`` goes into the run's
        context."""
        context = self._node_context[run_id]
        for workflow_name, values in context_by_workflow.items():
            await context.from_dict(workflow_name, values)

        return context

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

        Returns once the run has started: every node sets up, reports
        ready, and waits, and the leader starts them together -- or, past
        the workflow's timeout, starts those ready so far and each later
        one as it reports (see ``_release_workflow_start``).

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
                task_id,
                run_id=task_id,
            )


            self._stop_expected_nodes[run_id][workflow.name] = set(node_ids)
            self._stop_completion_events[run_id][workflow.name] = asyncio.Event()

            self.tasks.run(
                "wait_stop_signal",
                run_id,
                workflow.name,
            )

            # Registered before the first submission: a node can finish
            # setting up before the last submission has been answered.
            start_barrier = WorkflowStartBarrier(set(node_ids))
            self._workflow_start_barriers[(run_id, workflow.name)] = start_barrier

            try:
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

                await self._release_workflow_start(
                    run_id,
                    workflow.name,
                    start_barrier,
                    TimeParser(workflow.timeout).time,
                )

            finally:
                self._remove_workflow_start_barrier(
                    run_id,
                    workflow.name,
                    start_barrier,
                )

            return results

    async def _release_workflow_start(
        self,
        run_id: int,
        workflow_name: str,
        start_barrier: WorkflowStartBarrier,
        setup_timeout: float,
    ) -> None:
        """
        Wait until every node the run was submitted to has set up -- or
        has already finished, as a node whose setup failed has -- then
        start the ready nodes together. Waits at most ``setup_timeout``,
        the workflow's timeout: past it, the nodes ready so far start, and
        each later one starts as soon as it reports ready.
        """
        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            try:
                await asyncio.wait_for(
                    start_barrier.all_reported.wait(),
                    timeout=setup_timeout,
                )

            except asyncio.TimeoutError:
                await ctx.log_prepared(
                    message=(
                        f"Workflow {workflow_name} run {run_id} starting {len(start_barrier.ready_nodes)} of "
                        f"{len(start_barrier.expected_nodes)} nodes after its {setup_timeout}s setup timeout; "
                        "the rest start as they report ready"
                    ),
                    name="error",
                )

            # Removed before releasing: a node reporting ready from here on
            # is answered released and starts at once, so none is missed
            # between this snapshot and the releases below.
            self._remove_workflow_start_barrier(run_id, workflow_name, start_barrier)

            # Sent in the background: the run has started once they are on
            # their way, and a lost acknowledgement -- which send retries
            # for seconds -- must not hold the run's clocks. Sorted for a
            # deterministic release order (SIM replay).
            ready_nodes = sorted(start_barrier.ready_nodes)
            self.tasks.run("send_workflow_releases", run_id, workflow_name, ready_nodes)

            await ctx.log_prepared(
                message=(
                    f"Workflow {workflow_name} run {run_id} released {len(ready_nodes)} of "
                    f"{len(start_barrier.expected_nodes)} nodes"
                ),
                name="info",
            )

    def _remove_workflow_start_barrier(
        self,
        run_id: int,
        workflow_name: str,
        start_barrier: WorkflowStartBarrier,
    ) -> None:
        # Only this run's barrier, never one registered after it.
        barrier_key = (run_id, workflow_name)
        if self._workflow_start_barriers.get(barrier_key) is start_barrier:
            del self._workflow_start_barriers[barrier_key]

    async def submit_workflow_throttle(
        self,
        run_id: int,
        workflow_name: str,
        scale: float | None,
    ) -> list[WorkflowThrottleUpdate]:
        """AD-41 THROTTLE: apply ``scale`` (None: release) to the workflow on
        every node running it; each node's answer, in node order."""
        running_nodes = [
            node_id
            for node_id, status in self._statuses[run_id][workflow_name].items()
            if status == WorkflowStatus.RUNNING
        ]
        responses = await asyncio.gather(
            *[
                self.request_workflow_throttle(run_id, workflow_name, scale, node_id)
                for node_id in running_nodes
            ]
        )
        return [response.data for _, response in responses]

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

            # Wait for the event with periodic UI updates. Loop time,
            # not wall time — identical on a real loop, virtual under
            # SIM, so both the timeout math and the UI-throttle branch
            # follow the (deterministic) timeline instead of host load.
            start_time = self._loop.time()
            last_update_time = start_time

            while not self._workers_ready_event.is_set():
                # Calculate remaining timeout
                remaining_timeout = None
                if timeout is not None:
                    elapsed = self._loop.time() - start_time
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
                current_time = self._loop.time()
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

    @staticmethod
    def _ignore_start_acknowledgement(node_id: int) -> None:
        """The acknowledgement listener of a controller given none."""

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
                self._on_start_acknowledged(node_id)

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
            # Stored as a WorkflowStatus like every other writer of
            # ``_statuses``: results carry the status's string value, and
            # a raw string made aggregation raise (WorkflowStatusUpdate
            # takes the enum) once every node had reported.
            self._statuses[run_id][workflow_name][node_id] = (
                WorkflowStatus.map_value_to_status(status)
            )
            self._errors[run_id][workflow_name][node_id] = Exception(error)

            self._completions[run_id][workflow_name].add(node_id)

            # A node that finishes before reporting ready -- its setup
            # failed -- no longer holds back the run's start.
            if (
                start_barrier := self._workflow_start_barriers.get((run_id, workflow_name))
            ) is not None:
                start_barrier.mark_finished(node_id)

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
                    # Each node sent its terminal status before its results
                    # (run_workflow); publish the aggregate now so the run's
                    # final counts reach its consumer with the completion,
                    # not on the next aggregation tick -- after the consumer
                    # stopped reading.
                    self._publish_aggregated_status(run_id, workflow_name, completion_state)
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

            # Stop once this request's reply has gone out: stopping here cancels
            # every pending reply, this one included, which leaves the sender
            # retrying until its timeouts and backoff run out (11s by default).
            asyncio.current_task().add_done_callback(lambda _: self.stop())

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

            # This submission's own stop signal, shared by its stop report
            # and its run: nothing left over from an earlier run can set it.
            stop_event = asyncio.Event()
            # Registered before the run-id map below makes the run
            # reachable by a cancel; run_workflow releases it once the
            # run's final stats and results are out.
            control = self._workflows.register_run(context.run_id, workflow_name)

            self.tasks.run(
                "await_stop",
                context.run_id,
                node_id,
                context.data.workflow.name,
                stop_event,
            )

            self.tasks.run(
                "run_workflow",
                node_id,
                context.run_id,
                context.data,
                stop_event,
                control,
                task_id,
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
                task_id,
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

    @send()
    async def request_workflow_throttle(
        self,
        run_id: int,
        workflow_name: str,
        scale: float | None,
        node_id: str,
    ) -> Response[JobContext[WorkflowThrottleUpdate]]:
        return await self.send(
            "throttle_workflow",
            JobContext(
                data=WorkflowThrottle(workflow_name=workflow_name, scale=scale),
                run_id=run_id,
            ),
            node_id=node_id,
        )

    @receive()
    async def throttle_workflow(
        self,
        shard_id: int,
        throttle: JobContext[WorkflowThrottle],
    ) -> JobContext[WorkflowThrottleUpdate]:
        """Apply a throttle or release to this node's run of the workflow."""
        run_id = throttle.run_id
        workflow_name = throttle.data.workflow_name
        if throttle.data.scale is None:
            update = WorkflowThrottleUpdate(
                workflow_name=workflow_name,
                applied=self._workflows.release_workflow_throttle(run_id, workflow_name),
            )
        else:
            concurrency_cap = self._workflows.throttle_workflow(run_id, workflow_name, throttle.data.scale)
            update = WorkflowThrottleUpdate(
                workflow_name=workflow_name,
                applied=concurrency_cap is not None,
                concurrency_cap=concurrency_cap,
            )
        return JobContext(data=update, run_id=run_id)

    @send()
    async def acknowledge_workflow_ready(
        self,
        leader_node_id: int,
        run_id: int,
        workflow_name: str,
    ) -> Response[JobContext[WorkflowRelease]]:
        return await self.send(
            "receive_workflow_ready",
            JobContext(
                data=WorkflowReady(workflow_name),
                run_id=run_id,
            ),
            node_id=leader_node_id,
        )

    @send()
    async def request_workflow_release(
        self,
        run_id: int,
        workflow_name: str,
        node_id: int,
    ) -> Response[JobContext[ReceivedReceipt]]:
        return await self.send(
            "release_workflow",
            JobContext(
                data=WorkflowRelease(workflow_name, released=True),
                run_id=run_id,
            ),
            node_id=node_id,
        )

    @receive()
    async def receive_workflow_ready(
        self,
        shard_id: int,
        ready: JobContext[WorkflowReady],
    ) -> JobContext[WorkflowRelease]:
        """
        A node finished setting up its run of the workflow and waits to
        start. Recorded on the run's start barrier; when the run has none
        -- already released, or submitted without one -- the answer
        releases the node at once.
        """
        run_id = ready.run_id
        workflow_name = ready.data.workflow_name

        start_barrier = self._workflow_start_barriers.get((run_id, workflow_name))
        if start_barrier is not None:
            start_barrier.mark_ready(ready.node_id)

        else:
            # Released already: this answer says so, and so does a release
            # of its own -- either one starts the node, so a lost answer
            # cannot leave it waiting out its timeout.
            self.tasks.run("send_workflow_releases", run_id, workflow_name, [ready.node_id])

        return JobContext(
            data=WorkflowRelease(
                workflow_name,
                released=start_barrier is None,
            ),
            run_id=run_id,
        )

    @receive()
    async def release_workflow(
        self,
        shard_id: int,
        release: JobContext[WorkflowRelease],
    ) -> JobContext[ReceivedReceipt]:
        """
        Start this node's run of the workflow, waiting at its start gate.
        A release for a run no longer waiting -- started, finished,
        cancelled, or a retried release -- changes nothing.
        """
        run_id = release.run_id
        workflow_name = release.data.workflow_name

        self._open_workflow_start_gate(run_id, workflow_name)

        return JobContext(
            data=ReceivedReceipt(
                workflow_name,
                release.node_id,
            ),
            run_id=run_id,
        )

    def _open_workflow_start_gate(
        self,
        run_id: int,
        workflow_name: str,
    ) -> None:
        if (start_gate := self._workflow_start_gates.get((run_id, workflow_name))) is not None:
            start_gate.set()

    async def _await_workflow_release(
        self,
        leader_node_id: int,
        run_id: int,
        workflow_name: str,
        release_timeout: float,
    ) -> None:
        """
        This node's start gate for its run of the workflow: report the run
        set up to the leader, then wait for the leader to start every
        node's run together. The gate opens on the leader's release, or on
        its answer that the run is already released -- whichever arrives
        first. Waits at most ``release_timeout`` -- the workflow's timeout,
        which also bounds the leader's wait -- for a release that never
        arrives: past it the run starts unreleased rather than never.
        """
        gate_key = (run_id, workflow_name)
        start_gate = asyncio.Event()
        self._workflow_start_gates[gate_key] = start_gate

        # Reported in the background: the release can arrive before the
        # report's answer does -- or the answer can be lost while send
        # retries it for seconds -- and the run must start on the release.
        self.tasks.run(
            "report_workflow_ready",
            leader_node_id,
            run_id,
            workflow_name,
            start_gate,
        )

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            try:
                await asyncio.wait_for(
                    start_gate.wait(),
                    timeout=release_timeout,
                )

            except asyncio.TimeoutError:
                await ctx.log_prepared(
                    message=(
                        f"Workflow {workflow_name} run {run_id} on Node {self._node_id_base} received no "
                        f"release from Node {leader_node_id} within {release_timeout}s and is starting without one"
                    ),
                    name="error",
                )

            finally:
                # Only this run's gate: a replacing run may have registered its own.
                if self._workflow_start_gates.get(gate_key) is start_gate:
                    del self._workflow_start_gates[gate_key]

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
        # Use full 64-bit node_id from JobContext instead of 10-bit snowflake instance
        node_id = stop_signal.node_id

        run_id = stop_signal.run_id
        workflow_name = stop_signal.data.workflow

        try:
            # Remove node from expected set and check for completion
            expected_nodes = self._stop_expected_nodes[run_id][workflow_name]
            expected_nodes.discard(node_id)

            if len(expected_nodes) == 0:
                completion_event = self._stop_completion_events[run_id].get(workflow_name)
                if completion_event is not None and not completion_event.is_set():
                    completion_event.set()
                    workflow_slug = workflow_name.lower()

                    await update_workflow_executions_total_rate(workflow_slug, None, False)

        except Exception as err:
            async with self._logger.context(
                name=f"workflow_run_{run_id}",
            ) as ctx:
                await ctx.log_prepared(
                    message=(
                        f"Node {self._node_id_base} at {self.host}:{self.port} failed to record the stop of "
                        f"Workflow {workflow_name} run {run_id} on Node {node_id}: {err}"
                    ),
                    name="error",
                )

        # Always answered: a handler that raises sends no reply, and the
        # stopping node retries until its send times out.
        return JobContext(
            data=WorkflowStopSignal(
                workflow=workflow_name,
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
        stop_event: asyncio.Event,
        control: WorkflowRunControl,
        task_id: int,
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
                    await_start=functools.partial(
                        self._await_workflow_release,
                        node_id,
                        run_id,
                        job.workflow.name,
                        TimeParser(job.workflow.timeout).time,
                    ),
                    stop_event=stop_event,
                    control=control,
                )

                if context is None:
                    context = job.context

                # The run's final counts go out before its results: the
                # receiving node signals completion on the results, and the
                # periodic status push could otherwise land after it --
                # leaving the run's consumer with the last in-flight counts.
                await self._send_workflow_status_update(
                    node_id, run_id, job.workflow.name
                )
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
            except asyncio.CancelledError:
                # Hard cancellation (the graceful window expired and
                # ``hard_cancel`` cancelled the run task). The consumer
                # waiting on ``push_results`` — the worker's
                # execute_workflow — MUST still receive a terminal, or
                # the workflow stays in its active set forever (the
                # first hard-cancel iteration replaced zombie execution
                # with a silent hang: cores freed, workflow never
                # drained, client stats never settled). Report the
                # CANCELLED terminal with the pre-run context, then
                # re-raise to finish dying.
                await self.push_results(
                    node_id,
                    WorkflowResults(
                        job.workflow.name,
                        None,
                        job.context,
                        None,
                        WorkflowStatus.CANCELLED,
                    ),
                    run_id,
                )
                raise
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

            finally:
                # The runner sets it as the run's load ends; this covers a
                # submission that ends before reaching the runner.
                stop_event.set()
                # The run's final stats and results are out (or the run
                # never reached the runner): a cancel no longer finds it
                # -- unless a resubmission took its place in the run-id
                # map -- and nothing reads its state now.
                run_task_ids = self._run_workflow_run_id_map.get(run_id, {})
                node_task_ids = run_task_ids.get(job.workflow.name, {})
                if node_task_ids.get(self._node_id_base) == task_id:
                    del node_task_ids[self._node_id_base]
                    if not node_task_ids:
                        del run_task_ids[job.workflow.name]

                    if not run_task_ids:
                        del self._run_workflow_run_id_map[run_id]

                self._workflows.release_run(run_id, job.workflow.name, control)

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

            self._workflows.request_cancellation(run_id, workflow_name)
            # A run still waiting at its start gate would hold the
            # cancellation until its release: open the gate so it starts
            # with cancellation already requested and winds down at once.
            self._open_workflow_start_gate(run_id, workflow_name)
            try:
                await asyncio.wait_for(
                    self._workflows.await_cancellation(run_id, workflow_name),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                # The graceful window expired — for ACTION workflows the
                # graceful flag is a no-op (only a TEST workflow's VUs
                # consult it), so without escalation "cancel" meant
                # waiting out the workflow's natural length while its
                # cores stayed busy. Hard-stop the run and wait briefly
                # for the runner's forced convergence (hard_cancel sets
                # the completion events itself, so this second wait is
                # bounded by event delivery, not by execution). The
                # runner keys its run state by the ORIGINAL run_id
                # (``workflow_run_id`` is the taskex task id used for
                # background-task bookkeeping, not a runner key).
                self._workflows.hard_cancel(run_id, workflow_name)
                await asyncio.wait_for(
                    self._workflows.await_cancellation(run_id, workflow_name),
                    timeout=5.0,
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
            os.getenv("HYPERSCALE_MAX_JOBS", 100),
        ),
        trigger="MANUAL",
        repeat="NEVER",
        max_age="1m",
        keep_policy="COUNT_AND_AGE",
    )
    async def report_workflow_ready(
        self,
        leader_node_id: int,
        run_id: int,
        workflow_name: str,
        start_gate: asyncio.Event,
    ):
        """Report this node's run of the workflow set up to the leader; an
        answer that the run is already released opens its start gate."""
        _, reply = await self.acknowledge_workflow_ready(
            leader_node_id,
            run_id,
            workflow_name,
        )

        if isinstance(reply, JobContext):
            if reply.data.released:
                start_gate.set()

            return

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=(
                    f"Workflow {workflow_name} run {run_id} on Node {self._node_id_base} could not report "
                    f"ready to Node {leader_node_id} ({reply.error if isinstance(reply, Message) else 'no reply'}); "
                    "it starts on the leader's release or once its own wait expires"
                ),
                name="error",
            )

    @task(
        keep=int(
            os.getenv("HYPERSCALE_MAX_JOBS", 100),
        ),
        trigger="MANUAL",
        repeat="NEVER",
        max_age="1m",
        keep_policy="COUNT_AND_AGE",
    )
    async def send_workflow_releases(
        self,
        run_id: int,
        workflow_name: str,
        node_ids: list[int],
    ):
        """Start the run of the workflow on ``node_ids``. A node the release
        cannot reach starts on its answer to its own report, or once its
        own wait expires."""
        releases = await asyncio.gather(
            *[
                self.request_workflow_release(run_id, workflow_name, node_id)
                for node_id in node_ids
            ]
        )

        unreleased = [
            (node_id, reply)
            for node_id, (_, reply) in zip(node_ids, releases)
            if not isinstance(reply, JobContext)
        ]
        if not unreleased:
            return

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            for node_id, reply in unreleased:
                await ctx.log_prepared(
                    message=(
                        f"Workflow {workflow_name} run {run_id} could not release Node {node_id} "
                        f"({reply.error if isinstance(reply, Message) else 'no reply'})"
                    ),
                    name="error",
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
    async def await_stop(
        self,
        run_id: str,
        node_id: str,
        workflow_name: str,
        stop_event: asyncio.Event,
    ):
        await stop_event.wait()
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
        task_id: int,
    ):
        workflow_name = job.workflow.name

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} submitting stat updates for Workflow {workflow_name} run {run_id} to Node {node_id}",
                name="debug",
            )

            status = await self._send_workflow_status_update(
                node_id, run_id, workflow_name
            )

            # A terminal status, or a run already released (its final
            # status went out with run_workflow): the pushes are done.
            if status is None or status in [
                WorkflowStatus.COMPLETED,
                WorkflowStatus.REJECTED,
                WorkflowStatus.FAILED,
                WorkflowStatus.CANCELLED,
            ]:
                # Stop THIS workflow's push schedule only: the schedule
                # was started under the same task id run_workflow was
                # (start_workflow's create_task_id).
                self.tasks.stop("push_workflow_status_update", task_id)

    async def _send_workflow_status_update(
        self,
        node_id: int,
        run_id: int,
        workflow_name: str,
    ) -> WorkflowStatus | None:
        """Send this node's current status and counts for the run's
        workflow to ``node_id``; returns the status sent -- None, sending
        nothing, once the run is released."""
        if (
            running_stats := self._workflows.get_running_workflow_stats(
                run_id,
                workflow_name,
            )
        ) is None:
            return None

        (
            status,
            completed_count,
            failed_count,
            step_stats,
        ) = running_stats

        avg_cpu_usage, avg_mem_usage = self._workflows.get_system_stats(
            run_id,
            workflow_name,
        )

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
        return status

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
        schedule_id: int,
    ):
        """
        Aggregates status updates from all workers and pushes to the completion state queue.

        This replaces the callback-based get_latest_completed task.
        """
        completion_state = self._workflow_completion_states.get(run_id, {}).get(workflow_name)
        if not completion_state:
            # No completion state registered — stop THIS workflow's
            # aggregation schedule only.
            self.tasks.stop("aggregate_status_updates", schedule_id)
            return

        async with self._logger.context(
            name=f"workflow_run_{run_id}",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Node {self._node_id_base} at {self.host}:{self.port} aggregating status updates for Workflow {workflow_name} run {run_id}",
                name="debug",
            )

            self._publish_aggregated_status(run_id, workflow_name, completion_state)

            # Stop THIS workflow's aggregation once it is complete (a
            # name-wide stop ended every concurrent workflow's).
            if completion_state.completion_event.is_set():
                self.tasks.stop("aggregate_status_updates", schedule_id)

    def _publish_aggregated_status(
        self,
        run_id: int,
        workflow_name: str,
        completion_state: WorkflowCompletionState,
    ) -> None:
        """Fold every node's latest status into the completion state and
        queue the aggregate for the run's consumer."""
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

        total_cpu_usage = sum(cpu_usage_stats)
        total_mem_usage_mb = sum(memory_usage_stats)

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
            total_cpu_usage=total_cpu_usage,
            total_memory_usage_mb=total_mem_usage_mb,
        )

        try:
            completion_state.status_update_queue.put_nowait(status_update)
        except asyncio.QueueFull:
            # Queue is full, skip this update
            pass

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

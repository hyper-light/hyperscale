"""
Hyperscale Client for Job Submission - Composition Root.

A thin orchestration layer that delegates to specialized modules.

Usage:
    client = HyperscaleClient(
        host='127.0.0.1',
        port=8500,
        managers=[('127.0.0.1', 9000)],
    )
    await client.start()

    job_id = await client.submit_job(
        workflows=[MyWorkflow],
        vus=10,
        timeout_seconds=60.0,
    )

    result = await client.wait_for_job(job_id)
    await client.stop()
"""

from collections.abc import AsyncIterator
from typing import Callable

from hyperscale.distributed.cluster import ClusterJoinError, decode_join_message
from hyperscale.distributed.cluster.models import (
    ClusterLeaveReply,
    ClusterLeaveRequest,
    ClusterMemberId,
    ClusterMetricsReply,
    ClusterModeReply,
    ClusterModeRequest,
    ClusterResizeReply,
    ClusterResizeRequest,
    ClusterStatusReply,
    ClusterStatusRequest,
    ClusterWatchReply,
    ClusterWatchRequest,
)
from hyperscale.distributed.server import tcp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)
from hyperscale.distributed.models import (
    JobStatusQuery,
    ReadConsistency,
    JobStatusPush,
    NodeJoinRequest,
    NodeJoinResponse,
    ReporterResultPush,
    WorkflowResultPush,
    ManagerPingResponse,
    GatePingResponse,
    WorkflowStatusInfo,
    DatacenterListResponse,
    JobCancelResponse,
    GlobalJobStatus,
    RegisterCallback,
    RegisterCallbackResponse,
)
from hyperscale.distributed.env.env import Env
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning
from hyperscale.distributed.resources.resource_budget import ResourceBudget
from hyperscale.distributed.runtime import (
    Clock,
    Random,
    RealClock,
    TransportFactory,
)
from hyperscale.distributed.reliability.rate_limiting import (
    AdaptiveRateLimiter,
    AdaptiveRateLimitConfig,
)
from hyperscale.distributed.reliability.overload import HybridOverloadDetector

# Import all client modules
from hyperscale.distributed.idempotency.idempotency_key import (
    IdempotencyKeyGenerator,
)
from hyperscale.distributed.jobs.logical_id_generator import (
    LogicalIdGenerator,
)
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.nodes.client.targets import ClientTargetSelector
from hyperscale.distributed.nodes.client.protocol import ClientProtocol
from hyperscale.distributed.nodes.client.leadership import ClientLeadershipTracker
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.nodes.client.submission import ClientJobSubmitter
from hyperscale.distributed.nodes.client.cancellation import ClientCancellationManager
from hyperscale.distributed.nodes.client.reporting import ClientReportingManager
from hyperscale.distributed.nodes.client.discovery import ClientDiscovery

# Import all TCP handlers
from hyperscale.distributed.nodes.client.handlers import (
    JobStatusPushHandler,
    JobBatchPushHandler,
    JobFinalResultHandler,
    GlobalJobResultHandler,
    ReporterResultPushHandler,
    WorkflowResultPushHandler,
    WindowedStatsPushHandler,
    CancellationCompleteHandler,
    GateLeaderTransferHandler,
    ManagerLeaderTransferHandler,
)

# Import client result models
from hyperscale.distributed.models import (
    ClientReporterResult,
    ClientWorkflowDCResult,
    ClientWorkflowResult,
    ClientJobResult,
)
from hyperscale.logging import Logger

# Type aliases for backwards compatibility
ReporterResult = ClientReporterResult
WorkflowDCResultClient = ClientWorkflowDCResult
WorkflowResult = ClientWorkflowResult
JobResult = ClientJobResult


class HyperscaleClient(MercurySyncBaseServer):
    """
    Client for submitting jobs and receiving status updates.

    Thin orchestration layer that delegates to specialized modules:
    - ClientConfig: Configuration
    - ClientState: Mutable state
    - ClientTargetSelector: Target selection and routing
    - ClientProtocol: Protocol version negotiation
    - ClientLeadershipTracker: Leadership transfer handling
    - ClientJobTracker: Job lifecycle tracking
    - ClientJobSubmitter: Job submission with retry
    - ClientCancellationManager: Job cancellation
    - ClientReportingManager: Local reporter submission
    - ClientDiscovery: Ping and query operations
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8500,
        env: Env | None = None,
        managers: list[tuple[str, int]] | None = None,
        gates: list[tuple[str, int]] | None = None,
        *,
        clock: Clock | None = None,
        random_source: Random | None = None,
        transport_factory: TransportFactory | None = None,
    ):
        """
        Initialize the client.

        Args:
            host: Local host to bind for receiving push notifications
            port: Local TCP port for receiving push notifications
            env: Environment configuration
            managers: List of manager (host, port) addresses
            gates: List of gate (host, port) addresses
            clock: Phase 6 SIM seam — virtual clock (None in REAL mode)
            random_source: Phase 6 SIM seam — seeded random (None in REAL mode)
            transport_factory: Phase 6 SIM seam — simulation transport in
                place of real sockets (None in REAL mode)
        """
        env = env or Env()

        super().__init__(
            host=host,
            tcp_port=port,
            udp_port=port + 1,  # UDP not used but required by base
            env=env,
            clock=clock,
            random_source=random_source,
            transport_factory=transport_factory,
        )

        # Logger used by every client submodule. The base class also creates
        # `_tcp_logger` / `_udp_logger` during `start_server`, but the
        # submodules below take a single Logger reference at construction.
        self._logger = Logger()

        # Initialize config and state
        self._config = ClientConfig.from_env(
            env,
            host=host,
            tcp_port=port,
            managers=tuple(managers or []),
            gates=tuple(gates or []),
        )
        self._state = ClientState()

        # Rate limiter for inbound windowed-stats pushes (AD-24); named apart
        # from the base server's transport limiter (``_rate_limiter``), which
        # admits every inbound TCP request
        # Uses AdaptiveRateLimiter with operation limits: (300, 10.0) = 30/s
        self._progress_rate_limiter = AdaptiveRateLimiter(
            overload_detector=HybridOverloadDetector(),
            config=AdaptiveRateLimitConfig(),
        )

        # Initialize all modules with dependency injection
        self._targets = ClientTargetSelector(
            config=self._config,
            state=self._state,
            discovery=DiscoveryService(
                env.get_discovery_config(
                    node_role="client",
                    static_seeds=[],
                    allow_dynamic_registration=True,
                )
            ),
        )
        self._protocol = ClientProtocol(
            state=self._state,
            logger=self._logger,
        )
        self._leadership = ClientLeadershipTracker(state=self._state)
        self._tracker = ClientJobTracker(
            state=self._state,
            logger=self._logger,
            result_drain_timeout_seconds=self._config.result_drain_timeout_seconds,
            poll_gate_for_status=self._poll_gate_for_job_status,
            request_replay=self._request_job_replay,
        )
        self._submitter = ClientJobSubmitter(
            state=self._state,
            config=self._config,
            logger=self._logger,
            targets=self._targets,
            tracker=self._tracker,
            protocol=self._protocol,
            send_tcp_func=self.send_tcp,
            idempotency_key_generator=IdempotencyKeyGenerator(
                client_id=f"{host}:{port}"
            ),
            logical_id_generator=LogicalIdGenerator(
                scope=f"{host}-{port}",
                clock=clock if clock is not None else RealClock(),
            ),
        )
        self._cancellation = ClientCancellationManager(
            state=self._state,
            config=self._config,
            logger=self._logger,
            targets=self._targets,
            tracker=self._tracker,
            send_tcp_func=self.send_tcp,
        )
        self._reporting = ClientReportingManager(
            state=self._state,
            config=self._config,
            logger=self._logger,
            clock=self._clock,
        )
        self._discovery = ClientDiscovery(
            state=self._state,
            config=self._config,
            logger=self._logger,
            targets=self._targets,
            send_tcp_func=self.send_tcp,
        )

        # Initialize all TCP handlers with dependencies
        self._register_handlers()

    def _register_handlers(self) -> None:
        """Register all TCP handlers with module dependencies.

        Handler constructors take only ``(state, logger)`` plus a small
        number of optional dependencies; the matching kwargs below are
        the ones each handler actually accepts. (The dropped ``tracker``
        and ``node_id`` kwargs are not present on the handler signatures
        — passing them raised TypeError and broke client construction.)
        """
        self._job_status_push_handler = JobStatusPushHandler(
            state=self._state,
            logger=self._logger,
        )
        self._job_batch_push_handler = JobBatchPushHandler(
            state=self._state,
            logger=self._logger,
        )
        self._workflow_result_push_handler = WorkflowResultPushHandler(
            state=self._state,
            logger=self._logger,
            reporting_manager=self._reporting,
        )
        self._job_final_result_handler = JobFinalResultHandler(
            state=self._state,
            logger=self._logger,
            workflow_results=self._workflow_result_push_handler,
        )
        self._global_job_result_handler = GlobalJobResultHandler(
            state=self._state,
            logger=self._logger,
            workflow_results=self._workflow_result_push_handler,
        )
        self._reporter_result_push_handler = ReporterResultPushHandler(
            state=self._state,
            logger=self._logger,
        )
        self._windowed_stats_push_handler = WindowedStatsPushHandler(
            state=self._state,
            logger=self._logger,
            rate_limiter=self._progress_rate_limiter,
        )
        self._cancellation_complete_handler = CancellationCompleteHandler(
            state=self._state,
            logger=self._logger,
        )
        self._gate_leader_transfer_handler = GateLeaderTransferHandler(
            state=self._state,
            logger=self._logger,
            leadership_manager=self._leadership,
        )
        self._manager_leader_transfer_handler = ManagerLeaderTransferHandler(
            state=self._state,
            logger=self._logger,
            leadership_manager=self._leadership,
        )

    async def start(self) -> None:
        """Start the client and begin listening for push notifications."""
        init_context = {"nodes": {}}
        await self.start_server(init_context=init_context)

    async def stop(self) -> None:
        """Stop the client and cancel all pending operations."""
        # Signal all job events to unblock waiting coroutines
        for event in self._state._job_events.values():
            event.set()
        for event in self._state._cancellation_events.values():
            event.set()
        await super().shutdown()

    # =========================================================================
    # Public API - Job Submission and Management
    # =========================================================================

    async def submit_job(
        self,
        workflows: list[tuple[list[str], object]],
        vus: int = 1,
        timeout_seconds: float | None = None,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable | None = None,
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        reporting_configs: list | None = None,
        on_reporter_result: Callable[[ReporterResultPush], None] | None = None,
        retry_budget: int = 0,
        retry_budget_per_workflow: int = 0,
        resource_budget: ResourceBudget | None = None,
        best_effort: bool = False,
        best_effort_min_dcs: int = 0,
        best_effort_deadline_seconds: float = 0.0,
    ) -> str:
        """Submit a job for execution (delegates to ClientJobSubmitter).

        ``datacenters`` is a placement CONSTRAINT, not a hint: when
        provided, the job runs only in the listed datacenters (the gate
        still picks the best of them by health/score). If none of the
        listed datacenters is available, the submission is rejected
        with an explicit error — it never silently runs elsewhere.

        Phase H2: ``timeout_seconds=None`` (the default) lets the
        manager apply the AD-26/AD-34 override hierarchy:
        ``Workflow.timeout`` (when overridden in the workflow class)
        wins over the framework default of
        ``workflow.duration × HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER``
        (1.5 by default). Pass a positive number to force an explicit
        per-job override that the manager honors verbatim.

        AD-44: ``retry_budget`` caps total workflow retries across the
        job and ``retry_budget_per_workflow`` caps any single workflow;
        0 (the default) applies the manager's configured defaults, and
        the manager clamps explicit values to its configured maxima.

        AD-41: ``resource_budget`` sets the CPU/memory limits each of the
        job's workflows is enforced against (None applies the manager's
        configured default). A manager with resource guards disabled
        rejects a job that sets one rather than run it unenforced.

        AD-44: ``best_effort`` completes a multi-datacenter job once
        ``best_effort_min_dcs`` datacenters completed, or with whatever
        completed once ``best_effort_deadline_seconds`` passed, instead
        of waiting for every datacenter; the rest are cancelled. 0 applies
        the gate's configured default for either.

        Jobs finished for longer than the configured retention are
        forgotten here, so a long-lived client's tracking stays bounded.
        """
        self._state.release_finished_jobs(
            now=self._clock.monotonic(),
            retention_seconds=self._config.job_retention_seconds,
        )
        return await self._submitter.submit_job(
            workflows=workflows,
            vus=vus,
            timeout_seconds=timeout_seconds,
            datacenter_count=datacenter_count,
            datacenters=datacenters,
            on_status_update=on_status_update,
            on_progress_update=on_progress_update,
            on_workflow_result=on_workflow_result,
            reporting_configs=reporting_configs,
            on_reporter_result=on_reporter_result,
            retry_budget=retry_budget,
            retry_budget_per_workflow=retry_budget_per_workflow,
            resource_budget=resource_budget,
            best_effort=best_effort,
            best_effort_min_dcs=best_effort_min_dcs,
            best_effort_deadline_seconds=best_effort_deadline_seconds,
        )

    async def join_node(
        self,
        node_addr: tuple[str, int],
        target_addr: tuple[str, int],
        timeout: float,
    ) -> NodeJoinResponse:
        """Tell the node at ``node_addr`` to join the node at ``target_addr``.

        The node runs its own registration routine against the target
        (worker->manager, manager->gate, gate->manager); the target's
        register endpoint validates isolation and protocol version.

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a join response. A refused join is returned, not
                raised, so callers can report the node's reason.
        """
        request = NodeJoinRequest(target_host=target_addr[0], target_port=target_addr[1])
        response, _ = await self.send_tcp(
            node_addr,
            "node_join",
            request.dump(),
            timeout=timeout,
        )
        if isinstance(response, Exception):
            raise ClusterJoinError(
                f"node {node_addr[0]}:{node_addr[1]} is unreachable: "
                f"{type(response).__name__}: {response}"
            )

        return decode_join_message(
            response,
            NodeJoinResponse,
            f"join reply from {node_addr[0]}:{node_addr[1]}",
        )

    async def remove_cluster_member(
        self,
        node_addr: tuple[str, int],
        member_addr: tuple[str, int],
        timeout: float,
    ) -> ClusterLeaveReply:
        """Ask the cluster that the node at ``node_addr`` belongs to to
        release the member at ``member_addr`` now (AD-52 section 13
        force-remove) -- for a manager or gate that is gone for good,
        instead of waiting out the tombstone retention. The group's leader
        refuses while the member still answers.

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a leave reply. A refusal is returned, not raised.
        """
        request = ClusterLeaveRequest(host=member_addr[0], port=member_addr[1]).dump()
        # A member that is not the group's leader names it: ask it once.
        asked_addr = node_addr
        for _ in range(2):
            response, _ = await self.send_tcp(asked_addr, "cluster_leave", request, timeout=timeout)
            if isinstance(response, Exception):
                raise ClusterJoinError(
                    f"node {asked_addr[0]}:{asked_addr[1]} is unreachable: "
                    f"{type(response).__name__}: {response}"
                )
            reply = decode_join_message(
                response,
                ClusterLeaveReply,
                f"leave reply from {asked_addr[0]}:{asked_addr[1]} "
                "(only managers and gates hold cluster membership)",
            )
            if reply.released or reply.leader_member_id is None or asked_addr != node_addr:
                return reply
            asked_addr = ClusterMemberId.parse(reply.leader_member_id).address
        return reply

    async def set_cluster_mode(
        self,
        node_addr: tuple[str, int],
        mode: str,
        timeout: float,
    ) -> ClusterModeReply:
        """Set the mode of the cluster the node at ``node_addr`` belongs to
        (AD-52 section 13): ``open``, ``frozen`` (no membership change
        commits) or ``read-only`` (frozen, and job submissions refused).
        A member that does not know its leader names none; one that does
        passes the change on.

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a mode reply. A refusal is returned, not raised.
        """
        request = ClusterModeRequest(mode=mode).dump()
        asked_addr = node_addr
        for _ in range(2):
            response, _ = await self.send_tcp(asked_addr, "cluster_mode", request, timeout=timeout)
            if isinstance(response, Exception):
                raise ClusterJoinError(
                    f"node {asked_addr[0]}:{asked_addr[1]} is unreachable: "
                    f"{type(response).__name__}: {response}"
                )
            reply = decode_join_message(
                response,
                ClusterModeReply,
                f"mode reply from {asked_addr[0]}:{asked_addr[1]} "
                "(only managers and gates hold cluster membership)",
            )
            if reply.applied or reply.leader_member_id is None or asked_addr != node_addr:
                return reply
            asked_addr = ClusterMemberId.parse(reply.leader_member_id).address
        return reply

    async def resize_cluster(
        self,
        node_addr: tuple[str, int],
        member_addr: tuple[str, int],
        add: bool,
        timeout: float,
    ) -> ClusterResizeReply:
        """Add ``member_addr`` to the cohort of the cluster the node at
        ``node_addr`` belongs to, or remove it (AD-52 ``ResizeCluster``).
        A member that does not know its leader names none; one that does
        passes the change on.

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a resize reply. A refusal is returned, not raised.
        """
        request = ClusterResizeRequest(host=member_addr[0], port=member_addr[1], add=add).dump()
        asked_addr = node_addr
        for _ in range(2):
            response, _ = await self.send_tcp(asked_addr, "cluster_resize", request, timeout=timeout)
            if isinstance(response, Exception):
                raise ClusterJoinError(
                    f"node {asked_addr[0]}:{asked_addr[1]} is unreachable: "
                    f"{type(response).__name__}: {response}"
                )
            reply = decode_join_message(
                response,
                ClusterResizeReply,
                f"resize reply from {asked_addr[0]}:{asked_addr[1]} "
                "(only managers and gates hold cluster membership)",
            )
            if reply.applied or reply.leader_member_id is None or asked_addr != node_addr:
                return reply
            asked_addr = ClusterMemberId.parse(reply.leader_member_id).address
        return reply

    async def cluster_status(self, node_addr: tuple[str, int], timeout: float) -> ClusterStatusReply:
        """The membership of the cluster the node at ``node_addr`` belongs to,
        as of now (AD-52 section 11): its leader answers after confirming it
        still leads. A member that does not know its leader names none; one
        that does passes the request on.

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a status reply. A refusal is returned, not raised.
        """
        request = ClusterStatusRequest().dump()
        asked_addr = node_addr
        for _ in range(2):
            response, _ = await self.send_tcp(asked_addr, "cluster_status", request, timeout=timeout)
            if isinstance(response, Exception):
                raise ClusterJoinError(
                    f"node {asked_addr[0]}:{asked_addr[1]} is unreachable: "
                    f"{type(response).__name__}: {response}"
                )
            reply = decode_join_message(
                response,
                ClusterStatusReply,
                f"status reply from {asked_addr[0]}:{asked_addr[1]} "
                "(only managers and gates hold cluster membership)",
            )
            if reply.served or reply.leader_member_id is None or asked_addr != node_addr:
                return reply
            asked_addr = ClusterMemberId.parse(reply.leader_member_id).address
        return reply

    async def watch_cluster(
        self,
        node_addr: tuple[str, int],
        cluster_uuid: str | None,
        after_index: int,
        wait_seconds: float,
        timeout: float,
    ) -> ClusterWatchReply:
        """One long poll of a membership watch (AD-52 section 9): the
        changes the node applied after ``after_index`` of ``cluster_uuid``,
        or a snapshot to resume from -- answered within ``wait_seconds``
        when nothing changes. Resume the next poll after the reply's
        ``applied_index``.

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a watch reply. A refusal is returned, not raised.
        """
        response, _ = await self.send_tcp(
            node_addr,
            "cluster_watch",
            ClusterWatchRequest(
                cluster_uuid=cluster_uuid, after_index=after_index, wait_seconds=wait_seconds
            ).dump(),
            timeout=timeout,
        )
        if isinstance(response, Exception):
            raise ClusterJoinError(
                f"node {node_addr[0]}:{node_addr[1]} is unreachable: "
                f"{type(response).__name__}: {response}"
            )
        return decode_join_message(
            response,
            ClusterWatchReply,
            f"watch reply from {node_addr[0]}:{node_addr[1]} "
            "(only managers and gates hold cluster membership)",
        )

    async def cluster_metrics(self, node_addr: tuple[str, int], timeout: float) -> ClusterMetricsReply:
        """The node's own metrics of its cluster's membership (AD-52 section
        18).

        Raises:
            ClusterJoinError: the node was unreachable or did not answer
                with a metrics reply.
        """
        response, _ = await self.send_tcp(node_addr, "cluster_metrics", b"", timeout=timeout)
        if isinstance(response, Exception):
            raise ClusterJoinError(
                f"node {node_addr[0]}:{node_addr[1]} is unreachable: "
                f"{type(response).__name__}: {response}"
            )
        return decode_join_message(
            response,
            ClusterMetricsReply,
            f"metrics reply from {node_addr[0]}:{node_addr[1]} "
            "(only managers and gates hold cluster membership)",
        )

    async def wait_for_job(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> ClientJobResult:
        """Wait for job completion (delegates to ClientJobTracker)."""
        return await self._tracker.wait_for_job(job_id, timeout=timeout)

    def stream_workflow_results(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> AsyncIterator[ClientWorkflowResult]:
        """Each workflow result of a job as it arrives, until the job is
        done (delegates to ClientJobTracker)::

            async for workflow_result in client.stream_workflow_results(job_id):
                print(workflow_result.workflow_id, workflow_result.status)
        """
        return self._tracker.stream_workflow_results(job_id, timeout=timeout)

    def release_job(self, job_id: str) -> None:
        """Forget a job the caller is done with: its status, results and
        callbacks. Finished jobs are also forgotten once the configured
        retention has passed."""
        self._state.release_job(job_id)

    def get_job_status(self, job_id: str) -> ClientJobResult | None:
        """Get current job status (delegates to ClientJobTracker)."""
        return self._tracker.get_job_status(job_id)

    async def cancel_job(
        self,
        job_id: str,
        reason: str = "",
        max_redirects: int = 3,
        max_retries: int = 3,
        retry_base_delay: float = 0.5,
        timeout: float = 10.0,
    ) -> JobCancelResponse:
        """Cancel a running job (delegates to ClientCancellationManager)."""
        return await self._cancellation.cancel_job(
            job_id=job_id,
            reason=reason,
            max_redirects=max_redirects,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
            timeout=timeout,
        )

    async def await_job_cancellation(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> tuple[bool, list[str]]:
        """Wait for cancellation completion (delegates to ClientCancellationManager)."""
        return await self._cancellation.await_job_cancellation(job_id, timeout=timeout)

    # =========================================================================
    # Public API - Discovery and Query
    # =========================================================================

    async def ping_manager(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> ManagerPingResponse:
        """Ping a manager (delegates to ClientDiscovery)."""
        return await self._discovery.ping_manager(addr=addr, timeout=timeout)

    async def ping_gate(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> GatePingResponse:
        """Ping a gate (delegates to ClientDiscovery)."""
        return await self._discovery.ping_gate(addr=addr, timeout=timeout)

    async def ping_all_managers(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], ManagerPingResponse | Exception]:
        """Ping all managers concurrently (delegates to ClientDiscovery)."""
        return await self._discovery.ping_all_managers(timeout=timeout)

    async def ping_all_gates(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], GatePingResponse | Exception]:
        """Ping all gates concurrently (delegates to ClientDiscovery)."""
        return await self._discovery.ping_all_gates(timeout=timeout)

    async def query_workflows(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        timeout: float = 5.0,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """Query workflow status from managers (delegates to ClientDiscovery)."""
        return await self._discovery.query_workflows(
            workflow_names=workflow_names,
            job_id=job_id,
            timeout=timeout,
        )

    async def query_workflows_via_gate(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        addr: tuple[str, int] | None = None,
        timeout: float = 10.0,
    ) -> dict[str, list[WorkflowStatusInfo]]:
        """Query workflow status via gate (delegates to ClientDiscovery)."""
        return await self._discovery.query_workflows_via_gate(
            workflow_names=workflow_names,
            job_id=job_id,
            addr=addr,
            timeout=timeout,
        )

    async def query_all_gates_workflows(
        self,
        workflow_names: list[str],
        job_id: str | None = None,
        timeout: float = 10.0,
    ) -> dict[tuple[str, int], dict[str, list[WorkflowStatusInfo]] | Exception]:
        """Query all gates concurrently (delegates to ClientDiscovery)."""
        return await self._discovery.query_all_gates_workflows(
            workflow_names=workflow_names,
            job_id=job_id,
            timeout=timeout,
        )

    async def get_datacenters(
        self,
        addr: tuple[str, int] | None = None,
        timeout: float = 5.0,
    ) -> DatacenterListResponse:
        """Get datacenter list from gate (delegates to ClientDiscovery)."""
        return await self._discovery.get_datacenters(addr=addr, timeout=timeout)

    async def get_datacenters_from_all_gates(
        self,
        timeout: float = 5.0,
    ) -> dict[tuple[str, int], DatacenterListResponse | Exception]:
        """Query all gates for datacenters (delegates to ClientDiscovery)."""
        return await self._discovery.get_datacenters_from_all_gates(timeout=timeout)

    # =========================================================================
    # Internal Helper Methods
    # =========================================================================

    async def query_job_status(
        self,
        node_addr: tuple[str, int],
        job_id: str,
        timeout: float,
        consistency: ReadConsistency = ReadConsistency.EVENTUAL,
        max_staleness_seconds: float = 0.0,
    ) -> GlobalJobStatus | None:
        """A job's status, asked of the gate or manager at ``node_addr`` at
        ``consistency`` (AD-38 Part 8): it answers from what it holds (live
        state, or a manager's durable ledger) or from the job's leader --
        None when it has no answer at that level. A SESSION read of a job
        this client tracks is never older than the newest view of it a read
        here answered; BOUNDED_STALENESS bounds the view's age by
        ``max_staleness_seconds``.

        Raises:
            ClusterJoinError: the node was unreachable.
        """
        observed_fence_token, observed_view_time = self._state.get_job_read_view(job_id)
        query = JobStatusQuery(
            job_id=job_id,
            consistency=consistency.value,
            observed_fence_token=observed_fence_token,
            observed_view_time=observed_view_time,
            max_staleness_seconds=max_staleness_seconds,
        )
        response, _ = await self.send_tcp(node_addr, "job_status", query.dump(), timeout=timeout)
        if isinstance(response, Exception):
            raise ClusterJoinError(
                f"node {node_addr[0]}:{node_addr[1]} is unreachable: "
                f"{type(response).__name__}: {response}"
            )
        if not response:
            return None
        job_status = GlobalJobStatus.load(response)
        self._state.record_job_read_view(job_id, job_status.fence_token, job_status.view_time)
        return job_status

    async def _poll_gate_for_job_status(
        self,
        job_id: str,
    ) -> GlobalJobStatus | None:
        """Poll the job's gate — or, gateless (L2), its manager — for
        authoritative status. The manager answers from live state or
        its durable ledger, so this reads truth even across a manager
        restart."""
        poll_addr = self._targets.get_gate_for_job(job_id)
        if not poll_addr:
            poll_addr = self._targets.get_next_gate()
        if not poll_addr:
            poll_addr = self._targets.get_next_manager()
        if not poll_addr:
            return None

        try:
            response_data, _ = await self.send_tcp(
                poll_addr,
                "job_status",
                job_id.encode(),
                timeout=self._config.status_query_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response_data, Exception):
                raise response_data
            if response_data and response_data != b"":
                return GlobalJobStatus.load(response_data)
        except Exception as poll_error:
            await self._logger.log(
                ServerDebug(
                    message=(
                        f"Status poll to {poll_addr} for job "
                        f"{job_id[:8]} failed: {poll_error}"
                    ),
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )

        return None

    async def _request_job_replay(self, job_id: str) -> bool:
        """Have the job's gate send again what it recorded for this client
        and could not deliver: re-registering this client's callback for
        the job makes the gate replay its update history from the last
        update that reached the client -- answered once the replay is sent.
        Gateless (L2), a manager keeps no such history: nothing to ask."""
        gate_address = self._targets.get_gate_for_job(job_id)
        if not gate_address:
            return False

        try:
            response_data, _ = await self.send_tcp(
                gate_address,
                "register_callback",
                RegisterCallback(
                    job_id=job_id,
                    callback_addr=(self._host, self._tcp_port),
                ).dump(),
                timeout=self._config.status_query_timeout_seconds,
            )
            # send_tcp returns transport errors rather than raising.
            if isinstance(response_data, Exception):
                raise response_data
            if response_data and RegisterCallbackResponse.load(response_data).success:
                return True
        except Exception as replay_error:
            await self._logger.log(
                ServerWarning(
                    message=(
                        f"Asking gate {gate_address} to replay job {job_id[:8]}... "
                        f"failed: {replay_error}"
                    ),
                    node_host="client",
                    node_port=0,
                    node_id="client",
                )
            )
        return False

    # =========================================================================
    # TCP Handlers - Delegate to Handler Classes
    # =========================================================================

    @tcp.receive()
    async def job_status_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job status push notification."""
        return await self._job_status_push_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def job_batch_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle batch job status push."""
        return await self._job_batch_push_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def receive_job_final_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle job final result push."""
        return await self._job_final_result_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def receive_global_job_result(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle global job result push."""
        return await self._global_job_result_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def reporter_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle reporter result push."""
        return await self._reporter_result_push_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def workflow_result_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle workflow result push."""
        return await self._workflow_result_push_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def windowed_stats_push(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle windowed stats push."""
        return await self._windowed_stats_push_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle cancellation completion push (AD-20).

        Wire-action name MUST match what the manager pushes (see
        ``ManagerServer._push_cancellation_complete_to_origin`` —
        action ``"job_cancellation_complete"``). The
        ``@tcp.receive()`` decorator registers handlers by
        ``func.__name__``; this method name is the wire match.
        """
        return await self._cancellation_complete_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def receive_gate_job_leader_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle gate leader transfer notification."""
        return await self._gate_leader_transfer_handler.handle(addr, data, clock_time)

    @tcp.receive()
    async def receive_manager_job_leader_transfer(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle manager leader transfer notification."""
        return await self._manager_leader_transfer_handler.handle(
            addr, data, clock_time
        )

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

from typing import Callable

from hyperscale.distributed.server import tcp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)
from hyperscale.distributed.models import (
    JobStatusPush,
    ReporterResultPush,
    WorkflowResultPush,
    ManagerPingResponse,
    GatePingResponse,
    WorkflowStatusInfo,
    DatacenterListResponse,
    JobCancelResponse,
)
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.reliability.rate_limiting import (
    AdaptiveRateLimiter,
    AdaptiveRateLimitConfig,
)
from hyperscale.distributed.reliability.overload import HybridOverloadDetector

# Import all client modules
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.state import ClientState
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
    ):
        """
        Initialize the client.

        Args:
            host: Local host to bind for receiving push notifications
            port: Local TCP port for receiving push notifications
            env: Environment configuration
            managers: List of manager (host, port) addresses
            gates: List of gate (host, port) addresses
        """
        env = env or Env()

        super().__init__(
            host=host,
            tcp_port=port,
            udp_port=port + 1,  # UDP not used but required by base
            env=env,
        )

        # Initialize config and state
        self._config = ClientConfig(
            host=host,
            tcp_port=port,
            managers=tuple(managers or []),
            gates=tuple(gates or []),
            env=env,
        )
        self._state = ClientState()

        # Initialize rate limiter for progress updates (AD-24)
        # Uses AdaptiveRateLimiter with operation limits: (300, 10.0) = 30/s
        self._rate_limiter = AdaptiveRateLimiter(
            overload_detector=HybridOverloadDetector(),
            config=AdaptiveRateLimitConfig(),
        )

        # Initialize all modules with dependency injection
        self._targets = ClientTargetSelector(
            config=self._config,
            state=self._state,
        )
        self._protocol = ClientProtocol(
            state=self._state,
            logger=self._logger,
        )
        self._leadership = ClientLeadershipTracker(
            state=self._state,
            config=self._config,
            logger=self._logger,
        )
        self._tracker = ClientJobTracker(
            state=self._state,
            logger=self._logger,
            poll_gate_for_status=self._poll_gate_for_job_status,
        )
        self._submitter = ClientJobSubmitter(
            state=self._state,
            config=self._config,
            logger=self._logger,
            targets=self._targets,
            tracker=self._tracker,
            protocol=self._protocol,
            send_tcp_func=self.send_tcp,
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
        """Register all TCP handlers with module dependencies."""
        self._job_status_push_handler = JobStatusPushHandler(
            state=self._state,
            logger=self._logger,
            tracker=self._tracker,
        )
        self._job_batch_push_handler = JobBatchPushHandler(
            state=self._state,
            logger=self._logger,
            tracker=self._tracker,
        )
        self._job_final_result_handler = JobFinalResultHandler(
            state=self._state,
            logger=self._logger,
            tracker=self._tracker,
        )
        self._global_job_result_handler = GlobalJobResultHandler(
            state=self._state,
            logger=self._logger,
            tracker=self._tracker,
        )
        self._reporter_result_push_handler = ReporterResultPushHandler(
            state=self._state,
            logger=self._logger,
        )
        self._workflow_result_push_handler = WorkflowResultPushHandler(
            state=self._state,
            logger=self._logger,
            reporting=self._reporting,
        )
        self._windowed_stats_push_handler = WindowedStatsPushHandler(
            state=self._state,
            logger=self._logger,
            rate_limiter=self._rate_limiter,
        )
        self._cancellation_complete_handler = CancellationCompleteHandler(
            state=self._state,
            logger=self._logger,
        )
        self._gate_leader_transfer_handler = GateLeaderTransferHandler(
            state=self._state,
            logger=self._logger,
            leadership_manager=self._leadership,
            node_id=self._node_id,
        )
        self._manager_leader_transfer_handler = ManagerLeaderTransferHandler(
            state=self._state,
            logger=self._logger,
            leadership_manager=self._leadership,
            node_id=self._node_id,
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
        timeout_seconds: float = 300.0,
        datacenter_count: int = 1,
        datacenters: list[str] | None = None,
        on_status_update: Callable[[JobStatusPush], None] | None = None,
        on_progress_update: Callable | None = None,
        on_workflow_result: Callable[[WorkflowResultPush], None] | None = None,
        reporting_configs: list | None = None,
        on_reporter_result: Callable[[ReporterResultPush], None] | None = None,
    ) -> str:
        """Submit a job for execution (delegates to ClientJobSubmitter)."""
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
        )

    async def wait_for_job(
        self,
        job_id: str,
        timeout: float | None = None,
    ) -> ClientJobResult:
        """Wait for job completion (delegates to ClientJobTracker)."""
        return await self._tracker.wait_for_job(job_id, timeout=timeout)

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
    async def receive_job_cancellation_complete(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle cancellation completion push."""
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

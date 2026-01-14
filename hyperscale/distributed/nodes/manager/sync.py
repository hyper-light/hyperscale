"""
Manager state sync module.

Handles state synchronization with workers and peer managers during
leader election and recovery scenarios. Uses AD-21 jitter strategies
for retry delays to prevent thundering herd.
"""

import asyncio
import time
from typing import Any, Callable, Coroutine, TYPE_CHECKING

from hyperscale.distributed.models import (
    ManagerStateSnapshot,
    NodeInfo,
    NodeRole,
    StateSyncRequest,
    StateSyncResponse,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
    WorkerStateSnapshot,
)
from hyperscale.distributed.reliability import (
    calculate_jittered_delay,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerDebug,
    ServerWarning,
    ServerError,
)

if TYPE_CHECKING:
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.nodes.manager.config import ManagerConfig
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

SendFunc = Callable[..., Coroutine[Any, Any, tuple[bytes, float] | None]]


class ManagerStateSync:
    """
    Manages state synchronization with workers and peers.

    Handles:
    - Worker state sync (workers are source of truth for workflows)
    - Peer manager state sync (for job metadata)
    - Retry logic with exponential backoff
    - Snapshot generation and application
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        registry: "ManagerRegistry",
        logger: "Logger",
        node_id: str,
        task_runner: "TaskRunner",
        send_tcp: SendFunc,
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._registry: "ManagerRegistry" = registry
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._send_tcp: SendFunc = send_tcp

    async def sync_state_from_workers(self) -> None:
        """
        Synchronize state from all known workers.

        Called during leader election to rebuild workflow state.
        Workers are the source of truth for active workflows.
        """
        workers = self._registry.get_all_workers()
        if not workers:
            return

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Starting state sync from {len(workers)} workers",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        request = StateSyncRequest(
            requester_id=self._node_id,
            sync_type="worker_state",
            state_version=self._state._state_version,
        )

        for worker_id, worker in workers.items():
            worker_addr = (worker.node.host, worker.node.tcp_port)
            snapshot = await self._request_worker_state(worker_addr, request)
            if snapshot:
                await self._apply_worker_state(snapshot)

    async def _request_worker_state(
        self,
        worker_addr: tuple[str, int],
        request: StateSyncRequest,
    ) -> WorkerStateSnapshot | None:
        """
        Request state from a single worker with retry.

        Args:
            worker_addr: Worker address
            request: Sync request

        Returns:
            WorkerStateSnapshot or None on failure
        """
        max_retries = self._config.state_sync_retries
        base_delay = 0.5
        max_delay = 30.0

        for attempt in range(max_retries):
            try:
                response = await self._send_tcp(
                    worker_addr,
                    "state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)
                    if sync_response.worker_state:
                        return sync_response.worker_state

            except Exception as sync_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Worker state sync attempt {attempt + 1} failed: {sync_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

            if attempt < max_retries - 1:
                delay = calculate_jittered_delay(
                    attempt=attempt,
                    base_delay=base_delay,
                    max_delay=max_delay,
                    jitter=JitterStrategy.FULL,
                )
                await asyncio.sleep(delay)

        return None

    async def _apply_worker_state(self, snapshot: WorkerStateSnapshot) -> None:
        """
        Apply worker state snapshot to local state.

        Args:
            snapshot: Worker state snapshot
        """
        worker_id = snapshot.node_id
        worker_key = f"worker:{worker_id}"
        worker_pool = self._registry._worker_pool
        worker_status = worker_pool.get_worker(worker_id) if worker_pool else None

        if (
            worker_status
            and worker_status.heartbeat
            and snapshot.version <= worker_status.heartbeat.version
        ):
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=(
                        f"Ignoring stale worker state from {worker_id[:8]}... "
                        f"(version {snapshot.version})"
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return

        if not await self._state._versioned_clock.should_accept_update(
            worker_key,
            snapshot.version,
        ):
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=(
                        f"Rejected worker state conflict for {worker_id[:8]}... "
                        f"(version {snapshot.version})"
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return

        registration = self._registry.get_worker(worker_id)
        if not registration:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=(
                        f"Worker state sync received for unknown worker "
                        f"{worker_id[:8]}..."
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return

        registration.total_cores = snapshot.total_cores
        registration.available_cores = snapshot.available_cores

        if worker_pool:
            if worker_status is None:
                await worker_pool.register_worker(registration)
                worker_status = worker_pool.get_worker(worker_id)

            if worker_status:
                heartbeat = WorkerHeartbeat(
                    node_id=worker_id,
                    state=snapshot.state,
                    available_cores=snapshot.available_cores,
                    queue_depth=0,
                    cpu_percent=0.0,
                    memory_percent=0.0,
                    version=snapshot.version,
                    active_workflows={
                        workflow_id: progress.status
                        for workflow_id, progress in snapshot.active_workflows.items()
                    },
                    tcp_host=registration.node.host,
                    tcp_port=registration.node.port,
                )

                async with worker_pool._cores_condition:
                    old_available = worker_status.available_cores
                    worker_status.heartbeat = heartbeat
                    worker_status.last_seen = time.monotonic()
                    worker_status.state = snapshot.state
                    worker_status.available_cores = snapshot.available_cores
                    worker_status.total_cores = snapshot.total_cores
                    worker_status.reserved_cores = 0

                    if worker_status.available_cores > old_available:
                        worker_pool._cores_condition.notify_all()

                    health_state = worker_pool._worker_health.get(worker_id)
                    if health_state:
                        health_state.update_liveness(success=True)
                        health_state.update_readiness(
                            accepting=worker_status.available_cores > 0,
                            capacity=worker_status.available_cores,
                        )

        await self._state._versioned_clock.update_entity(worker_key, snapshot.version)

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=(
                    f"Applied worker state from {worker_id[:8]}... "
                    f"cores={snapshot.available_cores}/{snapshot.total_cores}"
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def sync_state_from_manager_peers(self) -> None:
        """
        Synchronize state from peer managers.

        Called during leader election to get job metadata
        (retry counts, context versions, etc).
        """
        peers = list(self._state._active_manager_peers)
        if not peers:
            return

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Starting state sync from {len(peers)} manager peers",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        request = StateSyncRequest(
            requester_id=self._node_id,
            sync_type="manager_state",
            state_version=self._state._state_version,
        )

        for peer_addr in peers:
            snapshot = await self._request_manager_peer_state(peer_addr, request)
            if snapshot:
                await self._apply_manager_peer_state(snapshot)

    async def _request_manager_peer_state(
        self,
        peer_addr: tuple[str, int],
        request: StateSyncRequest,
    ) -> ManagerStateSnapshot | None:
        """
        Request state from a single peer manager with retry.

        Args:
            peer_addr: Peer address
            request: Sync request

        Returns:
            ManagerStateSnapshot or None on failure
        """
        max_retries = self._config.state_sync_retries
        base_delay = 0.5
        max_delay = 30.0

        for attempt in range(max_retries):
            try:
                response = await self._send_tcp(
                    peer_addr,
                    "state_sync_request",
                    request.dump(),
                    timeout=self._config.state_sync_timeout_seconds,
                )

                if response and not isinstance(response, Exception):
                    sync_response = StateSyncResponse.load(response)
                    if sync_response.manager_state:
                        return sync_response.manager_state

            except Exception as sync_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Peer state sync attempt {attempt + 1} failed: {sync_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

            if attempt < max_retries - 1:
                delay = calculate_jittered_delay(
                    attempt=attempt,
                    base_delay=base_delay,
                    max_delay=max_delay,
                    jitter=JitterStrategy.FULL,
                )
                await asyncio.sleep(delay)

        return None

    async def _apply_manager_peer_state(self, snapshot: ManagerStateSnapshot) -> None:
        """
        Apply manager peer state snapshot to local state.

        Args:
            snapshot: Manager state snapshot
        """
        for job_id, fence_token in snapshot.job_fence_tokens.items():
            current_token = self._state._job_fencing_tokens.get(job_id, -1)
            if fence_token > current_token:
                self._state._job_fencing_tokens[job_id] = fence_token

                leader_id = snapshot.job_leaders.get(job_id)
                if leader_id:
                    self._state._job_leaders[job_id] = leader_id

                leader_addr = snapshot.job_leader_addrs.get(job_id)
                if leader_addr:
                    leader_addr_tuple = (
                        tuple(leader_addr)
                        if isinstance(leader_addr, list)
                        else leader_addr
                    )
                    self._state._job_leader_addrs[job_id] = leader_addr_tuple

                incoming_layer_version = snapshot.job_layer_versions.get(job_id)
                if incoming_layer_version is not None:
                    current_layer_version = self._state._job_layer_version.get(
                        job_id, 0
                    )
                    if incoming_layer_version > current_layer_version:
                        self._state._job_layer_version[job_id] = incoming_layer_version

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Applied manager peer state (version {snapshot.state_version})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def get_state_snapshot(
        self,
        datacenter: str,
        is_leader: bool,
        term: int,
    ) -> ManagerStateSnapshot:
        worker_snapshots = [
            WorkerStateSnapshot(
                worker_id=worker_id,
                host=reg.node.host,
                tcp_port=reg.node.port,
                udp_port=reg.node.udp_port or reg.node.port,
                active_workflows={
                    wf_id: wf
                    for wf_id, wf in self._state._workflow_progress.items()
                    if wf.worker_id == worker_id
                },
            )
            for worker_id, reg in self._state._workers.items()
        ]

        return ManagerStateSnapshot(
            node_id=self._node_id,
            datacenter=datacenter,
            is_leader=is_leader,
            term=term,
            version=self._state._state_version,
            workers=worker_snapshots,
            jobs=dict(self._state._job_progress),
            job_leaders=dict(self._state._job_leaders),
            job_leader_addrs=dict(self._state._job_leader_addrs),
            job_fence_tokens=dict(self._state._job_fencing_tokens),
            job_layer_versions=dict(self._state._job_layer_version),
        )

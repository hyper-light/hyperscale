"""
Manager state sync module.

Handles state synchronization with workers and peer managers during
leader election and recovery scenarios. Uses AD-21 jitter strategies
for retry delays to prevent thundering herd.
"""

import asyncio
import time
from typing import Any, Callable, Coroutine, TYPE_CHECKING, cast

from hyperscale.distributed.jobs.worker_pool import WorkerPool
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
    WorkerStatus,
)
from hyperscale.distributed.reliability import (
    calculate_jittered_delay,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import (
    ServerInfo,
    ServerDebug,
    ServerWarning,
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
        is_leader_fn: Callable[[], bool] | None = None,
        get_term_fn: Callable[[], int] | None = None,
        handle_elected_fn: Callable[[tuple[str, int], int], Coroutine[Any, Any, None]]
        | None = None,
        should_yield_fn: Callable[[tuple[str, int], int], bool] | None = None,
        step_down_fn: Callable[[], Coroutine[Any, Any, None]] | None = None,
        set_dc_leader_fn: Callable[[str | None], None] | None = None,
        export_stats_checkpoint_fn: Callable[[], list[tuple[float, float]]] | None = None,
        import_stats_checkpoint_fn: Callable[
            [list[tuple[float, float]]], Coroutine[Any, Any, int]
        ]
        | None = None,
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._registry: "ManagerRegistry" = registry
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._task_runner: "TaskRunner" = task_runner
        self._send_tcp: SendFunc = send_tcp
        self._is_leader: Callable[[], bool] = is_leader_fn or (lambda: False)
        self._get_term: Callable[[], int] = get_term_fn or (lambda: 0)
        self._handle_elected: Callable[
            [tuple[str, int], int], Coroutine[Any, Any, None]
        ] = handle_elected_fn or self._noop_async
        self._should_yield_to_peer: Callable[[tuple[str, int], int], bool] = (
            should_yield_fn or (lambda _peer_addr, _peer_term: False)
        )
        self._step_down: Callable[[], Coroutine[Any, Any, None]] = (
            step_down_fn or self._noop_async
        )
        self._set_dc_leader: Callable[[str | None], None] = set_dc_leader_fn or (
            lambda _leader_id: None
        )
        self._export_stats_checkpoint: Callable[[], list[tuple[float, float]]] = (
            export_stats_checkpoint_fn or (lambda: [])
        )
        self._import_stats_checkpoint: Callable[
            [list[tuple[float, float]]], Coroutine[Any, Any, int]
        ] = import_stats_checkpoint_fn or self._noop_import_checkpoint
        self._worker_state_lock: asyncio.Lock = asyncio.Lock()

    async def _noop_import_checkpoint(
        self, _checkpoint: list[tuple[float, float]]
    ) -> int:
        return 0

    async def _noop_async(self, *_: Any) -> None:
        return None

    def _normalize_job_leader_addr(
        self,
        leader_addr: tuple[str, int] | list[str | int] | None,
    ) -> tuple[str, int] | None:
        if leader_addr is None:
            return None

        if isinstance(leader_addr, list):
            if len(leader_addr) != 2:
                return None
            return (str(leader_addr[0]), int(leader_addr[1]))

        return cast(tuple[str, int], leader_addr)

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
            requester_role="manager",
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
            since_version=self._state.state_version,
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
                    if sync_response.responder_ready and sync_response.worker_state:
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

    def _derive_worker_health_state(self, snapshot: WorkerStateSnapshot) -> str:
        if snapshot.state == WorkerState.HEALTHY.value:
            return "healthy" if snapshot.available_cores > 0 else "busy"
        if snapshot.state == WorkerState.DEGRADED.value:
            return "stressed"
        return "overloaded"

    def _build_worker_registration_from_snapshot(
        self,
        snapshot: WorkerStateSnapshot,
    ) -> WorkerRegistration | None:
        if not snapshot.host or snapshot.tcp_port <= 0:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=(
                        f"Worker sync missing address info for {snapshot.node_id[:8]}..."
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return None

        node_info = NodeInfo(
            node_id=snapshot.node_id,
            role=NodeRole.WORKER,
            host=snapshot.host,
            port=snapshot.tcp_port,
            udp_port=snapshot.udp_port or snapshot.tcp_port,
            datacenter=self._config.datacenter_id,
            version=snapshot.version,
        )

        return WorkerRegistration(
            node=node_info,
            total_cores=snapshot.total_cores,
            available_cores=snapshot.available_cores,
            memory_mb=0,
            available_memory_mb=0,
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
        )

    def _update_registration_from_snapshot(
        self,
        registration: WorkerRegistration,
        snapshot: WorkerStateSnapshot,
        should_update_mapping: bool,
    ) -> None:
        registration.total_cores = snapshot.total_cores
        registration.available_cores = snapshot.available_cores
        registration.node.version = snapshot.version

        if snapshot.host and snapshot.tcp_port > 0:
            incoming_udp_port = snapshot.udp_port or snapshot.tcp_port
            if (
                registration.node.host != snapshot.host
                or registration.node.port != snapshot.tcp_port
                or registration.node.udp_port != incoming_udp_port
            ):
                if should_update_mapping:
                    old_tcp_addr = (registration.node.host, registration.node.port)
                    old_udp_addr = (registration.node.host, registration.node.udp_port)
                    self._state._worker_addr_to_id.pop(old_tcp_addr, None)
                    self._state._worker_addr_to_id.pop(old_udp_addr, None)

                registration.node.host = snapshot.host
                registration.node.port = snapshot.tcp_port
                registration.node.udp_port = incoming_udp_port

                if should_update_mapping:
                    new_tcp_addr = (registration.node.host, registration.node.port)
                    new_udp_addr = (registration.node.host, registration.node.udp_port)
                    self._state._worker_addr_to_id[new_tcp_addr] = snapshot.node_id
                    self._state._worker_addr_to_id[new_udp_addr] = snapshot.node_id

    def _resolve_worker_registration(
        self,
        snapshot: WorkerStateSnapshot,
        worker_status: WorkerStatus | None,
    ) -> WorkerRegistration | None:
        registration = self._registry.get_worker(snapshot.node_id)
        if registration:
            self._update_registration_from_snapshot(
                registration,
                snapshot,
                should_update_mapping=True,
            )
            return registration

        if worker_status and worker_status.registration:
            registration = worker_status.registration
            self._update_registration_from_snapshot(
                registration,
                snapshot,
                should_update_mapping=False,
            )
            self._registry.register_worker(registration)
            return registration

        registration = self._build_worker_registration_from_snapshot(snapshot)
        if registration is None:
            return None

        self._registry.register_worker(registration)
        return registration

    async def _apply_worker_pool_snapshot(
        self,
        worker_pool: WorkerPool,
        worker_status: WorkerStatus,
        registration: WorkerRegistration,
        snapshot: WorkerStateSnapshot,
        health_state: str,
    ) -> None:
        queue_depth = len(snapshot.active_workflows)
        heartbeat = WorkerHeartbeat(
            node_id=snapshot.node_id,
            state=snapshot.state,
            available_cores=snapshot.available_cores,
            queue_depth=queue_depth,
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
            worker_status.queue_depth = queue_depth
            worker_status.cpu_percent = 0.0
            worker_status.memory_percent = 0.0
            worker_status.reserved_cores = 0
            worker_status.overload_state = health_state

            if worker_status.available_cores > old_available:
                worker_pool._cores_condition.notify_all()

            pool_health = worker_pool._worker_health.get(worker_status.worker_id)
            if pool_health:
                accepting = (
                    snapshot.state == WorkerState.HEALTHY.value
                    and worker_status.available_cores > 0
                )
                pool_health.update_liveness(success=True)
                pool_health.update_readiness(
                    accepting=accepting,
                    capacity=worker_status.available_cores,
                )

    async def _remove_worker_from_sync(
        self,
        worker_id: str,
        worker_key: str,
        snapshot_version: int,
        worker_pool: WorkerPool | None,
    ) -> None:
        registration = self._registry.get_worker(worker_id)
        if registration:
            self._registry.unregister_worker(worker_id)

        if worker_pool:
            await worker_pool.deregister_worker(worker_id)

        await self._state._versioned_clock.update_entity(worker_key, snapshot_version)

        self._task_runner.run(
            self._logger.log,
            ServerWarning(
                message=f"Removed offline worker {worker_id[:8]}... from sync",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def _apply_worker_state(self, snapshot: WorkerStateSnapshot) -> None:
        """
        Apply worker state snapshot to local state.

        Args:
            snapshot: Worker state snapshot
        """
        worker_id = snapshot.node_id
        worker_key = f"worker:{worker_id}"

        async with self._worker_state_lock:
            worker_pool = self._registry._worker_pool
            worker_status = worker_pool.get_worker(worker_id) if worker_pool else None

            if snapshot.state == WorkerState.OFFLINE.value:
                await self._remove_worker_from_sync(
                    worker_id,
                    worker_key,
                    snapshot.version,
                    worker_pool,
                )
                return

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

            registration = self._resolve_worker_registration(snapshot, worker_status)
            if registration is None:
                return

            health_state = self._derive_worker_health_state(snapshot)
            self._state._worker_health_states[worker_id] = health_state

            if snapshot.state == WorkerState.HEALTHY.value:
                self._state.clear_worker_unhealthy_since(worker_id)

            if worker_pool:
                worker_status = await worker_pool.register_worker(registration)
                await self._apply_worker_pool_snapshot(
                    worker_pool,
                    worker_status,
                    registration,
                    snapshot,
                    health_state,
                )

            await self._state._versioned_clock.update_entity(
                worker_key, snapshot.version
            )

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
            requester_role="manager",
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
            since_version=self._state.state_version,
        )

        for peer_addr in peers:
            snapshot = await self._request_manager_peer_state(peer_addr, request)
            if snapshot:
                await self._apply_manager_peer_state(peer_addr, snapshot)

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
                    if sync_response.responder_ready and sync_response.manager_state:
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

    async def _reconcile_peer_leadership(
        self,
        peer_addr: tuple[str, int],
        snapshot: ManagerStateSnapshot,
    ) -> None:
        if not snapshot.is_leader:
            return

        peer_term = snapshot.term
        local_term = self._get_term()

        if peer_term < local_term:
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=(
                        f"State sync ignored peer leader {snapshot.node_id[:8]}... "
                        f"term {peer_term} < local {local_term}"
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return

        if self._is_leader():
            should_yield = self._should_yield_to_peer(peer_addr, peer_term)
            if should_yield:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=(
                            f"Split-brain resolved: yielding to peer leader "
                            f"{snapshot.node_id[:8]}... term {peer_term}"
                        ),
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
                await self._step_down()
                self._set_dc_leader(snapshot.node_id)
            else:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=(
                            f"Split-brain detected: retaining leadership over "
                            f"peer {snapshot.node_id[:8]}... term {peer_term}"
                        ),
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
            return

        await self._handle_elected(peer_addr, peer_term)
        self._set_dc_leader(snapshot.node_id)
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=(
                    f"State sync updated leader to {snapshot.node_id[:8]}... "
                    f"term {peer_term}"
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def _apply_manager_peer_state(
        self,
        peer_addr: tuple[str, int],
        snapshot: ManagerStateSnapshot,
    ) -> None:
        await self._reconcile_peer_leadership(peer_addr, snapshot)

        for job_id, fence_token in snapshot.job_fence_tokens.items():
            current_token = self._state._job_fencing_tokens.get(job_id, -1)
            if fence_token > current_token:
                previous_leader = self._state._job_leaders.get(job_id)
                previous_addr = self._state._job_leader_addrs.get(job_id)
                self._state._job_fencing_tokens[job_id] = fence_token

                leader_id = snapshot.job_leaders.get(job_id)
                if leader_id:
                    self._state._job_leaders[job_id] = leader_id

                leader_addr = snapshot.job_leader_addrs.get(job_id)
                leader_addr_tuple = self._normalize_job_leader_addr(leader_addr)
                if leader_addr_tuple is not None:
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
                        message=(
                            f"State sync accepted job {job_id[:8]}... "
                            f"fence {current_token} -> {fence_token}, "
                            f"leader {previous_leader} -> {leader_id}, "
                            f"addr {previous_addr} -> {leader_addr_tuple}"
                        ),
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
            else:
                self._task_runner.run(
                    self._logger.log,
                    ServerDebug(
                        message=(
                            f"State sync rejected stale fence for job {job_id[:8]}... "
                            f"token {fence_token} <= {current_token}"
                        ),
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        if self._state.set_state_version_if_higher(snapshot.version):
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"State sync updated state version to {snapshot.version}",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

        if snapshot.pending_stats_checkpoint:
            await self._import_stats_checkpoint(snapshot.pending_stats_checkpoint)

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Applied manager peer state (version {snapshot.version})",
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
            pending_stats_checkpoint=self._export_stats_checkpoint(),
        )

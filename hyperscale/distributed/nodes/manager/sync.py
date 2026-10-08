"""
Manager state synchronization.

The single implementation of the manager's state sync: pulling worker
state from the workers (re-hydrating active workflows), pulling a peer
manager's snapshot -- its workers, job leadership, fences, layer
versions and job states -- and answering peers' state_sync_request.
The cluster-leader takeover forces a full sync through here.
"""

from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.distributed.models import (
    ManagerState as ManagerStateEnum,
    ManagerStateSnapshot,
    NodeInfo,
    StateSyncRequest,
    StateSyncResponse,
    WorkerRegistration,
    WorkerState,
    WorkerStateSnapshot,
)
from hyperscale.distributed.nodes.manager.models import StateSyncNotReadyError
from hyperscale.distributed.reliability import JitterStrategy, RetryConfig, RetryExecutor
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed.jobs import JobManager
    from hyperscale.distributed.nodes.manager.models.manager_config import ManagerConfig
    from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
    from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
    from hyperscale.distributed.nodes.manager.state import ManagerState
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger


class ManagerStateSync:
    """Owns the manager's state sync (see the module docstring).

    Server operations it relies on (job state sync messages, callback
    lookup, mTLS validation, leadership and term) are injected.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        registry: "ManagerRegistry",
        leases: "ManagerLeaseCoordinator",
        job_manager: "JobManager",
        logger: "Logger",
        node_id: "NodeId",
        node_host: str,
        node_port: int,
        task_runner: "TaskRunner",
        send_tcp: Callable[..., Awaitable],
        is_cluster_leader: Callable[[], bool],
        get_current_term: Callable[[], int],
        build_job_state_sync_message: Callable[..., object],
        apply_job_state_sync_message: Callable[..., Awaitable],
        get_job_callback_addr: Callable[..., "tuple[str, int] | None"],
        validate_mtls_claims: Callable[..., Awaitable["str | None"]],
    ) -> None:
        self._state = state
        self._config = config
        self._registry = registry
        self._leases = leases
        self._job_manager = job_manager
        self._logger = logger
        self._node_id = node_id
        self._node_host = node_host
        self._node_port = node_port
        self._task_runner = task_runner
        self._send_tcp = send_tcp
        self._is_cluster_leader = is_cluster_leader
        self._get_current_term = get_current_term
        self._build_job_state_sync_message = build_job_state_sync_message
        self._apply_job_state_sync_message = apply_job_state_sync_message
        self._get_job_callback_addr = get_job_callback_addr
        self._validate_mtls_claims = validate_mtls_claims
        # AD-11: a request refused or reset in transport (a target
        # restarting), or answered not-ready (a target still starting), is
        # retried with exponential backoff (full jitter),
        # the backoffs together spanning one sync timeout -- base *
        # (2^retries - 1) = timeout: a target back within one request's
        # budget is caught, and past it SWIM's verdict governs. A timeout
        # is not retried: it already spent a whole budget on a target
        # shown unresponsive, and the takeover path syncs peers in turn.
        sync_retries = config.state_sync_retries
        self._sync_retry_config = RetryConfig(
            max_attempts=sync_retries + 1,
            base_delay=config.state_sync_timeout_seconds / (2**sync_retries - 1) if sync_retries > 0 else 0.0,
            max_delay=config.state_sync_timeout_seconds,
            jitter=JitterStrategy.FULL,
            is_retryable=lambda sync_error: isinstance(sync_error, StateSyncNotReadyError)
            or (isinstance(sync_error, OSError) and not isinstance(sync_error, TimeoutError)),
        )

    async def sync_state_from_workers(self) -> None:
        """Sync state from all workers."""
        for worker_id, worker in self._state.iter_workers():
            try:
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_role="manager",
                    cluster_id=self._config.cluster_id,
                    environment_id=self._config.environment_id,
                    since_version=self._state.state_version,
                )

                worker_addr = (worker.node.host, worker.node.port)

                async def request_worker_state() -> StateSyncResponse | None:
                    response, _clock = await self._send_tcp(
                        worker_addr,
                        "state_sync_request",
                        request.dump(),
                        timeout=self._config.state_sync_timeout_seconds,
                    )
                    # send_tcp returns transport errors rather than raising.
                    if isinstance(response, Exception):
                        raise response
                    return self._parse_ready_sync_response(
                        response, lambda: f"worker {worker_id[:8]}... not ready"
                    )

                sync_response = await RetryExecutor(self._sync_retry_config).execute(
                    request_worker_state, operation_name=f"state_sync_from_worker_{worker_id}"
                )

                await self._apply_worker_sync_response(worker_id, sync_response)

            except Exception as error:
                await self._logger.log(
                    ServerWarning(
                        message=f"State sync from worker {worker_id[:8]}... failed: {error}",
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=self._node_id.short,
                    )
                )

    @staticmethod
    def _parse_ready_sync_response(
        response: bytes | None,
        describe_not_ready: Callable[[], str],
    ) -> StateSyncResponse | None:
        """Decode a non-empty sync reply; a not-ready responder raises so AD-11 retries it."""
        if not response:
            return None
        if not (sync_response := StateSyncResponse.load(response)).responder_ready:
            raise StateSyncNotReadyError(describe_not_ready())
        return sync_response

    async def _apply_worker_sync_response(
        self,
        worker_id: str,
        sync_response: StateSyncResponse | None,
    ) -> None:
        """Refresh the worker's cores and re-hydrate its active workflows from its snapshot."""
        if sync_response is None or not sync_response.worker_state:
            return

        worker_snapshot = sync_response.worker_state
        self._refresh_worker_available_cores(worker_id, worker_snapshot)
        await self._hydrate_active_workflows_from_worker_snapshot(
            worker_snapshot
        )

    def _refresh_worker_available_cores(
        self,
        worker_id: str,
        worker_snapshot: WorkerStateSnapshot,
    ) -> None:
        """Copy the snapshot's available cores onto a registered worker."""
        if self._state.has_worker(worker_id):
            worker_reg = self._state.get_worker(worker_id)
            if worker_reg:
                worker_reg.available_cores = (
                    worker_snapshot.available_cores
                )

    async def _hydrate_active_workflows_from_worker_snapshot(
        self,
        worker_snapshot: WorkerStateSnapshot,
    ) -> None:
        """Rebuild active job/sub-workflow indexes from worker-owned state."""
        leader_addr = (self._node_host, self._node_port)
        for progress in worker_snapshot.active_workflows.values():
            if not self._leases.is_job_leader(progress.job_id):
                continue

            await self._job_manager.hydrate_worker_active_workflow(
                progress=progress,
                worker_id=worker_snapshot.node_id,
                leader_node_id=self._node_id.full,
                leader_addr=leader_addr,
                fencing_token=self._leases.get_fence_token(progress.job_id),
                callback_addr=self._get_job_callback_addr(progress.job_id),
            )

    async def sync_state_from_manager_peers(self, *, force_full: bool = False) -> int:
        """Sync state from peer managers; returns how many peers answered
        with their state -- what a caller concluding from the cluster's
        view needs to know it heard a quorum."""
        peers_answered = 0
        # Snapshot the live set before iterating. Each loop iteration
        # awaits ``send_tcp`` and now also routes through
        # ``_apply_peer_worker_snapshots``, both of which yield to the
        # event loop. A concurrent ``_handle_manager_peer_death``
        # (which calls ``remove_active_peer`` → ``discard``) can
        # mutate ``_active_manager_peers`` mid-iteration and raise
        # ``Set changed size during iteration``. The cancel handler
        # catches that on the takeover path and the client surfaces
        # it as ``Job cancellation failed: Set changed size during
        # iteration`` — a permanent failure that aborts the request.
        for peer_addr in sorted(self._state.get_active_manager_peers()):
            try:
                since_version = self._peer_sync_since_version(force_full)
                request = StateSyncRequest(
                    requester_id=self._node_id.full,
                    requester_role="manager",
                    cluster_id=self._config.cluster_id,
                    environment_id=self._config.environment_id,
                    since_version=since_version,
                )

                async def request_peer_state() -> StateSyncResponse | None:
                    response, _clock = await self._send_tcp(
                        peer_addr,
                        "state_sync_request",
                        request.dump(),
                        timeout=self._config.state_sync_timeout_seconds,
                    )
                    # send_tcp returns transport errors rather than raising.
                    if isinstance(response, Exception):
                        raise response
                    return self._parse_ready_sync_response(
                        response, lambda: f"peer {peer_addr} not ready"
                    )

                sync_response = await RetryExecutor(self._sync_retry_config).execute(
                    request_peer_state, operation_name=f"state_sync_from_peer_{peer_addr}"
                )

                peers_answered += await self._apply_peer_sync_response(sync_response, peer_addr)

            except Exception as error:
                await self._logger.log(
                    ServerWarning(
                        message=f"State sync from peer {peer_addr} failed: {error}",
                        node_host=self._node_host,
                        node_port=self._node_port,
                        node_id=self._node_id.short,
                    )
                )

        return peers_answered

    def _peer_sync_since_version(self, force_full: bool) -> int:
        """-1 asks the peer for its full state; otherwise only what is newer than ours."""
        return -1 if force_full else self._state.state_version

    async def _apply_peer_sync_response(
        self,
        sync_response: StateSyncResponse | None,
        peer_addr: tuple[str, int],
    ) -> int:
        """Apply a peer's manager snapshot; 1 when it answered with state, else 0."""
        if sync_response is None or not sync_response.manager_state:
            return 0

        peer_snapshot = sync_response.manager_state
        await self._apply_peer_worker_snapshots(peer_snapshot.workers)
        self._apply_peer_job_leadership(peer_snapshot)
        # A peer's snapshot speaks for the jobs it leads;
        # its copies of the rest are evidence, merged forward.
        await self._apply_peer_job_states(sync_response, peer_snapshot, peer_addr)
        return 1

    def _apply_peer_job_leadership(self, peer_snapshot: ManagerStateSnapshot) -> None:
        """Adopt each fenced job leadership the peer's snapshot names a leader for."""
        for job_id, fence_token in peer_snapshot.job_fence_tokens.items():
            self._apply_peer_job_leader(peer_snapshot, job_id, fence_token)

    def _apply_peer_job_leader(
        self,
        peer_snapshot: ManagerStateSnapshot,
        job_id: str,
        fence_token: int,
    ) -> None:
        """Adopt one job's leadership when the snapshot carries both its leader id and address."""
        leader_id = peer_snapshot.job_leaders.get(job_id)
        leader_addr = peer_snapshot.job_leader_addrs.get(job_id)
        if leader_id is None or leader_addr is None:
            return
        self._leases.apply_job_leadership(
            job_id=job_id,
            leader_id=leader_id,
            leader_addr=tuple(leader_addr),
            fencing_token=fence_token,
        )

    async def _apply_peer_job_states(
        self,
        sync_response: StateSyncResponse,
        peer_snapshot: ManagerStateSnapshot,
        peer_addr: tuple[str, int],
    ) -> None:
        """Merge every job state the peer shipped; it speaks for the jobs it leads."""
        for sync_msg in peer_snapshot.job_states.values():
            await self._apply_job_state_sync_message(
                sync_msg,
                peer_addr,
                sender_leads_job=(
                    sync_msg.leader_id == sync_response.responder_id
                ),
            )

    def build_peer_worker_snapshots(self) -> list[WorkerStateSnapshot]:
        """Serialize this manager's worker registry for a peer sync.

        The exact inverse of ``_apply_peer_worker_snapshots``: it
        rebuilds a ``NodeInfo`` plus ``WorkerRegistration`` from
        ``node_id``/``host``/``tcp_port``/``udp_port``/``version`` and
        the two core counts, so those are the fields that have to
        round-trip. ``active_workflows`` is left empty because nothing
        reads it on the receiving side; shipping it would be cost
        without a consumer.
        """
        return [
            WorkerStateSnapshot(
                node_id=registration.node.node_id,
                state=self._state._worker_health_states.get(
                    worker_id, WorkerState.HEALTHY.value
                ),
                total_cores=registration.total_cores,
                available_cores=registration.available_cores,
                version=registration.node.version,
                host=registration.node.host,
                tcp_port=registration.node.port,
                udp_port=registration.node.udp_port or registration.node.port,
            )
            for worker_id, registration in self._state.iter_workers()
        ]

    async def _apply_peer_worker_snapshots(
        self,
        worker_snapshots: list[WorkerStateSnapshot],
    ) -> None:
        """Reconstruct worker registrations from a peer's state snapshot.

        Workers register with managers via the ``worker_register`` RPC,
        which is delivered only to the seed managers configured on each
        worker. After a leader-kill the elected new leader may have
        zero entries in ``_manager_state._workers`` until each surviving
        worker independently re-registers — at minute-scale jitter.

        ``ManagerStateSnapshot.workers`` is shipped on every peer sync
        precisely so the new leader can recover that registry without
        waiting for worker-side re-registration. Replicating the
        snapshot to the registry is what makes the takeover-side
        ``_get_running_workflows_to_cancel`` succeed: it resolves
        ``sub_workflow.token.worker_id`` through
        ``ManagerState.get_worker`` to obtain a worker address; without
        a populated registry the lookup returns ``None``, every
        running workflow is silently skipped, ``workflows_to_cancel``
        ends up empty, no ``workflow_cancellation_complete`` ever
        decrements the pending tracker to zero, and the client times
        out on ``await_job_cancellation`` because the
        ``job_cancellation_complete`` push that the manager fires only
        from that zero-pending branch is never scheduled.

        Only the snapshot fields the cancel and dispatch paths
        actually consume are reconstructed: identity (host/tcp/udp
        ports) for addressing, cores for capacity gating, and the
        manager's own ``cluster_id`` / ``environment_id`` so a future
        ``WorkerRegistration`` consumer that inspects them sees
        consistent values. SWIM/probe/disseminator wiring deliberately
        stays untouched — those channels rejoin under the worker's
        own re-registration, where SWIM incarnation and rejoin gating
        are authoritative.
        """
        if not worker_snapshots:
            return

        for snapshot in filter(self._peer_worker_snapshot_addressable, worker_snapshots):
            await self._register_peer_worker_snapshot(snapshot)

    @staticmethod
    def _peer_worker_snapshot_addressable(snapshot: WorkerStateSnapshot) -> bool:
        """Whether the snapshot carries a node id, a host and a positive TCP port."""
        return snapshot.node_id and snapshot.host and snapshot.tcp_port > 0

    async def _register_peer_worker_snapshot(self, snapshot: WorkerStateSnapshot) -> None:
        """Register an unknown worker rebuilt from a peer snapshot (see _apply_peer_worker_snapshots)."""
        if self._state.has_worker(snapshot.node_id):
            return

        node = NodeInfo(
            node_id=snapshot.node_id,
            role="worker",
            host=snapshot.host,
            port=snapshot.tcp_port,
            datacenter=self._node_id.datacenter,
            udp_port=snapshot.udp_port,
            version=snapshot.version,
        )
        registration = WorkerRegistration(
            node=node,
            total_cores=snapshot.total_cores,
            available_cores=snapshot.available_cores,
            memory_mb=0,
            cluster_id=self._config.cluster_id,
            environment_id=self._config.environment_id,
        )
        await self._registry.register_worker(registration)

    def _not_ready_state_sync_response(self) -> bytes:
        """A responder_ready=False reply at this manager's current state version."""
        return StateSyncResponse(
            responder_id=self._node_id.full,
            current_version=self._state.state_version,
            responder_ready=False,
        ).dump()

    def _state_sync_identity_mismatch(self, request: StateSyncRequest) -> str | None:
        """Why the requester belongs to another cluster or environment, or None when it matches."""
        if request.cluster_id != self._config.cluster_id:
            return (
                "State sync cluster_id mismatch: "
                f"{request.cluster_id} != {self._config.cluster_id}"
            )
        if request.environment_id != self._config.environment_id:
            return (
                "State sync environment_id mismatch: "
                f"{request.environment_id} != {self._config.environment_id}"
            )
        return None

    async def _reject_state_sync_requester(self, request: StateSyncRequest, reason: str) -> bytes:
        """Log the identity mismatch and answer not-ready."""
        await self._logger.log(
            ServerWarning(
                message=(
                    f"State sync requester {request.requester_id} rejected: {reason}"
                ),
                node_host=self._node_host,
                node_port=self._node_port,
                node_id=self._node_id.short,
            )
        )
        return self._not_ready_state_sync_response()

    async def _state_sync_rejection(
        self,
        addr: tuple[str, int],
        request: StateSyncRequest,
    ) -> bytes | None:
        """The not-ready reply for a requester failing identity or mTLS checks; None when admitted."""
        if (reason := self._state_sync_identity_mismatch(request)) is not None:
            return await self._reject_state_sync_requester(request, reason)

        mtls_error = await self._validate_mtls_claims(
            addr,
            "State sync requester",
            request.requester_id,
        )
        if mtls_error:
            return self._not_ready_state_sync_response()
        return None

    def _build_state_sync_reply(self, request: StateSyncRequest) -> bytes:
        """This manager's state since the requester's version (version-only when it is current)."""
        current_version = self._state.state_version
        # A manager still syncing its own state has none to vouch for
        # (ManagerState has no INITIALIZING member: naming it raised on
        # every full sync).
        is_ready = (
            self._state.manager_state_enum != ManagerStateEnum.SYNCING
        )

        if request.since_version >= current_version:
            return StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=current_version,
                responder_ready=is_ready,
            ).dump()

        snapshot = ManagerStateSnapshot(
            node_id=self._node_id.full,
            datacenter=self._config.datacenter_id,
            is_leader=self._is_cluster_leader(),
            term=self._get_current_term(),
            version=current_version,
            # Job state travels in ``job_states`` (consumed by the
            # receiver); ``jobs`` read a ManagerState attribute that
            # never existed, so every full snapshot raised and peers
            # got responder_ready=False -- no peer sync, including the
            # takeover's forced full sync, ever delivered state.
            workers=self.build_peer_worker_snapshots(),
            job_leaders=dict(self._state._job_leaders),
            job_leader_addrs=dict(self._state._job_leader_addrs),
            job_fence_tokens=dict(self._state._job_fencing_tokens),
            job_states=self._build_peer_job_states(),
        )

        return StateSyncResponse(
            responder_id=self._node_id.full,
            current_version=current_version,
            responder_ready=is_ready,
            manager_state=snapshot,
        ).dump()

    def _build_peer_job_states(self) -> dict[str, object]:
        """Each job's state-sync message, keyed by job id."""
        return {
            job.job_id: self._build_job_state_sync_message(job.job_id, job)
            for job in self._job_manager.iter_jobs()
        }

    async def sync_full_state_from_manager_peers(self) -> None:
        """Force a full peer-manager state sync after leadership changes."""
        await self.sync_state_from_manager_peers(force_full=True)

    async def handle_state_sync_request(
        self,
        addr: tuple[str, int],
        data: bytes,
        clock_time: int,
    ) -> bytes:
        """Handle state sync request from peer managers or workers."""
        try:
            request = StateSyncRequest.load(data)

            if (rejection := await self._state_sync_rejection(addr, request)) is not None:
                return rejection

            await self._logger.log(
                ServerInfo(
                    message=f"State sync request from {request.requester_id[:8]}... role={request.requester_role} since_version={request.since_version}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                ),
            )

            return self._build_state_sync_reply(request)

        except Exception as error:
            await self._logger.log(
                ServerWarning(
                    message=f"State sync request failed: {error}",
                    node_host=self._node_host,
                    node_port=self._node_port,
                    node_id=self._node_id.short,
                ),
            )
            return StateSyncResponse(
                responder_id=self._node_id.full,
                current_version=0,
                responder_ready=False,
            ).dump()

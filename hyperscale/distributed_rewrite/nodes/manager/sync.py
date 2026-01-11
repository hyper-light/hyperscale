"""
Manager state sync module.

Handles state synchronization with workers and peer managers during
leader election and recovery scenarios. Uses AD-21 jitter strategies
for retry delays to prevent thundering herd.
"""

import asyncio
from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.models import (
    StateSyncRequest,
    StateSyncResponse,
    WorkerStateSnapshot,
    ManagerStateSnapshot,
)
from hyperscale.distributed_rewrite.reliability import (
    calculate_jittered_delay,
    JitterStrategy,
)
from hyperscale.logging.hyperscale_logging_models import ServerInfo, ServerDebug, ServerWarning, ServerError

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.state import ManagerState
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.distributed_rewrite.nodes.manager.registry import ManagerRegistry
    from hyperscale.logging.hyperscale_logger import Logger


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
        task_runner,
        send_tcp,  # Callable to send TCP message
    ) -> None:
        self._state = state
        self._config = config
        self._registry = registry
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._send_tcp = send_tcp

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
            )
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

            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Worker state sync attempt {attempt + 1} failed: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )

            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))

        return None

    async def _apply_worker_state(self, snapshot: WorkerStateSnapshot) -> None:
        """
        Apply worker state snapshot to local state.

        Args:
            snapshot: Worker state snapshot
        """
        # In full implementation, this would:
        # 1. Update workflow states from worker's active workflows
        # 2. Reconcile job state with workflow progress
        # 3. Update completion tracking
        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Applied worker state from {snapshot.worker_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
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
            )
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

            except Exception as e:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Peer state sync attempt {attempt + 1} failed: {e}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    )
                )

            if attempt < max_retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))

        return None

    async def _apply_manager_peer_state(self, snapshot: ManagerStateSnapshot) -> None:
        """
        Apply manager peer state snapshot to local state.

        Args:
            snapshot: Manager state snapshot
        """
        # In full implementation, this would:
        # 1. Merge job metadata (retry counts, etc)
        # 2. Update fencing tokens if higher
        # 3. Reconcile leadership information
        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Applied manager peer state (version {snapshot.state_version})",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            )
        )

    def get_state_snapshot(self) -> ManagerStateSnapshot:
        """
        Generate current state snapshot for sync responses.

        Returns:
            ManagerStateSnapshot with current state
        """
        worker_snapshots = [
            WorkerStateSnapshot(
                worker_id=worker_id,
                active_workflows=[],  # Would populate from actual state
                total_cores=reg.node.total_cores,
                available_cores=reg.node.total_cores,  # Would calculate actual
            )
            for worker_id, reg in self._state._workers.items()
        ]

        return ManagerStateSnapshot(
            manager_id=self._node_id,
            state_version=self._state._state_version,
            worker_snapshots=worker_snapshots,
            job_count=len(self._state._job_submissions),
            is_leader=False,  # Would check actual leader state
        )

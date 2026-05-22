"""
Worker state dissemination for cross-manager visibility (AD-48).
"""

import asyncio
import time
from typing import TYPE_CHECKING, Any, Callable, Coroutine

from hyperscale.distributed.models import WorkerRegistration, WorkerState
from hyperscale.distributed.models.worker_state import (
    WorkerStateUpdate,
    WorkerListResponse,
    WorkerListRequest,
    WorkflowReassignmentBatch,
)
from hyperscale.distributed.swim.gossip.worker_state_gossip_buffer import (
    WorkerStateGossipBuffer,
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
    from hyperscale.distributed.jobs.worker_pool import WorkerPool
    from hyperscale.distributed.taskex import TaskRunner
    from hyperscale.logging import Logger

SendTcpFunc = Callable[
    [tuple[str, int], str, bytes, float],
    Coroutine[Any, Any, bytes | None],
]


class WorkerDisseminator:
    """
    Handles cross-manager worker state dissemination.

    Broadcasts worker events (register, death) to peer managers via TCP
    and adds updates to gossip buffer for steady-state dissemination.
    """

    def __init__(
        self,
        state: "ManagerState",
        config: "ManagerConfig",
        worker_pool: "WorkerPool",
        logger: "Logger",
        node_id: str,
        datacenter: str,
        task_runner: "TaskRunner",
        send_tcp: SendTcpFunc,
        gossip_buffer: WorkerStateGossipBuffer,
    ) -> None:
        self._state: "ManagerState" = state
        self._config: "ManagerConfig" = config
        self._worker_pool: "WorkerPool" = worker_pool
        self._logger: "Logger" = logger
        self._node_id: str = node_id
        self._datacenter: str = datacenter
        self._task_runner: "TaskRunner" = task_runner
        self._send_tcp: SendTcpFunc = send_tcp
        self._gossip_buffer: WorkerStateGossipBuffer = gossip_buffer

        self._worker_incarnations: dict[str, int] = {}
        self._incarnation_lock: asyncio.Lock = asyncio.Lock()

    async def _get_next_incarnation(self, worker_id: str) -> int:
        async with self._incarnation_lock:
            current = self._worker_incarnations.get(worker_id, 0)
            next_incarnation = current + 1
            self._worker_incarnations[worker_id] = next_incarnation
            return next_incarnation

    def get_worker_incarnation(self, worker_id: str) -> int:
        return self._worker_incarnations.get(worker_id, 0)

    def should_accept_worker_update(
        self,
        worker_id: str,
        incoming_incarnation: int,
    ) -> bool:
        current = self._worker_incarnations.get(worker_id, 0)
        return incoming_incarnation > current

    async def broadcast_worker_registered(
        self,
        registration: WorkerRegistration,
    ) -> None:
        worker_id = registration.node.node_id
        incarnation = await self._get_next_incarnation(worker_id)

        update = WorkerStateUpdate(
            worker_id=worker_id,
            owner_manager_id=self._node_id,
            host=registration.node.host,
            tcp_port=registration.node.port,
            udp_port=registration.node.udp_port or registration.node.port,
            state="registered",
            incarnation=incarnation,
            total_cores=registration.total_cores,
            available_cores=registration.available_cores,
            timestamp=time.monotonic(),
            datacenter=self._datacenter,
        )

        self._gossip_buffer.add_update(
            update,
            number_of_managers=len(self._state._active_manager_peers) + 1,
        )

        await self._broadcast_to_peers(update)

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Broadcast worker registration: {worker_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def broadcast_worker_dead(
        self,
        worker_id: str,
        reason: str,
    ) -> None:
        incarnation = await self._get_next_incarnation(worker_id)

        worker = self._worker_pool.get_worker(worker_id)
        host = ""
        tcp_port = 0
        udp_port = 0
        total_cores = 0

        if worker and worker.registration:
            host = worker.registration.node.host
            tcp_port = worker.registration.node.port
            udp_port = worker.registration.node.udp_port or tcp_port

        update = WorkerStateUpdate(
            worker_id=worker_id,
            owner_manager_id=self._node_id,
            host=host,
            tcp_port=tcp_port,
            udp_port=udp_port,
            state=reason,
            incarnation=incarnation,
            total_cores=total_cores,
            available_cores=0,
            timestamp=time.monotonic(),
            datacenter=self._datacenter,
        )

        self._gossip_buffer.add_update(
            update,
            number_of_managers=len(self._state._active_manager_peers) + 1,
        )

        await self._broadcast_to_peers(update)

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"Broadcast worker {reason}: {worker_id[:8]}...",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    async def broadcast_workers_draining(
        self,
        worker_ids: set[str],
        reason: str,
    ) -> None:
        """Broadcast planned worker drain intent to peer managers."""
        updates: list[WorkerStateUpdate] = []

        for worker_id in worker_ids:
            worker = self._worker_pool.get_worker(worker_id)
            if worker is None or worker.registration is None:
                continue

            incarnation = await self._get_next_incarnation(worker_id)
            node = worker.registration.node
            updates.append(
                WorkerStateUpdate(
                    worker_id=worker_id,
                    owner_manager_id=self._node_id,
                    host=node.host,
                    tcp_port=node.port,
                    udp_port=node.udp_port or node.port,
                    state="draining",
                    incarnation=incarnation,
                    total_cores=worker.total_cores,
                    available_cores=0,
                    timestamp=time.monotonic(),
                    datacenter=self._datacenter,
                )
            )

        for update in updates:
            self._gossip_buffer.add_update(
                update,
                number_of_managers=len(self._state._active_manager_peers) + 1,
            )

        if updates:
            await asyncio.gather(
                *(self._broadcast_to_peers(update) for update in updates),
                return_exceptions=True,
            )

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=(
                        f"Broadcast worker drain intent for {len(updates)} workers "
                        f"({reason})"
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

    async def _broadcast_to_peers(self, update: WorkerStateUpdate) -> None:
        peers = list(self._state._active_manager_peers)
        if not peers:
            return

        update_bytes = update.to_bytes()

        async def send_to_peer(peer_addr: tuple[str, int]) -> None:
            try:
                await asyncio.wait_for(
                    self._send_tcp(
                        peer_addr,
                        "worker_state_update",
                        update_bytes,
                        5.0,
                    ),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Timeout broadcasting worker state to {peer_addr}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
            except Exception as broadcast_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Failed to broadcast worker state to {peer_addr}: {broadcast_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        await asyncio.gather(
            *[send_to_peer(peer) for peer in peers],
            return_exceptions=True,
        )

    async def handle_worker_state_update(
        self,
        update: WorkerStateUpdate,
        source_addr: tuple[str, int],
    ) -> bool:
        if update.owner_manager_id == self._node_id:
            return False

        if not self.should_accept_worker_update(update.worker_id, update.incarnation):
            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"Rejected stale worker update for {update.worker_id[:8]}... (inc={update.incarnation})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
            return False

        async with self._incarnation_lock:
            self._worker_incarnations[update.worker_id] = update.incarnation

        if update.is_alive_state():
            await self._worker_pool.register_remote_worker(update)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=(
                        f"Registered remote worker {update.worker_id[:8]}... "
                        f"from manager {update.owner_manager_id[:8]}..."
                    ),
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )
        else:
            await self._worker_pool.deregister_remote_worker(update.worker_id)

            self._task_runner.run(
                self._logger.log,
                ServerInfo(
                    message=f"Deregistered remote worker {update.worker_id[:8]}... (reason={update.state})",
                    node_host=self._config.host,
                    node_port=self._config.tcp_port,
                    node_id=self._node_id,
                ),
            )

        self._gossip_buffer.add_update(
            update,
            number_of_managers=len(self._state._active_manager_peers) + 1,
        )

        return True

    async def request_worker_list_from_peers(self) -> None:
        peers = list(self._state._active_manager_peers)
        if not peers:
            return

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Requesting worker lists from {len(peers)} peer managers",
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

        request = WorkerListRequest(
            requester_id=self._node_id,
            requester_datacenter=self._datacenter,
        )

        async def request_from_peer(peer_addr: tuple[str, int]) -> None:
            try:
                response = await asyncio.wait_for(
                    self._send_tcp(
                        peer_addr,
                        "list_workers",
                        request.dump(),
                        10.0,
                    ),
                    timeout=10.0,
                )

                if response:
                    worker_list = WorkerListResponse.from_bytes(response)
                    if worker_list:
                        for worker_update in worker_list.workers:
                            await self.handle_worker_state_update(
                                worker_update, peer_addr
                            )

                        self._task_runner.run(
                            self._logger.log,
                            ServerDebug(
                                message=f"Received {len(worker_list.workers)} workers from peer {peer_addr}",
                                node_host=self._config.host,
                                node_port=self._config.tcp_port,
                                node_id=self._node_id,
                            ),
                        )

            except asyncio.TimeoutError:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Timeout requesting worker list from {peer_addr}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
            except Exception as request_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Failed to request worker list from {peer_addr}: {request_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        await asyncio.gather(
            *[request_from_peer(peer) for peer in peers],
            return_exceptions=True,
        )

    async def push_registration_to_remote_workers(
        self,
        is_leader: bool,
        term: int,
    ) -> None:
        """Push ``ManagerToWorkerRegistration`` to every remote worker.

        After ``request_worker_list_from_peers`` populates the remote-
        worker registry, this manager knows *about* the workers
        (host, tcp_port, udp_port) but the workers do not yet know
        about *this* manager — their TCP-level ``_known_managers``
        registry was built from whichever manager(s) they last
        registered with. Without an explicit notification, the
        workers' next manager-aware operation (heartbeat-piggyback
        AD-26 extension request, ``send_cancellation_complete`` fall-
        back) cannot route through this manager. AD-48 already
        documents the bidirectional registration but the manager-side
        send was never wired.

        Each push uses the same payload shape the manager-initiated
        ``ManagerToWorkerRegistration`` path on the worker
        (``handle_manager_register``) already processes: full
        ``ManagerInfo`` for this manager + the active peer-manager
        list so the worker rebuilds its known-managers view in one
        round-trip. The worker's handler is idempotent — if the
        worker already knows us it simply refreshes the entry.

        Failures are fire-and-forget at the per-worker level: the
        steady-state SWIM gossip / dead-manager-reap loops on the
        worker will eventually re-converge. We log warnings so
        operationally-visible regressions don't slip through.
        """
        remote_workers = self._worker_pool.iter_remote_workers()
        if not remote_workers:
            return

        from hyperscale.distributed.models import (
            ManagerInfo,
            ManagerToWorkerRegistration,
        )

        manager_info = ManagerInfo(
            node_id=self._node_id,
            tcp_host=self._config.host,
            tcp_port=self._config.tcp_port,
            udp_host=self._config.host,
            udp_port=self._config.udp_port,
            datacenter=self._datacenter,
            is_leader=is_leader,
        )
        known_managers = self._state.get_active_known_manager_peers()

        registration = ManagerToWorkerRegistration(
            manager=manager_info,
            is_leader=is_leader,
            term=term,
            known_managers=known_managers,
        )
        payload = registration.dump()

        async def push_to_worker(worker_addr: tuple[str, int]) -> None:
            try:
                await asyncio.wait_for(
                    self._send_tcp(
                        worker_addr,
                        "manager_register",
                        payload,
                        5.0,
                    ),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=(
                            f"Manager-to-worker register push to {worker_addr} "
                            f"timed out; SWIM gossip will eventually converge"
                        ),
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
            except Exception as push_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=(
                            f"Manager-to-worker register push to {worker_addr} "
                            f"failed: {push_error}"
                        ),
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        addresses: list[tuple[str, int]] = []
        for worker in remote_workers:
            if worker.registration is None:
                continue
            node = worker.registration.node
            addresses.append((node.host, node.port))

        if not addresses:
            return

        await asyncio.gather(
            *[push_to_worker(addr) for addr in addresses],
            return_exceptions=True,
        )

    def build_worker_list_response(self) -> WorkerListResponse:
        workers = self._worker_pool.iter_workers()

        updates = [
            WorkerStateUpdate(
                worker_id=worker.worker_id,
                owner_manager_id=self._node_id,
                host=worker.registration.node.host if worker.registration else "",
                tcp_port=worker.registration.node.port if worker.registration else 0,
                udp_port=(
                    worker.registration.node.udp_port or worker.registration.node.port
                    if worker.registration
                    else 0
                ),
                state=(
                    "draining"
                    if worker.health == WorkerState.DRAINING
                    else "registered"
                ),
                incarnation=self.get_worker_incarnation(worker.worker_id),
                total_cores=worker.total_cores,
                available_cores=worker.available_cores,
                timestamp=time.monotonic(),
                datacenter=self._datacenter,
            )
            for worker in workers
            if worker.registration and not getattr(worker, "is_remote", False)
        ]

        return WorkerListResponse(
            manager_id=self._node_id,
            workers=updates,
        )

    async def broadcast_workflow_reassignments(
        self,
        failed_worker_id: str,
        reason: str,
        reassignments: list[tuple[str, str, str]],
    ) -> None:
        if not reassignments:
            return

        peers = list(self._state._active_manager_peers)
        if not peers:
            return

        batch = WorkflowReassignmentBatch(
            originating_manager_id=self._node_id,
            failed_worker_id=failed_worker_id,
            reason=reason,
            timestamp=time.monotonic(),
            datacenter=self._datacenter,
            reassignments=reassignments,
        )

        batch_bytes = batch.to_bytes()

        async def send_to_peer(peer_addr: tuple[str, int]) -> None:
            try:
                await asyncio.wait_for(
                    self._send_tcp(
                        peer_addr,
                        "workflow_reassignment",
                        batch_bytes,
                        5.0,
                    ),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Timeout broadcasting workflow reassignment to {peer_addr}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )
            except Exception as broadcast_error:
                self._task_runner.run(
                    self._logger.log,
                    ServerWarning(
                        message=f"Failed to broadcast workflow reassignment to {peer_addr}: {broadcast_error}",
                        node_host=self._config.host,
                        node_port=self._config.tcp_port,
                        node_id=self._node_id,
                    ),
                )

        await asyncio.gather(
            *[send_to_peer(peer) for peer in peers],
            return_exceptions=True,
        )

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=(
                    f"Broadcast {len(reassignments)} workflow reassignments "
                    f"from failed worker {failed_worker_id[:8]}..."
                ),
                node_host=self._config.host,
                node_port=self._config.tcp_port,
                node_id=self._node_id,
            ),
        )

    def get_gossip_buffer(self) -> WorkerStateGossipBuffer:
        return self._gossip_buffer

    def get_stats(self) -> dict[str, Any]:
        return {
            "tracked_worker_incarnations": len(self._worker_incarnations),
            "gossip_buffer_stats": self._gossip_buffer.get_stats(),
        }

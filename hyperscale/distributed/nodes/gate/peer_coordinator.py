"""
Gate peer coordination for GateServer.

Handles gate-to-gate peer management including:
- Peer failure and recovery handling
- Gate heartbeat processing
- Consistent hash ring management for job ownership
- Job forwarding tracker registration
"""

import asyncio
import random
import time
from typing import TYPE_CHECKING, Awaitable, Callable

from hyperscale.distributed.models import (
    GateHeartbeat,
    GateInfo,
)
from hyperscale.distributed.health import GateHealthState
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.logging import Logger
from hyperscale.logging.hyperscale_logging_models import (
    ServerDebug,
    ServerInfo,
    ServerWarning,
)

from .state import GateRuntimeState

if TYPE_CHECKING:
    from hyperscale.distributed.swim.core import NodeId
    from hyperscale.distributed.jobs.gates.consistent_hash_ring import (
        ConsistentHashRing,
    )
    from hyperscale.distributed.jobs import JobLeadershipTracker
    from hyperscale.distributed.jobs.gates.job_forwarding_tracker import (
        JobForwardingTracker,
    )
    from hyperscale.distributed.server.events.lamport_clock import VersionedStateClock
    from hyperscale.distributed.taskex import TaskRunner


class GatePeerCoordinator:
    """
    Coordinates gate peer operations.

    Handles peer lifecycle events (failure, recovery), heartbeat processing,
    and maintains peer tracking structures for job routing.
    """

    def __init__(
        self,
        state: GateRuntimeState,
        logger: Logger,
        task_runner: "TaskRunner",
        peer_discovery: DiscoveryService,
        job_hash_ring: "ConsistentHashRing",
        job_forwarding_tracker: "JobForwardingTracker",
        job_leadership_tracker: "JobLeadershipTracker",
        versioned_clock: "VersionedStateClock",
        gate_health_config: dict,
        recovery_semaphore: asyncio.Semaphore,
        recovery_jitter_min: float,
        recovery_jitter_max: float,
        get_node_id: Callable[[], "NodeId"],
        get_host: Callable[[], str],
        get_tcp_port: Callable[[], int],
        get_udp_port: Callable[[], int],
        confirm_peer: Callable[[tuple[str, int]], None],
        handle_job_leader_failure: Callable[[tuple[str, int]], "asyncio.Task"],
        remove_peer_circuit: Callable[[tuple[str, int]], Awaitable[None]],
        is_leader: Callable[[], bool] | None = None,
    ) -> None:
        """
        Initialize the peer coordinator.

        Args:
            state: Runtime state container
            logger: Async logger instance
            task_runner: Background task executor
            peer_discovery: Discovery service for peer selection
            job_hash_ring: Consistent hash ring for job ownership
            job_forwarding_tracker: Tracks cross-gate job forwarding
            job_leadership_tracker: Tracks per-job leadership
            versioned_clock: Version tracking for stale update rejection
            gate_health_config: Configuration for gate health states
            recovery_semaphore: Limits concurrent recovery operations
            recovery_jitter_min: Minimum jitter for recovery delay
            recovery_jitter_max: Maximum jitter for recovery delay
            get_node_id: Callback to get this gate's node ID
            get_host: Callback to get this gate's host
            get_tcp_port: Callback to get this gate's TCP port
            get_udp_port: Callback to get this gate's UDP port
            confirm_peer: Callback to confirm peer in SWIM layer
            handle_job_leader_failure: Callback to handle job leader failure
            remove_peer_circuit: Callback to clear peer circuit breakers
        """
        self._state: GateRuntimeState = state
        self._logger: Logger = logger
        self._task_runner: "TaskRunner" = task_runner
        self._peer_discovery: DiscoveryService = peer_discovery
        self._job_hash_ring: "ConsistentHashRing" = job_hash_ring
        self._job_forwarding_tracker: "JobForwardingTracker" = job_forwarding_tracker
        self._job_leadership_tracker: "JobLeadershipTracker" = job_leadership_tracker
        self._versioned_clock: "VersionedStateClock" = versioned_clock
        self._gate_health_config: dict = gate_health_config
        self._recovery_semaphore: asyncio.Semaphore = recovery_semaphore
        self._recovery_jitter_min: float = recovery_jitter_min
        self._recovery_jitter_max: float = recovery_jitter_max
        self._get_node_id: Callable[[], "NodeId"] = get_node_id
        self._get_host: Callable[[], str] = get_host
        self._get_tcp_port: Callable[[], int] = get_tcp_port
        self._get_udp_port: Callable[[], int] = get_udp_port
        self._confirm_peer: Callable[[tuple[str, int]], None] = confirm_peer
        self._handle_job_leader_failure: Callable[[tuple[str, int]], "asyncio.Task"] = (
            handle_job_leader_failure
        )
        self._remove_peer_circuit: Callable[[tuple[str, int]], Awaitable[None]] = (
            remove_peer_circuit
        )
        self._is_leader: Callable[[], bool] = is_leader or (lambda: False)

    async def on_peer_confirmed(self, peer: tuple[str, int]) -> None:
        """
        Add confirmed peer to active peer sets (AD-29).

        Called when a peer is confirmed via successful SWIM communication.
        This is the ONLY place where peers should be added to active sets,
        ensuring failure detection only applies to peers we've communicated with.

        Args:
            peer: The UDP address of the confirmed peer.
        """
        tcp_addr = self._state._gate_udp_to_tcp.get(peer)
        if not tcp_addr:
            return

        await self._state.add_active_peer(tcp_addr)
        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=f"AD-29: Gate peer {tcp_addr[0]}:{tcp_addr[1]} confirmed via SWIM, added to active sets",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )

    async def handle_peer_failure(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """
        Handle a gate peer becoming unavailable (detected via SWIM).

        This is important for split-brain awareness and per-job leadership takeover.

        Args:
            udp_addr: UDP address of the failed peer
            tcp_addr: TCP address of the failed peer
        """
        peer_lock = await self._state.get_or_create_peer_lock(tcp_addr)
        async with peer_lock:
            await self._state.increment_peer_epoch(tcp_addr)
            await self._state.remove_active_peer(tcp_addr)
            self._state.mark_peer_unhealthy(tcp_addr, time.monotonic())

            peer_host, peer_port = tcp_addr
            peer_id = f"{peer_host}:{peer_port}"
            self._peer_discovery.remove_peer(peer_id)

            peer_heartbeat = self._state._gate_peer_info.get(udp_addr)
            real_peer_id = peer_heartbeat.node_id if peer_heartbeat else peer_id

            if peer_heartbeat:
                await self._job_hash_ring.remove_node(peer_heartbeat.node_id)
            else:
                await self._job_hash_ring.remove_node(peer_id)

            self._job_forwarding_tracker.unregister_peer(real_peer_id)

        await self._remove_peer_circuit(tcp_addr)

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Gate peer at {tcp_addr} (UDP: {udp_addr}) marked as DEAD, removed from hash ring",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )

        await self._handle_job_leader_failure(tcp_addr)

        active_count = self._state.get_active_peer_count() + 1
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Gate cluster: {active_count} active",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )

    async def handle_peer_recovery(
        self,
        udp_addr: tuple[str, int],
        tcp_addr: tuple[str, int],
    ) -> None:
        """
        Handle a gate peer recovering/rejoining the cluster.

        Uses epoch checking to detect if failure handler ran during jitter,
        and recovery semaphore to prevent thundering herd.

        Args:
            udp_addr: UDP address of the recovered peer
            tcp_addr: TCP address of the recovered peer
        """
        peer_lock = await self._state.get_or_create_peer_lock(tcp_addr)

        async with peer_lock:
            initial_epoch = await self._state.get_peer_epoch(tcp_addr)

        async with self._recovery_semaphore:
            if self._recovery_jitter_max > 0:
                jitter = random.uniform(
                    self._recovery_jitter_min, self._recovery_jitter_max
                )
                await asyncio.sleep(jitter)

            async with peer_lock:
                current_epoch = await self._state.get_peer_epoch(tcp_addr)
                if current_epoch != initial_epoch:
                    self._task_runner.run(
                        self._logger.log,
                        ServerDebug(
                            message=f"Gate peer recovery for {tcp_addr} aborted: epoch changed "
                            f"({initial_epoch} -> {current_epoch}) during jitter",
                            node_host=self._get_host(),
                            node_port=self._get_tcp_port(),
                            node_id=self._get_node_id().short,
                        ),
                    )
                    return

                await self._state.add_active_peer(tcp_addr)
                self._state.mark_peer_healthy(tcp_addr)

                peer_host, peer_port = tcp_addr
                synthetic_peer_id = f"{peer_host}:{peer_port}"
                self._peer_discovery.add_peer(
                    peer_id=synthetic_peer_id,
                    host=peer_host,
                    port=peer_port,
                    role="gate",
                )

        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Gate peer at {tcp_addr} (UDP: {udp_addr}) has REJOINED the cluster",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )

        self._task_runner.run(self._request_state_sync_from_peer, tcp_addr)

        active_count = self._state.get_active_peer_count() + 1
        self._task_runner.run(
            self._logger.log,
            ServerInfo(
                message=f"Gate cluster: {active_count} active",
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )

    async def cleanup_dead_peer(self, peer_addr: tuple[str, int]) -> set[str]:
        """
        Clean up tracking for a reaped peer gate.

        Args:
            peer_addr: TCP address of the dead peer

        Returns:
            Set of gate IDs removed from runtime state.
        """
        udp_addr: tuple[str, int] | None = None
        peer_heartbeat: GateHeartbeat | None = None

        for candidate_udp_addr, candidate_tcp_addr in list(
            self._state.iter_udp_to_tcp_mappings()
        ):
            if candidate_tcp_addr == peer_addr:
                udp_addr = candidate_udp_addr
                peer_heartbeat = self._state.get_gate_peer_heartbeat(udp_addr)
                break

        peer_host, peer_port = peer_addr
        fallback_peer_id = f"{peer_host}:{peer_port}"
        gate_id = peer_heartbeat.node_id if peer_heartbeat else fallback_peer_id

        self._state.mark_peer_healthy(peer_addr)

        self._peer_discovery.remove_peer(fallback_peer_id)
        if gate_id != fallback_peer_id:
            self._peer_discovery.remove_peer(gate_id)

        await self._job_hash_ring.remove_node(gate_id)
        self._job_forwarding_tracker.unregister_peer(gate_id)

        gate_ids_to_remove = self._state.cleanup_dead_peer(peer_addr)

        self._task_runner.run(
            self._logger.log,
            ServerDebug(
                message=(
                    "Cleaned up tracking for reaped gate peer "
                    f"{peer_addr} (gate_id={gate_id}, udp_addr={udp_addr})"
                ),
                node_host=self._get_host(),
                node_port=self._get_tcp_port(),
                node_id=self._get_node_id().short,
            ),
        )

        return gate_ids_to_remove

    async def handle_gate_heartbeat(
        self,
        heartbeat: GateHeartbeat,
        source_addr: tuple[str, int],
    ) -> None:
        """
        Handle GateHeartbeat received from peer gates via SWIM.

        Updates peer tracking, discovery service, hash ring, and health states.

        Args:
            heartbeat: Received gate heartbeat
            source_addr: UDP source address of the heartbeat
        """
        if await self._versioned_clock.is_entity_stale(
            heartbeat.node_id, heartbeat.version
        ):
            return

        peer_tcp_host = heartbeat.tcp_host if heartbeat.tcp_host else source_addr[0]
        peer_tcp_port = heartbeat.tcp_port if heartbeat.tcp_port else source_addr[1]
        peer_tcp_addr = (peer_tcp_host, peer_tcp_port)

        self._confirm_peer(source_addr)

        udp_addr = source_addr
        if udp_addr not in self._state._gate_udp_to_tcp:
            self._state._gate_udp_to_tcp[udp_addr] = peer_tcp_addr
        elif self._state._gate_udp_to_tcp[udp_addr] != peer_tcp_addr:
            old_tcp_addr = self._state._gate_udp_to_tcp[udp_addr]
            await self._state.remove_active_peer(old_tcp_addr)
            self._state.cleanup_peer_udp_tracking(old_tcp_addr)
            self._state.cleanup_peer_tcp_tracking(old_tcp_addr)
            self._state._gate_udp_to_tcp[udp_addr] = peer_tcp_addr

        self._state._gate_peer_info[source_addr] = heartbeat

        self._peer_discovery.add_peer(
            peer_id=heartbeat.node_id,
            host=peer_tcp_host,
            port=peer_tcp_port,
            role="gate",
        )

        await self._job_hash_ring.add_node(
            node_id=heartbeat.node_id,
            tcp_host=peer_tcp_host,
            tcp_port=peer_tcp_port,
        )

        self._job_forwarding_tracker.register_peer(
            gate_id=heartbeat.node_id,
            tcp_host=peer_tcp_host,
            tcp_port=peer_tcp_port,
        )

        gate_id = heartbeat.node_id
        health_state = self._state._gate_peer_health.get(gate_id)
        if not health_state:
            health_state = GateHealthState(
                gate_id=gate_id,
                config=self._gate_health_config,
            )
            self._state._gate_peer_health[gate_id] = health_state

        health_state.update_liveness(success=True)
        health_state.update_readiness(
            has_dc_connectivity=heartbeat.connected_dc_count > 0,
            connected_dc_count=heartbeat.connected_dc_count,
            overload_state=getattr(heartbeat, "overload_state", "healthy"),
        )

        self._task_runner.run(
            self._versioned_clock.update_entity, heartbeat.node_id, heartbeat.version
        )

    def get_healthy_gates(self) -> list[GateInfo]:
        gates: list[GateInfo] = []

        node_id = self._get_node_id()
        gates.append(
            GateInfo(
                node_id=node_id.full,
                tcp_host=self._get_host(),
                tcp_port=self._get_tcp_port(),
                udp_host=self._get_host(),
                udp_port=self._get_udp_port(),
                datacenter=node_id.datacenter,
                is_leader=self._is_leader(),
            )
        )

        for tcp_addr in list(self._state.get_active_peers()):
            udp_addr: tuple[str, int] | None = None
            for udp, tcp in list(self._state.iter_udp_to_tcp_mappings()):
                if tcp == tcp_addr:
                    udp_addr = udp
                    break

            if udp_addr is None:
                udp_addr = tcp_addr

            peer_heartbeat = self._state.get_gate_peer_heartbeat(udp_addr)

            if peer_heartbeat:
                gates.append(
                    GateInfo(
                        node_id=peer_heartbeat.node_id,
                        tcp_host=tcp_addr[0],
                        tcp_port=tcp_addr[1],
                        udp_host=udp_addr[0],
                        udp_port=udp_addr[1],
                        datacenter=peer_heartbeat.datacenter,
                        is_leader=peer_heartbeat.is_leader,
                    )
                )
            else:
                gates.append(
                    GateInfo(
                        node_id=f"gate-{tcp_addr[0]}:{tcp_addr[1]}",
                        tcp_host=tcp_addr[0],
                        tcp_port=tcp_addr[1],
                        udp_host=udp_addr[0],
                        udp_port=udp_addr[1],
                        datacenter=node_id.datacenter,
                        is_leader=False,
                    )
                )

        return gates

    def get_known_gates_for_piggyback(self) -> dict[str, tuple[str, int, str, int]]:
        """
        Get known gates for piggybacking in SWIM heartbeats.

        Returns:
            Dict mapping gate_id -> (tcp_host, tcp_port, udp_host, udp_port)
        """
        return {
            gate_id: (
                gate_info.tcp_host,
                gate_info.tcp_port,
                gate_info.udp_host,
                gate_info.udp_port,
            )
            for gate_id, gate_info in self._state._known_gates.items()
        }

    async def _request_state_sync_from_peer(
        self,
        peer_tcp_addr: tuple[str, int],
    ) -> None:
        """
        Request job leadership state from a peer gate after it rejoins.

        This ensures we have up-to-date information about which jobs the
        rejoined peer was leading, allowing proper orphan detection.
        """
        try:
            peer_jobs = self._job_leadership_tracker.get_jobs_led_by_addr(peer_tcp_addr)
            if peer_jobs:
                self._task_runner.run(
                    self._logger.log,
                    ServerDebug(
                        message=f"Peer {peer_tcp_addr} rejoined with {len(peer_jobs)} known jobs",
                        node_host=self._get_host(),
                        node_port=self._get_tcp_port(),
                        node_id=self._get_node_id().short,
                    ),
                )

            self._state.clear_dead_leader(peer_tcp_addr)

            self._task_runner.run(
                self._logger.log,
                ServerDebug(
                    message=f"State sync completed for rejoined peer {peer_tcp_addr}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )
        except Exception as error:
            self._task_runner.run(
                self._logger.log,
                ServerWarning(
                    message=f"Failed to sync state from rejoined peer {peer_tcp_addr}: {error}",
                    node_host=self._get_host(),
                    node_port=self._get_tcp_port(),
                    node_id=self._get_node_id().short,
                ),
            )

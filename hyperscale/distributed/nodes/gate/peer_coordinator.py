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
from typing import TYPE_CHECKING, Callable

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
    from hyperscale.distributed.hash_ring import ConsistentHashRing
    from hyperscale.distributed.tracking import (
        JobForwardingTracker,
        JobLeadershipTracker,
    )
    from hyperscale.distributed.versioning import VersionedClock
    from taskex import TaskRunner


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
        versioned_clock: "VersionedClock",
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
        """
        self._state = state
        self._logger = logger
        self._task_runner = task_runner
        self._peer_discovery = peer_discovery
        self._job_hash_ring = job_hash_ring
        self._job_forwarding_tracker = job_forwarding_tracker
        self._job_leadership_tracker = job_leadership_tracker
        self._versioned_clock = versioned_clock
        self._gate_health_config = gate_health_config
        self._recovery_semaphore = recovery_semaphore
        self._recovery_jitter_min = recovery_jitter_min
        self._recovery_jitter_max = recovery_jitter_max
        self._get_node_id = get_node_id
        self._get_host = get_host
        self._get_tcp_port = get_tcp_port
        self._get_udp_port = get_udp_port
        self._confirm_peer = confirm_peer
        self._handle_job_leader_failure = handle_job_leader_failure

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
        peer_lock = self._state.get_or_create_peer_lock(tcp_addr)

        async with peer_lock:
            initial_epoch = self._state.get_peer_epoch(tcp_addr)

        async with self._recovery_semaphore:
            if self._recovery_jitter_max > 0:
                jitter = random.uniform(
                    self._recovery_jitter_min, self._recovery_jitter_max
                )
                await asyncio.sleep(jitter)

            async with peer_lock:
                current_epoch = self._state.get_peer_epoch(tcp_addr)
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

                self._state.add_active_peer(tcp_addr)

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
        if self._versioned_clock.is_entity_stale(heartbeat.node_id, heartbeat.version):
            return

        self._state._gate_peer_info[source_addr] = heartbeat

        peer_tcp_host = heartbeat.tcp_host if heartbeat.tcp_host else source_addr[0]
        peer_tcp_port = heartbeat.tcp_port if heartbeat.tcp_port else source_addr[1]
        peer_tcp_addr = (peer_tcp_host, peer_tcp_port)

        self._confirm_peer(source_addr)

        udp_addr = source_addr
        if udp_addr not in self._state._gate_udp_to_tcp:
            self._state._gate_udp_to_tcp[udp_addr] = peer_tcp_addr
        elif self._state._gate_udp_to_tcp[udp_addr] != peer_tcp_addr:
            old_tcp_addr = self._state._gate_udp_to_tcp[udp_addr]
            self._state.remove_active_peer(old_tcp_addr)
            self._state._gate_udp_to_tcp[udp_addr] = peer_tcp_addr

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
        """
        Build list of all known healthy gates for manager discovery.

        Includes self and all active peer gates.

        Returns:
            List of GateInfo for healthy gates
        """
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
                is_leader=False,
            )
        )

        for tcp_addr in list(self._state._active_gate_peers):
            udp_addr: tuple[str, int] | None = None
            for udp, tcp in list(self._state._gate_udp_to_tcp.items()):
                if tcp == tcp_addr:
                    udp_addr = udp
                    break

            if udp_addr is None:
                udp_addr = tcp_addr

            peer_heartbeat = self._state._gate_peer_info.get(udp_addr)

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

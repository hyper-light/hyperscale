"""
Federated Health Monitor for cross-cluster health probing.

Gates use this to monitor datacenter manager clusters that are globally
distributed. Uses a SWIM-style probe/ack mechanism but with:
- Higher latency tolerance (50-300ms RTT)
- Longer suspicion timeouts (30s)
- No gossip exchange (irrelevant across clusters)
- Aggregate health responses from DC leaders

This is NOT cluster membership - just health monitoring using probe/ack.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Awaitable

from hyperscale.distributed.models import Message


class DCReachability(Enum):
    """Network reachability state for a datacenter."""

    REACHABLE = "reachable"
    SUSPECTED = "suspected"
    UNREACHABLE = "unreachable"


@dataclass(slots=True)
class CrossClusterProbe(Message):
    """
    Cross-cluster health probe (xprobe).

    Sent from gates to DC leader managers to check health.
    Minimal format - no gossip, just identity.
    """

    source_cluster_id: str  # Gate cluster ID
    source_node_id: str  # Sending gate's node ID
    source_addr: tuple[str, int]  # For response routing


@dataclass(slots=True)
class CrossClusterAck(Message):
    """
    Cross-cluster health acknowledgment (xack).

    Response from DC leader with aggregate datacenter health.
    """

    # Identity
    datacenter: str
    node_id: str
    incarnation: int  # External incarnation (separate from cluster incarnation)

    # Leadership
    is_leader: bool
    leader_term: int

    # Cluster health
    cluster_size: int  # Total managers in DC
    healthy_managers: int  # Managers responding to SWIM

    # Worker capacity
    worker_count: int
    healthy_workers: int
    total_cores: int
    available_cores: int

    # Workload
    active_jobs: int
    active_workflows: int

    # Self-reported health
    dc_health: str  # "HEALTHY", "DEGRADED", "BUSY", "UNHEALTHY"

    # Optional: reason for non-healthy status
    health_reason: str = ""


@dataclass(slots=True)
class DCLeaderAnnouncement(Message):
    """
    Announcement when a manager becomes DC leader.

    Sent via TCP to notify gates of leadership changes.
    """

    datacenter: str
    leader_node_id: str
    leader_tcp_addr: tuple[str, int]
    leader_udp_addr: tuple[str, int]
    term: int
    timestamp: float = field(default_factory=time.time)


@dataclass(slots=True)
class DCHealthState:
    """
    Gate's view of a datacenter's health.

    Combines probe reachability with self-reported health.
    """

    datacenter: str
    leader_udp_addr: tuple[str, int] | None = None
    leader_tcp_addr: tuple[str, int] | None = None
    leader_node_id: str = ""
    leader_term: int = 0

    # Probe state
    reachability: DCReachability = DCReachability.UNREACHABLE
    last_probe_sent: float = 0.0
    last_ack_received: float = 0.0
    consecutive_failures: int = 0

    # External incarnation tracking
    incarnation: int = 0

    # Last known health (from ack)
    last_ack: CrossClusterAck | None = None

    # Suspicion timing
    suspected_at: float = 0.0

    @property
    def effective_health(self) -> str:
        """Combine reachability and reported health."""
        if self.reachability == DCReachability.UNREACHABLE:
            return "UNREACHABLE"
        if self.reachability == DCReachability.SUSPECTED:
            return "SUSPECTED"
        if self.last_ack:
            return self.last_ack.dc_health
        return "UNKNOWN"

    @property
    def is_healthy_for_jobs(self) -> bool:
        """Can this DC accept new jobs?"""
        if self.reachability == DCReachability.UNREACHABLE:
            return False
        if not self.last_ack:
            return False
        return self.last_ack.dc_health in ("HEALTHY", "DEGRADED", "BUSY")


@dataclass(slots=True)
class FederatedHealthMonitor:
    """
    Monitors external datacenter clusters using SWIM-style probes.

    NOT a SWIM cluster member - uses probe/ack for health detection
    with separate incarnation tracking and suspicion state.

    Designed for high-latency, globally distributed links:
    - Longer probe intervals (2s default)
    - Longer suspicion timeouts (30s default)
    - Higher failure tolerance before marking unreachable
    """

    # Probe configuration (tuned for global distribution)
    probe_interval: float = 2.0  # Seconds between probes to each DC
    probe_timeout: float = 5.0  # Timeout for single probe
    suspicion_timeout: float = 30.0  # Time before suspected -> unreachable
    max_consecutive_failures: int = 5  # Failures before suspected

    # Identity
    cluster_id: str = ""
    node_id: str = ""

    # Callbacks (set by owner)
    _send_udp: Callable[[tuple[str, int], bytes], Awaitable[bool]] | None = None
    _on_dc_health_change: Callable[[str, str], None] | None = None  # (dc, new_health)
    _on_dc_latency: Callable[[str, float], None] | None = (
        None  # (dc, latency_ms) - Phase 7
    )
    _on_dc_leader_change: (
        Callable[[str, str, tuple[str, int], tuple[str, int], int], None] | None
    ) = None  # (dc, leader_node_id, tcp_addr, udp_addr, term)

    # State
    _dc_health: dict[str, DCHealthState] = field(default_factory=dict)
    _running: bool = False
    _probe_task: asyncio.Task | None = None

    def set_callbacks(
        self,
        send_udp: Callable[[tuple[str, int], bytes], Awaitable[bool]],
        cluster_id: str,
        node_id: str,
        on_dc_health_change: Callable[[str, str], None] | None = None,
        on_dc_latency: Callable[[str, float], None] | None = None,
        on_dc_leader_change: Callable[
            [str, str, tuple[str, int], tuple[str, int], int], None
        ]
        | None = None,
    ) -> None:
        """
        Set callback functions.

        Args:
            send_udp: Async function to send UDP packets.
            cluster_id: This gate's cluster ID.
            node_id: This gate's node ID.
            on_dc_health_change: Called when DC health changes (dc, new_health).
            on_dc_latency: Called with latency measurements (dc, latency_ms).
                Used for cross-DC correlation to distinguish network issues.
            on_dc_leader_change: Called when DC leader changes (dc, leader_node_id, tcp_addr, udp_addr, term).
                Used to propagate DC leadership changes to peer gates.
        """
        self._send_udp = send_udp
        self.cluster_id = cluster_id
        self.node_id = node_id
        self._on_dc_health_change = on_dc_health_change
        self._on_dc_latency = on_dc_latency
        self._on_dc_leader_change = on_dc_leader_change

    def add_datacenter(
        self,
        datacenter: str,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None = None,
        leader_node_id: str = "",
        leader_term: int = 0,
    ) -> None:
        """Add or update a datacenter to monitor."""
        if datacenter in self._dc_health:
            state = self._dc_health[datacenter]
            state.leader_udp_addr = leader_udp_addr
            if leader_tcp_addr:
                state.leader_tcp_addr = leader_tcp_addr
            if leader_node_id:
                state.leader_node_id = leader_node_id
            if leader_term > state.leader_term:
                state.leader_term = leader_term
        else:
            self._dc_health[datacenter] = DCHealthState(
                datacenter=datacenter,
                leader_udp_addr=leader_udp_addr,
                leader_tcp_addr=leader_tcp_addr,
                leader_node_id=leader_node_id,
                leader_term=leader_term,
            )

    def remove_datacenter(self, datacenter: str) -> None:
        """Stop monitoring a datacenter."""
        self._dc_health.pop(datacenter, None)

    def update_leader(
        self,
        datacenter: str,
        leader_udp_addr: tuple[str, int],
        leader_tcp_addr: tuple[str, int] | None = None,
        leader_node_id: str = "",
        leader_term: int = 0,
    ) -> bool:
        """
        Update DC leader address (from leader announcement).

        Returns True if leader actually changed (term is higher), False otherwise.
        """
        if datacenter not in self._dc_health:
            self.add_datacenter(
                datacenter,
                leader_udp_addr,
                leader_tcp_addr,
                leader_node_id,
                leader_term,
            )
            # New DC is considered a change
            if self._on_dc_leader_change and leader_tcp_addr:
                self._on_dc_leader_change(
                    datacenter,
                    leader_node_id,
                    leader_tcp_addr,
                    leader_udp_addr,
                    leader_term,
                )
            return True

        state = self._dc_health[datacenter]

        # Only update if term is higher (prevent stale updates)
        if leader_term < state.leader_term:
            return False

        # Check if this is an actual leader change (term increased or node changed)
        leader_changed = (
            leader_term > state.leader_term or leader_node_id != state.leader_node_id
        )

        state.leader_udp_addr = leader_udp_addr
        if leader_tcp_addr:
            state.leader_tcp_addr = leader_tcp_addr
        state.leader_node_id = leader_node_id
        state.leader_term = leader_term

        # Reset suspicion on leader change
        if state.reachability == DCReachability.SUSPECTED:
            state.reachability = DCReachability.UNREACHABLE
            state.consecutive_failures = 0

        # Fire callback if leader actually changed
        if leader_changed and self._on_dc_leader_change and leader_tcp_addr:
            self._on_dc_leader_change(
                datacenter,
                leader_node_id,
                leader_tcp_addr,
                leader_udp_addr,
                leader_term,
            )

        return leader_changed

    def get_dc_health(self, datacenter: str) -> DCHealthState | None:
        """Get current health state for a datacenter."""
        return self._dc_health.get(datacenter)

    def get_all_dc_health(self) -> dict[str, DCHealthState]:
        """Get health state for all monitored datacenters."""
        return dict(self._dc_health)

    def get_healthy_datacenters(self) -> list[str]:
        """Get list of DCs that can accept jobs."""
        # Snapshot to avoid dict mutation during iteration
        return [
            dc
            for dc, state in list(self._dc_health.items())
            if state.is_healthy_for_jobs
        ]

    async def start(self) -> None:
        """Start the health monitoring probe loop."""
        self._running = True
        self._probe_task = asyncio.create_task(self._probe_loop())

    async def stop(self) -> None:
        """Stop the health monitoring probe loop."""
        self._running = False
        if self._probe_task:
            self._probe_task.cancel()
            try:
                await self._probe_task
            except asyncio.CancelledError:
                pass
            self._probe_task = None

    async def _probe_loop(self) -> None:
        """Main probe loop - probes all DCs in round-robin."""
        while self._running:
            try:
                dcs = list(self._dc_health.keys())
                if not dcs:
                    await asyncio.sleep(self.probe_interval)
                    continue

                # Probe each DC with interval spread across all DCs
                interval_per_dc = self.probe_interval / len(dcs)

                for dc in dcs:
                    if not self._running:
                        break
                    await self._probe_datacenter(dc)
                    self._check_ack_timeouts()
                    await asyncio.sleep(interval_per_dc)

            except asyncio.CancelledError:
                break
            except Exception:
                # Log error but continue probing
                await asyncio.sleep(1.0)

    async def _probe_datacenter(self, datacenter: str) -> None:
        """Send a probe to a datacenter's leader."""
        state = self._dc_health.get(datacenter)
        if not state or not state.leader_udp_addr:
            return

        if not self._send_udp:
            return

        # Build probe
        probe = CrossClusterProbe(
            source_cluster_id=self.cluster_id,
            source_node_id=self.node_id,
            source_addr=(self.node_id, 0),  # Will be filled by transport
        )

        state.last_probe_sent = time.monotonic()

        # Send probe (with timeout)
        try:
            probe_data = b"xprobe>" + probe.dump()
            success = await asyncio.wait_for(
                self._send_udp(state.leader_udp_addr, probe_data),
                timeout=self.probe_timeout,
            )

            if not success:
                self._handle_probe_failure(state)
        except asyncio.TimeoutError:
            self._handle_probe_failure(state)
        except Exception:
            self._handle_probe_failure(state)

    def _check_ack_timeouts(self) -> None:
        """
        Check all DCs for ack timeout and transition to SUSPECTED/UNREACHABLE.

        This handles the case where probes are sent successfully but no ack arrives.
        Without this, a DC could remain REACHABLE indefinitely after its last ack.
        """
        now = time.monotonic()
        ack_grace_period = self.probe_timeout * self.max_consecutive_failures

        for state in self._dc_health.values():
            if state.reachability == DCReachability.UNREACHABLE:
                continue

            if state.last_ack_received == 0.0:
                continue

            time_since_last_ack = now - state.last_ack_received

            if time_since_last_ack > ack_grace_period:
                old_reachability = state.reachability

                if state.reachability == DCReachability.REACHABLE:
                    state.reachability = DCReachability.SUSPECTED
                    state.suspected_at = now
                elif state.reachability == DCReachability.SUSPECTED:
                    if now - state.suspected_at > self.suspicion_timeout:
                        state.reachability = DCReachability.UNREACHABLE

                if state.reachability != old_reachability and self._on_dc_health_change:
                    self._on_dc_health_change(state.datacenter, state.effective_health)

    def _handle_probe_failure(self, state: DCHealthState) -> None:
        state.consecutive_failures += 1

        old_reachability = state.reachability

        if state.consecutive_failures >= self.max_consecutive_failures:
            if state.reachability == DCReachability.REACHABLE:
                state.reachability = DCReachability.SUSPECTED
                state.suspected_at = time.monotonic()
            elif state.reachability == DCReachability.SUSPECTED:
                if time.monotonic() - state.suspected_at > self.suspicion_timeout:
                    state.reachability = DCReachability.UNREACHABLE

        if state.reachability != old_reachability and self._on_dc_health_change:
            self._on_dc_health_change(state.datacenter, state.effective_health)

    def handle_ack(self, ack: CrossClusterAck) -> None:
        """Handle an xack response from a DC leader."""
        state = self._dc_health.get(ack.datacenter)
        if not state:
            return

        # Check incarnation for staleness
        if ack.incarnation < state.incarnation:
            # Stale ack - ignore
            return

        old_reachability = state.reachability
        old_health = state.effective_health

        now = time.monotonic()

        # Calculate latency for cross-DC correlation (Phase 7)
        # Latency = time between sending probe and receiving ack
        if state.last_probe_sent > 0 and self._on_dc_latency:
            latency_ms = (now - state.last_probe_sent) * 1000
            self._on_dc_latency(ack.datacenter, latency_ms)

        # Update state
        state.incarnation = ack.incarnation
        state.last_ack_received = now
        state.last_ack = ack
        state.consecutive_failures = 0
        state.reachability = DCReachability.REACHABLE

        # Update leader info from ack
        if ack.is_leader:
            state.leader_node_id = ack.node_id
            state.leader_term = ack.leader_term

        # Notify on change
        new_health = state.effective_health
        if (
            state.reachability != old_reachability or new_health != old_health
        ) and self._on_dc_health_change:
            self._on_dc_health_change(state.datacenter, new_health)
